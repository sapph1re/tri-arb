import asyncio
import time
from typing import Dict
from pydispatch import dispatcher
from decimal import Decimal, ROUND_DOWN
from typing import List, Tuple
from collections import deque
from config import WAIT_ORDER_TO_FILL, TRADE_FEE
from binance_api import BinanceApi, BinanceSymbolInfo, BinanceAPIException
from binance_account_info import BinanceAccountInfo
from arbitrage_detector import ArbitrageDetector, Arbitrage
from logger import get_logger

logger = get_logger(__name__)


class BinanceActionException(Exception):
    pass


class BinanceSingleAction:

    def __init__(self, pair: Tuple[str, str], side: str, quantity, price=None,
                 order_type='LIMIT', timeInForce: str = 'FOK', newClientOrderId: str = None):
        """
        :param base: EOS/USDT -> BTC
        :param quote: EOS/USDT -> USDT
        :param symbol: пара (т.е. 'BTCUSDT', 'ETHBTC', 'EOSETH')
        :param side: тип ордера (BUY либо SELL)
        :param quantity: количество к покупке
        :param price: цена (не обязательно для MARKET ордера)
        :param order_type: тип ордера (LIMIT, MARKET, STOP_LOSS, STOP_LOSS_LIMIT,
                    TAKE_PROFIT, TAKE_PROFIT_LIMIT, LIMIT_MAKER)
        :param timeInForce: (GTC, IOC, FOK). По умолчанию GTC. Расшифрую.
                    GTC (Good Till Cancelled) – ордер будет висеть до тех пор, пока его не отменят.
                    IOC (Immediate Or Cancel) – Будет куплено то количество, которое можно купить немедленно.
                        Все, что не удалось купить, будет отменено.
                    FOK (Fill-Or-Kill) – Либо будет куплено все указанное количество немедленно,
                        либо не будет куплено вообще ничего, ордер отменится.
        :param newClientOrderId: Идентификатор ордера, который вы сами придумаете (строка).
                    Если не указан, генерится автоматически.
        """
        self.pair = pair
        self.base = pair[0]
        self.quote = pair[1]
        self.symbol = (pair[0]+pair[1]).upper()
        self.side = side.upper()
        self.quantity = quantity
        self.price = price
        self.type = order_type.upper()
        self.timeInForce = timeInForce.upper()
        self.newClientOrderId = newClientOrderId

    def __str__(self):
        if self.type == 'MARKET':
            return '{} {} {} {}/{}'.format(
                self.type, self.side, self.quantity,
                self.base, self.quote
            )
        else:
            return '{} {} {} {}/{} @ {} ({})'.format(
                self.type, self.side, self.quantity,
                self.base, self.quote, self.price, self.timeInForce
            )

    def __repr__(self):
        return self.__str__()


class BinanceActionsExecutor:
    def __init__(self, api: BinanceApi, actions: List[BinanceSingleAction],
                 symbols_info: Dict[str, BinanceSymbolInfo], detector: ArbitrageDetector = None,
                 arbitrage: Arbitrage = None, account_info: BinanceAccountInfo = None):
        self._api = api
        self._actions_list = actions
        self._account_info = account_info
        self._symbols_info = symbols_info
        self._detector = detector
        self._arbitrage = arbitrage

    def __str__(self):
        return ' -> '.join([action.side + ': ' + action.symbol for action in self._actions_list])

    def get_actions_list(self) -> List[BinanceSingleAction]:
        return self._actions_list

    def set_actions_list(self, actions_list: List[BinanceSingleAction]):
        self._actions_list = actions_list

    async def run(self):
        logger.info('Executor starting...')
        # init account info if it hasn't been passed from above
        if self._account_info is None:
            self._account_info = BinanceAccountInfo(self._api)

        actions_list = self._get_executable_action_list()
        logger.info(f'Executable actions list: {actions_list}')

        # actions list is required to be exactly 3 actions for now
        actions_length = len(actions_list)
        if actions_length == 0:
            logger.info('Cannot execute those actions')
            dispatcher.send(signal='execution_finished', sender=self)
            return
        if actions_length != 3:
            logger.error(f'Bad actions list: {actions_list}')
            dispatcher.send(signal='execution_finished', sender=self)
            return

        # emergency actions in case of any failures
        emergency_actions = []

        for i in range(actions_length):
            action = actions_list[i]
            logger.info(f'Executing action: {action}...')
            result = await self._execute_action(action)
            if not result or 'error' in result:
                # order creation failed, execute emergency actions
                logger.error('Action failed! Failed to place an order.')
                break
            else:
                # order created, now wait for it to get filled
                t = time.time()
                symbol = result['symbol']
                order_id = result['orderId']
                status = result['status']
                while status != 'FILLED' and time.time() - t < WAIT_ORDER_TO_FILL:
                    status = await self._get_order_status(result)
                # possible statuses: NEW, PARTIALLY_FILLED, FILLED, CANCELED, REJECTED, EXPIRED
                if status in ['NEW', 'PARTIALLY_FILLED']:
                    # order is not filled, cancel it and execute emergency actions
                    logger.info(f'Action failed! Order is not filled. Cancelling order {order_id} on symbol {symbol}...')
                    result = await self._api.cancel_order(symbol, order_id)
                    try:
                        amount_filled = Decimal(result['executedQty'])
                    except KeyError:
                        if result['msg'] == 'Unknown order sent.':
                            logger.warning(f'Order {symbol} {order_id} not found, already completed?')
                        else:
                            logger.error(f'Order cancellation failed, response: {result}')
                        break
                    if amount_filled > 0:
                        logger.info(f'Order has been partially filled: {amount_filled} {action.base}')
                        if i < 2:
                            # emergency: revert partially executed amount
                            qty_filter = self._symbols_info[action.symbol].get_qty_filter()
                            amount_step = Decimal(qty_filter[2]).normalize()
                            amount_revert = (amount_filled * (1 - TRADE_FEE)).quantize(amount_step, rounding=ROUND_DOWN)
                            emergency_actions.append(
                                BinanceSingleAction(
                                    pair=action.pair,
                                    side='BUY' if action.side == 'SELL' else 'SELL',
                                    quantity=amount_revert,
                                    order_type='MARKET'
                                )
                            )
                        else:
                            # emergency: finalize remaining amount
                            actions_list[i].quantity -= amount_filled
                            logger.info(f'Remaining amount: {actions_list[i].quantity}')
                    break
                if status in ['CANCELED', 'REJECTED', 'EXPIRED']:
                    # order failed for unclear reasons, just execute emergency actions
                    logger.info(f'Action failed! Unexpected order status: {status}.')
                    break

                logger.info('Action completed!')
                continue
        else:
            dispatcher.send(signal='execution_finished', sender=self)
            return

        # performing emergency actions
        if i == 0:
            # first action failed: nothing to revert
            logger.info('Failed to execute the actions list')
        elif i == 1:
            # second action failed: revert first action
            action = actions_list[0]
            logger.info(f'Reverting first action: {action}')
            qty_filter = self._symbols_info[action.symbol].get_qty_filter()
            amount_step = Decimal(qty_filter[2]).normalize()
            amount_revert = (action.quantity * (1 - TRADE_FEE)).quantize(amount_step, rounding=ROUND_DOWN)
            emergency_actions.append(
                BinanceSingleAction(
                    pair=action.pair,
                    side='BUY' if action.side == 'SELL' else 'SELL',
                    quantity=amount_revert,
                    order_type='MARKET'
                )
            )
        elif i == 2:
            # third action failed: complete third action as a market order
            action = actions_list[2]
            logger.info(f'Finalizing last action: {action}, as a market order')
            emergency_actions.append(
                BinanceSingleAction(
                    pair=action.pair,
                    side=action.side,
                    quantity=action.quantity,
                    order_type='MARKET'
                )
            )

        for action in emergency_actions:
            await self._execute_emergency_action(action)

        dispatcher.send(signal='execution_finished', sender=self)
        logger.info('Executor finished')

    async def _execute_emergency_action(self, action):
        logger.info(f'Executing emergency action: {action}...')
        while 1:
            result = await self._execute_action(action)
            try:
                status = await self._get_order_status(result)
            except BinanceActionException:
                if 'msg' in result and 'insufficient balance' in result['msg']:
                    # reduce action amount and try again
                    qty_min, qty_max, qty_step = self._symbols_info[action.symbol].get_qty_filter()
                    action.quantity -= qty_step
                    if action.quantity < qty_min:
                        logger.error('Insufficient balance to execute emergency action')
                        return
                    # try executing the action again with the reduced amount
                    continue
                else:
                    logger.error(f'Failed to execute emergency action, server response: {result}')
                    return
            else:
                break
        if status == 'FILLED':
            logger.info('Action completed')
        else:
            logger.error(
                'Emergency action FAILED: {}. Status: {}',
                action,
                str(status)
            )

    def _get_executable_action_list(self) -> List[BinanceSingleAction]:
        actions = self._actions_list
        logger.debug(f'Initial actions list: {actions}')
        # actions list is expected to be exactly three items long, in a triangle
        if len(actions) != 3:
            logger.error(f'Number of actions is not 3: {actions}')
            return []
        # first rearrange actions in a sequence to pass funds along the sequence
        gain = []
        spend = []
        for action in actions:
            if action.side == 'BUY':
                gain.append(action.base)
                spend.append(action.quote)
            else:
                gain.append(action.quote)
                spend.append(action.base)
        if gain[0] == spend[1] and gain[1] == spend[2] and gain[2] == spend[0]:
            # sequence is already fine
            pass
        elif gain[0] == spend[2] and gain[2] == spend[1] and gain[1] == spend[0]:
            # sequence needs to be rearranged
            actions = [actions[0], actions[2], actions[1]]
        else:
            logger.error(f'Bad actions list: not a valid triangle! Actions: {actions}')
            return []
        logger.debug(f'Sequenced actions list: {actions}')
        # then figure out which action to start with and rotate the sequence
        candidates = []
        shift = 0
        for action in actions:
            side = action.side
            base = action.base
            quote = action.quote
            quantity = action.quantity
            price = action.price

            if side == 'BUY':
                asset = quote
                amount = quantity * price
            else:
                asset = base
                amount = quantity
            balance = self._account_info.get_balance(asset)
            logger.debug(f'{asset} balance: {balance:.8f}')
            candidates.append(balance / amount)
        # we will start with the action that has the highest balance/amount proportion
        proportion = max(candidates)
        idx = candidates.index(proportion)
        shift = -idx
        if shift != 0:
            dq = deque(actions)
            # logger.debug(f'Rotating actions list by: {shift}')
            dq.rotate(shift)
            actions = list(dq)
        if proportion < 1:
            if self._detector is None or self._arbitrage is None:
                logger.info('Action amounts cannot be reduced without a Detector')
                return []
            # recalculate action amounts to fit in our balance and keep the arbitrage profitable
            logger.debug(f'Reducing the arbitrage by: {proportion}')
            reduced = self._detector.reduce_arbitrage(
                arb=self._arbitrage,
                reduce_factor=proportion
            )
            if reduced is None:
                logger.info('Arbitrage is not available with reduced amounts')
                return []
            # extract amounts from the reduced arbitrage
            for action in actions:
                a = next((
                    a for a in reduced.actions if a.pair == action.pair and a.action.upper() == action.side
                ), None)
                action.quantity = a.amount
            logger.debug(f'Reduced arbitrage actions list: {actions}')
        return actions

    async def _execute_action(self, action: BinanceSingleAction):
        try:
            return await self._api.create_order(
                action.symbol,
                action.side,
                action.type,
                action.quantity,
                action.timeInForce,
                action.price,
                action.newClientOrderId
            )
        except BinanceAPIException as e:
            logger.error(f'Action failed: {action}. Reason: {e}')
            return None

    async def _get_order_status(self, order_result) -> str or None:
        try:
            status = order_result['status']
        except KeyError:
            raise BinanceActionException
        if status == 'NEW':
            symbol = order_result['symbol']
            order_id = order_result['orderId']
            # Order statuses in Binance:
            #   NEW, PARTIALLY_FILLED, FILLED, CANCELED, REJECTED, EXPIRED
            order_result = await self._api.order_info(symbol, order_id)
            if order_result and 'status' in order_result:
                status = order_result['status']
        return status


def test_on_execution_finished(sender):
    logger.info('Actions execution has finished!')


async def main():
    from config import API_KEY, API_SECRET

    # it will try to execute a demonstratory set of actions
    # they won't give actual profit, it's just to test that it all works

    api = await BinanceApi.create(API_KEY, API_SECRET)
    acc = await BinanceAccountInfo.create(api, auto_update_interval=10)
    symbols_info = await api.get_symbols_info()
    actions = [
        BinanceSingleAction(
            pair=('BTC', 'USDT'),
            side='SELL',
            quantity=Decimal('0.002'),
            price=Decimal('5000.0'),
            order_type='LIMIT',
            timeInForce='GTC'
        ),
        BinanceSingleAction(
            pair=('ETH', 'USDT'),
            side='BUY',
            quantity=Decimal('0.05'),
            price=Decimal('200'),
            order_type='LIMIT',
            timeInForce='GTC'
        ),
        BinanceSingleAction(
            pair=('ETH', 'BTC'),
            side='SELL',
            quantity=Decimal('0.05'),
            price=Decimal('0.01'),
            order_type='LIMIT',
            timeInForce='GTC'
        )
    ]

    executor = BinanceActionsExecutor(api=api, actions=actions, symbols_info=symbols_info, account_info=acc)
    dispatcher.connect(test_on_execution_finished, signal='execution_finished', sender=executor)
    await executor.run()

    await asyncio.sleep(5)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
