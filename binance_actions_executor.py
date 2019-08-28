import time
from decimal import Decimal, ROUND_DOWN
from typing import List, Tuple
from collections import deque
from PyQt5.QtCore import QThread, pyqtSignal
from config import WAIT_ORDER_TO_FILL, TRADE_FEE
from binance_api import BinanceApi
from binance_account_info import BinanceAccountInfo
from arbitrage_detector import ArbitrageDetector, Arbitrage
from custom_logging import get_logger


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


class BinanceActionsExecutor(QThread):

    action_executed = pyqtSignal()
    execution_finished = pyqtSignal()

    def __init__(self, api_key: str, api_secret: str, actions_list: List[BinanceSingleAction],
                 detector: ArbitrageDetector, arbitrage: Arbitrage,
                 account_info: BinanceAccountInfo = None, parent=None):
        super(BinanceActionsExecutor, self).__init__(parent=parent)

        self.__api_key = api_key
        self.__api_secret = api_secret
        self.__api = None  # will be set properly in run() to avoid threading problems
        self.__actions_list = actions_list

        self.__account_info = account_info

        self.__pretty_str = ''
        self.__set_pretty_str(actions_list)

        self.__detector = detector
        self.__arbitrage = arbitrage

    def __str__(self):
        return self.__pretty_str

    def __set_pretty_str(self, actions_list: List[BinanceSingleAction]):
        self.__pretty_str = ' -> '.join([action.side + ': ' + action.symbol for action in actions_list])

    def get_actions_list(self) -> List[BinanceSingleAction]:
        return self.__actions_list

    def set_actions_list(self, actions_list: List[BinanceSingleAction]):
        self.__actions_list = actions_list
        self.__set_pretty_str(actions_list)

    def run(self):
        logger.info('Executor starting...')
        # init api and account info
        self.__api = BinanceApi(self.__api_key, self.__api_secret)
        if self.__account_info is None:
            self.__account_info = BinanceAccountInfo(self.__api)
        self.action_executed.connect(self.__account_info.update_info_async)

        actions_list = self.__get_executable_actions_list()
        logger.info('Executable actions list: {}', actions_list)

        # actions list is required to be exactly 3 actions for now
        actions_length = len(actions_list)
        if actions_length == 0:
            logger.info('Cannot execute those actions')
            self.execution_finished.emit()
            return
        if actions_length != 3:
            logger.error('Bad actions list: {}', actions_list)
            self.execution_finished.emit()
            return

        # emergency actions in case of any failures
        emergency_actions = []

        for i in range(actions_length):
            action = actions_list[i]
            logger.info('Executing action: {}...', action)
            reply_json = self.__try_execute_action(action)
            # logger.debug('Order creation response: {}', reply_json)

            if not reply_json or 'error' in reply_json:
                # order creation failed, execute emergency actions
                logger.error('Action failed! Failed to place an order.')
                break
            else:
                # order created, now wait for it to get filled
                t = time.time()
                symbol = reply_json['symbol']
                order_id = reply_json['orderId']
                status = reply_json['status']
                while status != 'FILLED' and time.time() - t < WAIT_ORDER_TO_FILL:
                    status = self.__get_order_status(reply_json)
                # possible statuses: NEW, PARTIALLY_FILLED, FILLED, CANCELED, REJECTED, EXPIRED
                if status in ['NEW', 'PARTIALLY_FILLED']:
                    # order is not filled, cancel it and execute emergency actions
                    logger.info('Action failed! Order is not filled. Cancelling order {} on symbol {}...', order_id, symbol)
                    reply_json = self.__api.cancel_order(symbol, order_id)
                    try:
                        amount_filled = Decimal(reply_json['executedQty'])
                    except KeyError:
                        if reply_json['msg'] == 'Unknown order sent.':
                            logger.warning('Order {} {} not found, already completed?', symbol, order_id)
                        else:
                            logger.error('Order cancellation failed, response: {}', reply_json)
                        break
                    if amount_filled > 0:
                        logger.info('Order has been partially filled: {} {}', amount_filled, action.base)
                        if i < 2:
                            # emergency: revert partially executed amount
                            amount_revert = (amount_filled * (1 - TRADE_FEE)).quantize(
                                self.symbols_filters[symbol]['amount_step'], rounding=ROUND_DOWN
                            )
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
                            logger.info('Remaining amount: {}', actions_list[i].quantity)
                    break
                if status in ['CANCELED', 'REJECTED', 'EXPIRED']:
                    # order failed for unclear reasons, just execute emergency actions
                    logger.info('Action failed! Unexpected order status: {}.', status)
                    break

                logger.info('Action completed!')
                continue
        else:
            self.execution_finished.emit()
            return

        # performing emergency actions
        if i == 0:
            # first action failed: nothing to revert
            logger.info('Failed to execute the actions list')
        elif i == 1:
            # second action failed: revert first action
            action = actions_list[0]
            logger.info('Reverting first action: {}', action)
            amount_revert = (action.quantity * (1 - TRADE_FEE)).quantize(
                self.__detector.symbols_filters[symbol]['amount_step'], rounding=ROUND_DOWN
            )
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
            logger.info('Finalizing last action: {}, as a market order', action)
            emergency_actions.append(
                BinanceSingleAction(
                    pair=action.pair,
                    side=action.side,
                    quantity=action.quantity,
                    order_type='MARKET'
                )
            )

        for action in emergency_actions:
            self.__execute_emergency_action(action)

        self.execution_finished.emit()
        logger.info('Executor finished')

    def __execute_emergency_action(self, action):
        logger.info('Executing emergency action: {}...', action)
        while 1:
            reply_json = self.__try_execute_action(action)
            try:
                status = self.__get_order_status(reply_json)
            except BinanceActionException:
                if 'msg' in reply_json and 'insufficient balance' in reply_json['msg']:
                    # reduce action amount and try again
                    qty_min, qty_max, qty_step = self.__detector.symbols_info[action.symbol].get_qty_filter()
                    action.quantity -= qty_step
                    if action.quantity < qty_min:
                        logger.error('Insufficient balance to execute emergency action')
                        return
                    # try executing the action again with the reduced amount
                    continue
                else:
                    logger.error('Failed to execute emergency action, server response: {}', reply_json)
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

    def __get_executable_actions_list(self) -> List[BinanceSingleAction]:
        actions = self.__actions_list
        logger.debug('Initial actions list: {}', actions)
        # actions list is expected to be exactly three items long, in a triangle
        if len(actions) != 3:
            logger.error('Number of actions is not 3: {}', actions)
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
            logger.error('Bad actions list: not a valid triangle! Actions: {}', actions)
            return []
        logger.debug('Sequenced actions list: {}', actions)
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
            balance = self.__account_info.get_balance(asset)
            logger.debug('{} balance: {:.8f}', asset, balance)
            candidates.append(balance / amount)
        # we will start with the action that has the highest balance/amount proportion
        proportion = max(candidates)
        idx = candidates.index(proportion)
        shift = -idx
        if shift != 0:
            dq = deque(actions)
            # logger.debug('Rotating actions list by: {}', shift)
            dq.rotate(shift)
            actions = list(dq)
        if proportion < 1:
            # recalculate action amounts to fit in our balance and keep the arbitrage profitable
            logger.debug('Reducing the arbitrage by: {}', proportion)
            reduced = self.__detector.reduce_arbitrage(
                arb=self.__arbitrage,
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
            logger.debug('Reduced arbitrage actions list: {}', actions)
        return actions

    def __try_execute_action(self, action: BinanceSingleAction):
        logger.info('Executing action: {}...', action)
        return self.__api.create_order(
            action.symbol,
            action.side,
            action.type,
            action.quantity,
            action.timeInForce,
            action.price,
            action.newClientOrderId
        )

    def __get_order_status(self, reply_json) -> str or None:
        try:
            status = reply_json['status']
        except KeyError:
            raise BinanceActionException
        if status == 'NEW':
            symbol = reply_json['symbol']
            order_id = reply_json['orderId']
            # Order statuses in Binance:
            #   NEW, PARTIALLY_FILLED, FILLED, CANCELED, REJECTED, EXPIRED
            reply_json = self.__api.order_info(symbol, order_id)
            if reply_json and 'status' in reply_json:
                status = reply_json['status']
        return status


def main():
    pass


if __name__ == '__main__':
    main()
