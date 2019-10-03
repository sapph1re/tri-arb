import asyncio
from typing import Dict
from pydispatch import dispatcher
from decimal import Decimal, ROUND_DOWN
from typing import List, Tuple
from collections import deque
from config import CHECK_ORDER_INTERVAL, TRADE_FEE
from binance_api import BinanceApi, BinanceSymbolInfo, BinanceAPIException
from binance_account_info import BinanceAccountInfo
from arbitrage_detector import ArbitrageDetector, Arbitrage
from logger import get_logger

logger = get_logger(__name__)


class ActionException(Exception):
    pass


class Action:
    def __init__(self, pair: Tuple[str, str], side: str, quantity, price=None, order_type='LIMIT'):
        self.pair = pair
        self.base = pair[0]
        self.quote = pair[1]
        self.symbol = (pair[0]+pair[1]).upper()
        self.side = side.upper()
        self.quantity = quantity
        self.price = price
        self.type = order_type.upper()

    def __str__(self):
        s = f'{self.type} {self.side} {self.quantity:f} {self.base}/{self.quote}'
        if self.type != 'MARKET':
            s += f' @ {self.price:f}'
        return s

    def __repr__(self):
        return self.__str__()


class ActionSet:
    def __init__(self, steps: List[List[Action]]):
        """
        :param steps: list of sequential steps, on each step there is a list of parallel actions
        """
        self.steps = steps

    def __str__(self):
        s = []
        for i, actions in enumerate(self.steps):
            actions_str = '\n'.join([f'\t{action}' for action in actions])
            s.append(f'Step {i+1}:\n{actions_str}')
        return '\n'.join(s)

    def __repr__(self):
        return self.__str__()


class BinanceActionExecutor:
    def __init__(self, api: BinanceApi, actions: List[Action],
                 symbols_info: Dict[str, BinanceSymbolInfo], detector: ArbitrageDetector = None,
                 arbitrage: Arbitrage = None, account_info: BinanceAccountInfo = None):
        self._api = api
        self._raw_action_list = actions
        self._account_info = account_info
        self._symbols_info = symbols_info
        self._detector = detector
        self._arbitrage = arbitrage

    def __str__(self):
        return ' -> '.join([action.side + ': ' + action.symbol for action in self._raw_action_list])

    async def run(self):
        logger.info('Executor starting...')
        # init account info if it hasn't been passed from above
        if self._account_info is None:
            self._account_info = BinanceAccountInfo(self._api)

        action_set = self._get_executable_action_set()
        if action_set is None:
            logger.info('Cannot execute those actions')
            dispatcher.send(signal='execution_finished', sender=self)
            return
        logger.info(f'Executable action set:\n{action_set}')

        # emergency actions in case of any failures
        emergency_actions = []

        # execute each step one by one
        for step, actions in enumerate(action_set.steps):
            # execute all actions of each step in parallel
            actions_str = '\n'.join([f'\t{action}' for action in actions])
            logger.info(f'Step {step+1}/{len(action_set.steps)}. Executing actions:\n{actions_str}')
            results = await asyncio.gather(*[self._execute_action(action) for action in actions])
            # check whether the orders were placed successfully
            failed = []
            placed = []
            for idx, result in enumerate(results):
                action = actions[idx]
                if result is None or 'error' in result:
                    # order creation failed
                    logger.error(f'Failed to place an order! Failed action: {action}')
                    failed.append(idx)
                else:
                    placed.append(idx)
            if len(failed) == 0:
                logger.info('All actions at this step placed orders successfully')
            elif len(failed) == len(actions):
                logger.info('All actions at this step have failed')
                if step == 0:
                    logger.info('Aborting')
                elif step == 1 and len(action_set.steps) == 3:
                    logger.info('Step 1 will be reverted')
                    emergency_actions.append(self._revert_action(action_set.steps[0][0]))
                else:
                    logger.info('Last step failed, it will be finalized')
                    emergency_actions.append(self._finalize_action(actions[failed[0]]))
                break
            elif len(failed) == 1 and len(actions) == 2:
                # 1 of 2 failed
                logger.info('1 of 2 parallel actions has failed, cancelling and reverting the other one')
                emergency_actions.extend(
                    await self._cancel_and_revert(results[placed[0]], actions[placed[0]])
                )
                break
            elif len(failed) == 1 and len(actions) == 3:
                # 1 of 3 failed
                logger.info('1 of 3 parallel actions has failed, we will wait for the results of the other two')
                # first we'll let the other 2 get filled, and if they do, we'll finalize this one
            elif len(failed) == 2 and len(actions) == 3:
                # 2 of 3 failed
                logger.info('2 of 3 parallel actions have failed, cancelling and reverting the 3rd one')
                emergency_actions.extend(
                    await self._cancel_and_revert(results[placed[0]], actions[placed[0]])
                )

            # wait for the orders to get filled
            filled = []
            unfilled = []
            placed_results = await asyncio.gather(*[self._wait_to_fill(results[idx]) for idx in placed])
            for i, result in enumerate(placed_results):
                idx = placed[i]
                # update order results in our main results list
                results[idx] = result
                # check how well they got filled
                exec_amount = Decimal(result['executedQty'])
                action = actions[idx]
                if exec_amount == action.quantity:
                    logger.info(f'Action filled: {action}')
                    filled.append(idx)
                elif 0 < exec_amount < action.quantity:
                    logger.info(f'Action partially filled for {exec_amount}: {action}')
                    unfilled.append(idx)
                elif exec_amount == 0:
                    logger.info(f'Action not filled: {action}')
                    unfilled.append(idx)

            # based on how many orders got filled, decide what we do next
            if len(filled) == len(actions):
                logger.info('All actions got filled, perfect!')
            elif len(filled) == 0:
                logger.info('None of the actions got filled')
                if step == 0:
                    logger.info('First step failed, cancelling and reverting...')
                    for idx in unfilled:
                        emergency_actions.extend(await self._cancel_and_revert(results[idx], actions[idx]))
                elif step == 1 and len(action_set.steps) == 3:
                    logger.info('Step 2/3 failed, cancelling and reverting step 2...')
                    idx = unfilled[0]
                    emergency_actions.extend(await self._cancel_and_revert(results[idx], actions[idx]))
                    logger.info('And reverting step 1...')
                    emergency_actions.append(self._revert_action(action_set.steps[0][0]))
                else:
                    logger.info('Last step failed, it will be finalized')
                    for idx in unfilled:
                        emergency_actions.extend(await self._cancel_and_finalize(results[idx], actions[idx]))
                break
            elif len(filled) == 1 and len(actions) > 1:
                logger.info(f'1/{len(actions)} actions got filled, cancelling and reverting...')
                # cancel & revert the unfilled ones
                for idx in unfilled:
                    emergency_actions.extend(await self._cancel_and_revert(results[idx], actions[idx]))
                # and revert the filled one
                emergency_actions.append(self._revert_action(actions[filled[0]]))
                break
            elif len(filled) == 2 and len(actions) == 3:
                logger.info(f'2/3 actions got filled, the last action will be finalized')
                if len(unfilled) > 0:
                    idx = unfilled[0]
                    emergency_actions.extend(await self._cancel_and_finalize(results[idx], actions[idx]))
                else:
                    idx = failed[0]
                    emergency_actions.append(self._finalize_action(actions[idx]))

        # perform emergency actions
        for action in emergency_actions:
            await self._execute_emergency_action(action)

        dispatcher.send(signal='execution_finished', sender=self)
        logger.info('Executor finished')

    async def _execute_emergency_action(self, action):
        logger.info(f'Executing emergency action: {action}...')
        result = await self._execute_action(action)
        if result is None:
            logger.error('Failed to execute emergency action!')
            return
        try:
            status = await self._get_order_status(result)
        except ActionException:
            logger.error(f'Unexpected result of emergency action, server response: {result}')
            return
        if status == 'FILLED':
            logger.info('Emergency action completed')
        else:
            logger.error(f'Emergency action not filled: {action}. Status: {status}')

    def _get_executable_action_set(self) -> ActionSet or None:
        actions = self._raw_action_list
        logger.debug(f'Initial actions list: {actions}')
        # actions list is expected to be exactly three items long, in a triangle
        if len(actions) != 3:
            logger.error(f'Number of actions is not 3: {actions}')
            return None
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
            return None
        logger.debug(f'Sequenced actions list: {actions}')
        # then figure out which action to start with and rotate the sequence
        balance_props = []
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
            balance_props.append(balance / amount)
        prop_min, prop_mid, prop_max = sorted(balance_props)
        idx_min = balance_props.index(prop_min)
        idx_mid = balance_props.index(prop_mid)
        idx_max = balance_props.index(prop_max)
        # availability 3/3: first check the lowest balance/amount proportion
        red_actions = self._reduce_actions_by_proportion(actions, prop_min)
        if red_actions is not None:
            # instant 3/3 available, we'll execute all three actions in parallel
            return ActionSet([red_actions,])
        # availability 2/3:
        red_actions = self._reduce_actions_by_proportion(actions, prop_mid)
        if red_actions is not None:
            # 2/3 available, execute first two in parallel and then third
            return ActionSet([
                [actions[idx_mid], actions[idx_max]],
                [actions[idx_min],]
            ])
        # availability 1/3:
        actions = self._reduce_actions_by_proportion(actions, prop_max)
        if actions is not None:
            # 1/3 available, execute actions sequentially one by one
            # rotate to have the one with available balance first
            shift = -idx_max
            if shift != 0:
                dq = deque(actions)
                # logger.debug(f'Rotating actions list by: {shift}')
                dq.rotate(shift)
                actions = list(dq)
            return ActionSet([
                [actions[0],],
                [actions[1],],
                [actions[2],],
            ])
        return None

    def _reduce_actions_by_proportion(self, actions: List[Action], proportion: Decimal) -> List[Action] or None:
        if proportion < 1:
            if self._detector is None or self._arbitrage is None:
                logger.warning('Action amounts cannot be reduced without a Detector')
                return None
            # recalculate action amounts to fit in our balance and keep the arbitrage profitable
            logger.debug(f'Reducing the arbitrage by: {proportion}')
            reduced = self._detector.reduce_arbitrage(
                arb=self._arbitrage,
                reduce_factor=proportion
            )
            if reduced is None:
                logger.debug('Arbitrage is not available with reduced amounts')
                return None
            # extract amounts from the reduced arbitrage
            for action in actions:
                a = next((
                    a for a in reduced.actions if a.pair == action.pair and a.action.upper() == action.side
                ), None)
                action.quantity = a.amount
            logger.debug(f'Reduced arbitrage actions list: {actions}')
        return actions

    async def _execute_action(self, action: Action):
        try:
            return await self._api.create_order(
                symbol=action.symbol,
                side=action.side,
                order_type=action.type,
                quantity=f'{action.quantity:f}',
                price=None if action.price is None else f'{action.price:f}',
            )
        except BinanceAPIException as e:
            logger.error(f'Action failed: {action}. Reason: {e}')
            return None

    async def _get_order_status(self, order_result) -> str or None:
        try:
            status = order_result['status']
        except KeyError:
            raise ActionException
        if status == 'NEW':
            symbol = order_result['symbol']
            order_id = order_result['orderId']
            # Order statuses in Binance:
            #   NEW, PARTIALLY_FILLED, FILLED, CANCELED, REJECTED, EXPIRED
            order_result = await self._api.order_info(symbol, order_id)
            if order_result and 'status' in order_result:
                status = order_result['status']
        return status

    async def _wait_to_fill(self, order_result: dict) -> dict:
        """Returns amount that got filled, the rest is considered failed to fill"""

        symbol = order_result['symbol']
        order_id = order_result['orderId']
        status = order_result['status']
        price = Decimal(order_result['price'])
        side = order_result['side']
        if status in ['NEW', 'PARTIALLY_FILLED']:
            # keep checking until it gets filled or lost in the book
            while 1:
                try:
                    order_result = await self._api.order_info(symbol, order_id)
                except BinanceAPIException as e:
                    logger.error(f'Failed to get order info, order: {symbol:order_id}, error: {e}')
                else:
                    try:
                        status = order_result['status']
                    except (TypeError, KeyError):
                        logger.warning(f'Checking placed order status failed: {order_result}')
                    if status == 'FILLED':
                        break
                    elif status not in ['NEW', 'PARTIALLY_FILLED']:
                        logger.error(f'Unexpected order status: {status}')
                        break
                    # give up if the order is lost in the book
                    amount_left = Decimal(order_result['origQty']) - Decimal(order_result['executedQty'])
                    if amount_left > 0:
                        vol_in_front = self._detector.get_book_volume_in_front(symbol, price, side)
                        rel_in_front = vol_in_front / amount_left
                        if rel_in_front >= 1:
                            logger.info(
                                f'Order {symbol}:{order_id} is lost in the book: '
                                f'{int(rel_in_front*100)}% of unfilled amount'
                                f' is already in front of the order'
                            )
                            break
                await asyncio.sleep(CHECK_ORDER_INTERVAL)
        return order_result

    def _revert_action(self, action: Action) -> Action:
        qty_filter = self._symbols_info[action.symbol].get_qty_filter()
        amount_step = Decimal(qty_filter[2]).normalize()
        amount_revert = (action.quantity * (1 - TRADE_FEE)).quantize(amount_step, rounding=ROUND_DOWN)
        return Action(
            pair=action.pair,
            side='BUY' if action.side == 'SELL' else 'SELL',
            quantity=amount_revert,
            order_type='MARKET'
        )

    def _finalize_action(self, action: Action) -> Action:
        return Action(
            pair=action.pair,
            side=action.side,
            quantity=action.quantity,
            order_type='MARKET'
        )

    async def _cancel_order(self, order_result: dict) -> Decimal:
        """Returns amount filled prior to cancellation"""

        symbol = order_result['symbol']
        order_id = order_result['orderId']
        status = order_result['status']
        if status in ['NEW', 'PARTIALLY_FILLED']:
            # cancel the order
            logger.info(f'Cancelling order {symbol}:{order_id}...')
            cancel_result = await self._api.cancel_order(symbol, order_id)
            amount_filled = 0
            try:
                amount_filled = Decimal(cancel_result['executedQty'])
                logger.info(f'Order cancelled, executed amount: {amount_filled:f}')
            except KeyError:
                if cancel_result['msg'] == 'Unknown order sent.':
                    logger.info(f'Order {symbol} {order_id} not found, already completed?')
                    # check if order is already completed
                    r = await self._api.order_info(symbol, order_id)
                    try:
                        status = r['status']
                    except (TypeError, KeyError):
                        logger.error(f'Checking failed-to-cancel order status failed: {r}')
                    else:
                        if status == 'FILLED':
                            amount_filled = Decimal(r['executedQty'])
                        else:
                            logger.error(f'Unexpected failed-to-cancel order status: {status}')
                else:
                    logger.error(f'Order cancellation failed, response: {cancel_result}')
        else:
            amount_filled = order_result['executedQty']
        return amount_filled

    async def _cancel_and_revert(self, order_result: dict, action: Action) -> List[Action]:
        """Returns a list of emergency actions or an empty list"""

        # cancel the order
        amount_filled = await self._cancel_order(order_result)
        # revert what's been filled
        if amount_filled > 0:
            logger.info(f'Order has been filled for {amount_filled:f} {action.base}, it will be reverted')
            qty_filter = self._symbols_info[action.symbol].get_qty_filter()
            amount_step = Decimal(qty_filter[2]).normalize()
            amount_revert = (amount_filled * (1 - TRADE_FEE)).quantize(amount_step, rounding=ROUND_DOWN)
            return [
                Action(
                    pair=action.pair,
                    side='BUY' if action.side == 'SELL' else 'SELL',
                    quantity=amount_revert,
                    order_type='MARKET'
                )
            ]
        return []

    async def _cancel_and_finalize(self, order_result: dict, action: Action) -> List[Action]:
        """Returns a list of emergency actions or an empty list"""

        # cancel the order
        amount_filled = await self._cancel_order(order_result)
        # finalize what's unfilled
        if amount_filled < action.quantity:
            qty_filter = self._symbols_info[action.symbol].get_qty_filter()
            amount_step = Decimal(qty_filter[2]).normalize()
            amount_to_finalize = (action.quantity - amount_filled).quantize(amount_step, rounding=ROUND_DOWN)
            logger.info(
                f'Order has been filled for {amount_filled:f} {action.base}, '
                f'to be finalized: {amount_to_finalize:f} {action.base}'
            )
            return [
                Action(
                    pair=action.pair,
                    side=action.side,
                    quantity=amount_to_finalize,
                    order_type='MARKET'
                )
            ]
        return []




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
        Action(
            pair=('BTC', 'USDT'),
            side='SELL',
            quantity=Decimal('0.002'),
            price=Decimal('5000.0'),
            order_type='LIMIT'
        ),
        Action(
            pair=('ETH', 'USDT'),
            side='BUY',
            quantity=Decimal('0.05'),
            price=Decimal('200'),
            order_type='LIMIT'
        ),
        Action(
            pair=('ETH', 'BTC'),
            side='SELL',
            quantity=Decimal('0.05'),
            price=Decimal('0.01'),
            order_type='LIMIT'
        )
    ]

    executor = BinanceActionExecutor(api=api, actions=actions, symbols_info=symbols_info, account_info=acc)
    dispatcher.connect(test_on_execution_finished, signal='execution_finished', sender=executor)
    await executor.run()

    await asyncio.sleep(5)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
