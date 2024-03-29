import time
import asyncio
from pydispatch import dispatcher
from decimal import Decimal, ROUND_DOWN
from typing import List, Tuple
from collections import deque
from config import config, get_exchange_class
from exchanges.base_exchange import BaseExchange
from account_info import AccountInfo
from arbitrage_detector import ArbitrageDetector, Arbitrage
from logger import get_logger
logger = get_logger(__name__)


class ActionError(Exception):
    def __init__(self, message):
        self.message = message


class Action:
    def __init__(self, pair: Tuple[str, str], side: str, quantity, price=None, order_type='LIMIT'):
        self.pair = pair
        self.base = pair[0]
        self.quote = pair[1]
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


class ActionExecutor:

    class Error(BaseException):
        def __init__(self, message):
            self.message = message

    class Result:
        def __init__(self, parallels: int, scenario: str, order_results: list, action_orders: list, timings: dict):
            self.parallels = parallels
            self.scenario = scenario
            self.order_results = order_results
            self.action_orders = action_orders
            self.timings = timings

    def __init__(self, exchange: BaseExchange, actions: List[Action], detector: ArbitrageDetector = None,
                 arbitrage: Arbitrage = None, account_info: AccountInfo = None):
        self._exchange = exchange
        self._symbols_info = exchange.get_symbols_info()
        self._raw_action_list = actions
        self._account_info = account_info
        self._detector = detector
        self._arbitrage = arbitrage
        self._trade_fee = config.getdecimal('Exchange', 'TradeFee')
        self._min_fill_time = config.getint('Arbitrage', 'MinFillTime')
        self._min_fill_time_last = config.getint('Arbitrage', 'MinFillTimeLast')
        self._max_fill_time = config.getint('Arbitrage', 'MaxFillTime')
        self._min_parallel_actions = config.getint('Arbitrage', 'MinParallelActions')
        self._result = None
        self._orders_executed = []
        self._action_orders = []
        self._timings = {
            'all_placed': -1,
            'orders_placed': [-1, -1, -1],
            'orders_done': [-1, -1, -1],
            'completed': -1
        }
        self._stopping = False
        self._stopped = asyncio.Event()

    async def run(self):
        # init account info if it hasn't been passed from above
        if self._account_info is None:
            self._account_info = await AccountInfo.create(self._exchange)

        # prepare a set of actions for execution
        try:
            action_set = self._get_executable_action_set()
        except self.Error as e:
            logger.info(f'Cannot execute these actions: {e.message}')
            dispatcher.send(signal='execution_finished', sender=self)
            return
        logger.info(f'Executable action set:\n{action_set}')

        scenario = ''

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
            for idx, ores in enumerate(results):
                action = actions[idx]
                if ores is None:
                    # order creation failed
                    logger.error(f'Failed to place an order! Failed action: {action}')
                    failed.append(idx)
                else:
                    logger.info(f'Action order placed: {action}')
                    placed.append(idx)
            if len(failed) == 0:
                logger.info('All actions at this step placed orders successfully')
                # shall proceed to filling
            elif len(failed) == len(actions):
                logger.info('All actions at this step have failed')
                if step == 0:
                    logger.info('Aborting')
                    scenario = 'failed'
                elif step == 1 and len(action_set.steps) == 3:
                    logger.info('Step 1 will be reverted')
                    emergency_actions.append(self._revert_action(action_set.steps[0][0]))
                    scenario = 'reverted 1'
                else:
                    logger.info('Last step failed, it will be finalized')
                    emergency_actions.append(self._finalize_action(actions[failed[0]]))
                    scenario = 'finalized'
                break
            elif len(failed) == 1 and len(actions) == 2:
                # 1 of 2 failed
                logger.info('1 of 2 parallel actions has failed, cancelling and reverting the other one')
                to_revert = await self._cancel_and_revert(results[placed[0]], actions[placed[0]])
                emergency_actions.extend(to_revert)
                scenario = 'reverted 1' if len(to_revert) > 0 else 'failed'
                break
            elif len(failed) == 1 and len(actions) == 3:
                # 1 of 3 failed
                logger.info('1 of 3 parallel actions has failed, we will wait for the results of the other two')
                # first we'll let the other 2 get filled, and if they do, we'll finalize this one
            elif len(failed) == 2 and len(actions) == 3:
                # 2 of 3 failed
                logger.info('2 of 3 parallel actions have failed, cancelling and reverting the 3rd one')
                to_revert = await self._cancel_and_revert(results[placed[0]], actions[placed[0]])
                emergency_actions.extend(to_revert)
                scenario = 'reverted 1' if len(to_revert) > 0 else 'failed'
                break

            if step + 1 == len(action_set.steps):
                # last step has placed its orders
                self._timings['all_placed'] = int(time.time() * 1000) - self._arbitrage.ts

            # wait for the orders to get filled
            filled = []
            unfilled = []
            min_filling_time = self._min_fill_time_last if step == len(action_set.steps) - 1 else self._min_fill_time
            placed_results = [results[idx] for idx in placed]
            for pres in placed_results:
                self._action_orders.append((pres.symbol, pres.order_id))

            # let them fill
            placed_results = await self._wait_all_to_fill(placed_results, min_filling_time, self._max_fill_time)

            for i, ores in enumerate(placed_results):
                idx = placed[i]
                # update order results in our main results list
                results[idx] = ores
                # check how well they got filled
                action = actions[idx]
                # if result.amount_executed == action.quantity:
                if ores.status == 'FILLED':
                    logger.info(f'Action filled: {action}')
                    filled.append(idx)
                elif ores.status == 'PARTIALLY_FILLED':
                    logger.info(f'Action partially filled for {ores.amount_executed}: {action}')
                    unfilled.append(idx)
                elif ores.status == 'NEW':
                    logger.info(f'Action not filled: {action}')
                    unfilled.append(idx)
                else:
                    logger.info(f'Unexpected action result: {ores.status}, '
                                f'filled for {ores.amount_executed}: {action}')
                # timing if it's filled already
                if ores.done_at > 0:
                    self._set_done_timing(ores, ores.done_at)

            # based on how many orders got filled, decide what we do next
            if len(filled) == len(actions):
                logger.info('All actions got filled, perfect!')
                # shall continue to next steps if any
            elif len(filled) == 0:
                logger.info('None of the actions got filled')
                if step == 0:
                    logger.info('First step failed, cancelling and reverting...')
                    to_revert = []
                    for idx in unfilled:
                        # cancel
                        to_revert.extend(await self._cancel_and_revert(results[idx], actions[idx]))
                        # timing
                        self._set_done_timing(results[idx])
                    # revert
                    emergency_actions.extend(to_revert)
                    scenario = f'reverted {len(to_revert)}' if len(to_revert) > 0 else 'unfilled'
                elif step == 1 and len(action_set.steps) == 3:
                    logger.info('Step 2/3 failed, cancelling and reverting step 2...')
                    idx = unfilled[0]
                    # cancel
                    to_revert = await self._cancel_and_revert(results[idx], actions[idx])
                    # timing
                    self._set_done_timing(results[idx])
                    # revert
                    emergency_actions.extend(to_revert)
                    logger.info('And reverting step 1...')
                    emergency_actions.append(self._revert_action(action_set.steps[0][0]))
                    scenario = 'reverted 2' if len(to_revert) > 0 else 'reverted 1'
                else:
                    logger.info('Last step failed, it will be finalized')
                    to_finalize = []
                    for idx in unfilled:
                        # cancel
                        to_finalize.extend(await self._cancel_and_finalize(results[idx], actions[idx]))
                        # timing
                        self._set_done_timing(results[idx])
                    # finalize
                    emergency_actions.extend(to_finalize)
                    scenario = 'finalized' if len(to_finalize) > 0 else 'normal'
                break
            elif len(filled) == 1 and len(actions) > 1:
                logger.info(f'1/{len(actions)} actions got filled, cancelling and reverting...')
                # cancel & revert the unfilled ones
                to_revert = []
                for idx in unfilled:
                    # cancel
                    to_revert.extend(await self._cancel_and_revert(results[idx], actions[idx]))
                    # timing
                    self._set_done_timing(results[idx])
                # and revert if anything filled and revert the filled other one
                to_revert.append(self._revert_action(actions[filled[0]]))
                emergency_actions.extend(to_revert)
                scenario = f'reverted {len(to_revert)}'
                break
            elif len(filled) == 2 and len(actions) == 3:
                logger.info(f'2/3 actions got filled, the last action will be finalized')
                if len(unfilled) > 0:
                    idx = unfilled[0]
                    # cancel
                    to_finalize = await self._cancel_and_finalize(results[idx], actions[idx])
                    # timing
                    self._set_done_timing(results[idx])
                    # finalize
                    emergency_actions.extend(to_finalize)
                    scenario = 'finalized' if len(to_finalize) > 0 else 'normal'
                else:
                    idx = failed[0]
                    emergency_actions.append(self._finalize_action(actions[idx]))
                    scenario = 'finalized'
                break

        # scenario not set means the loop above did not break and all orders got filled normally
        if scenario == '':
            scenario = 'normal'

        # measure timings
        for ores in self._orders_executed:
            placed_in = ores.placed_at - self._arbitrage.ts
            j = self._action_orders.index((ores.symbol, ores.order_id))
            self._timings['orders_placed'][j] = placed_in

        # perform emergency actions
        for action in emergency_actions:
            await self._execute_emergency_action(action)

        self._timings['completed'] = int(time.time() * 1000) - self._arbitrage.ts

        # summarize arbitrage execution results
        self._result = self.Result(
            parallels=len(action_set.steps[0]),
            scenario=scenario,
            order_results=self._orders_executed,
            action_orders=self._action_orders,
            timings=self._timings
        )

        dispatcher.send(signal='execution_finished', sender=self)

        if self._stopping:
            self._stopped.set()

    async def stop(self):
        self._stopping = True
        await self._stopped.wait()

    def get_result(self):
        return self._result

    def get_raw_action_list(self):
        return self._raw_action_list

    def _set_done_timing(self, ores: BaseExchange.OrderResult, done_at: int = 0):
        if done_at == 0:
            done_at = int(time.time() * 1000)
        j = self._action_orders.index((ores.symbol, ores.order_id))
        done_in = done_at - (self._arbitrage.ts + self._timings['orders_placed'][j])
        self._timings['orders_done'][j] = done_in

    async def _execute_emergency_action(self, action):
        logger.info(f'Executing emergency action: {action}...')
        result = await self._execute_action(action)
        if result is None:
            logger.error('Failed to execute emergency action!')
            return
        try:
            status = await self._get_order_status(result)
        except ActionError:
            logger.error(f'Unexpected result of emergency action, server response: {result}')
            return
        if status == 'FILLED':
            logger.info('Emergency action completed')
        else:
            logger.error(f'Emergency action not filled: {action}. Status: {status}')

    def _get_executable_action_set(self) -> ActionSet:
        actions = self._raw_action_list
        # logger.debug(f'Initial actions list: {actions}')
        # actions list is expected to be exactly three items long, in a triangle
        if len(actions) != 3:
            raise self.Error(f'Number of actions is not 3: {actions}')
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
            raise self.Error(f'Bad actions list: not a valid triangle! Actions: {actions}')
        # logger.debug(f'Sequenced actions list: {actions}')
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
        logger.debug(f'Balance proportions: {balance_props}')
        idx_min = balance_props.index(prop_min)
        idx_mid = balance_props.index(prop_mid)
        idx_max = balance_props.index(prop_max)
        # availability 3/3: first check the lowest balance/amount proportion
        red_actions = self._reduce_actions_by_proportion(actions, prop_min)
        if red_actions is not None:
            # instant 3/3 available, we'll execute all three actions in parallel
            return ActionSet([red_actions,])
        if self._min_parallel_actions == 3:
            raise self.Error('No available balance for 3 parallel actions')
        # availability 2/3:
        red_actions = self._reduce_actions_by_proportion(actions, prop_mid)
        if red_actions is not None:
            # 2/3 available, execute first two in parallel and then third
            return ActionSet([
                [red_actions[idx_mid], red_actions[idx_max]],
                [red_actions[idx_min],]
            ])
        if self._min_parallel_actions == 2:
            raise self.Error('No available balance for 2 or 3 parallel actions')
        # availability 1/3:
        red_actions = self._reduce_actions_by_proportion(actions, prop_max)
        if red_actions is not None:
            # 1/3 available, execute actions sequentially one by one
            # rotate to have the one with available balance first
            shift = -idx_max
            if shift != 0:
                dq = deque(red_actions)
                # logger.debug(f'Rotating actions list by: {shift}')
                dq.rotate(shift)
                red_actions = list(dq)
            return ActionSet([
                [red_actions[0],],
                [red_actions[1],],
                [red_actions[2],],
            ])
        # no available balance
        raise self.Error('No available balance')

    def _reduce_actions_by_proportion(self, actions: List[Action], proportion: Decimal) -> List[Action] or None:
        if proportion < 1:
            if self._detector is None or self._arbitrage is None:
                logger.warning('Action amounts cannot be reduced without a Detector')
                return None
            # recalculate action amounts to fit in our balance and keep the arbitrage profitable
            # logger.debug(f'Reducing the arbitrage by: {proportion}')
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
            # logger.debug(f'Reduced arbitrage actions list: {actions}')
        return actions

    async def _execute_action(self, action: Action) -> BaseExchange.OrderResult or None:
        try:
            symbol = self._exchange.make_symbol(action.pair[0], action.pair[1])
            ores = await self._exchange.create_order(
                symbol=symbol,
                side=action.side,
                order_type=action.type,
                amount=action.quantity,
                price=action.price,
            )
            ores.placed_at = int(time.time() * 1000)
            self._orders_executed.append(ores)
            return ores
        except BaseExchange.Error as e:
            logger.error(f'Action failed: {action}. Reason: {e.message}')
            return None

    async def _get_order_status(self, order_result: BaseExchange.OrderResult) -> str:
        try:
            status = order_result.status
        except KeyError:
            raise ActionError('Status not found')
        if status == 'NEW':
            try:
                order_result = await self._exchange.get_order_result(order_result.symbol, order_result.order_id)
            except BaseExchange.Error as e:
                raise ActionError(f'Order info failed: {e.message}')
            status = order_result.status
        return status

    async def _wait_to_fill(self, ores: BaseExchange.OrderResult,
                            min_filling_time: int, max_filling_time: int) -> BaseExchange.OrderResult:
        """Returns result with amount that got filled, the rest is considered failed to fill"""

        if ores.status in ['NEW', 'PARTIALLY_FILLED']:
            started = time.time()
            # keep checking until it gets filled or lost in the book
            while not self._stopping:
                try:
                    ores = await self._exchange.get_order_result(ores.symbol, ores.order_id)
                except BaseExchange.Error as e:
                    logger.error(f'Failed to get order info, order: {ores.symbol}:{ores.order_id}, error: {e.message}')
                else:
                    if ores.status == 'FILLED':
                        break
                    elif ores.status not in ['NEW', 'PARTIALLY_FILLED']:
                        logger.error(f'Unexpected order status: {ores.status}')
                        break
                    if time.time() - started > min_filling_time:
                        # give up if the order is lost in the book
                        amount_left = ores.amount_original - ores.amount_executed
                        if amount_left > 0:
                            vol_in_front = self._detector.get_book_volume_in_front(ores.symbol, ores.price, ores.side)
                            rel_in_front = vol_in_front / amount_left
                            if rel_in_front >= 1:
                                logger.info(
                                    f'Order {ores.symbol}:{ores.order_id} is lost in the book: '
                                    f'{int(rel_in_front*100)}% of unfilled amount'
                                    f' is already in front of the order'
                                )
                                break
                    if time.time() - started > max_filling_time:
                        logger.info(
                            f'Max waiting time reached, order filled by '
                            f'{ores.amount_executed:f} of {ores.amount_original:f}'
                        )
                        break
                await asyncio.sleep(config.getint('Arbitrage', 'CheckOrderInterval'))
        if ores.status == 'FILLED':
            ores.done_at = int(time.time() * 1000)
        return ores

    async def _refresh_order_results(self, old_results: List[BaseExchange.OrderResult]):
        # check each order's result afresh
        order_results = []
        for old_r in old_results:
            try:
                if old_r.status in ['NEW', 'PARTIALLY_FILLED']:
                    new_r = await self._exchange.get_order_result(old_r.symbol, old_r.order_id)
                else:
                    new_r = old_r
            except BaseExchange.Error as e:
                logger.error(f'Failed to get order info, order: {old_r.symbol}:{old_r.order_id}, error: {e.message}')
                order_results.append(old_r)
            else:
                order_results.append(new_r)
        return order_results

    async def _wait_all_to_fill(self, old_results: list, min_filling_time: int, max_filling_time: int) -> list:
        # wait to fill each
        old_results = await asyncio.gather(
            *[self._wait_to_fill(result, min_filling_time, max_filling_time) for result in old_results]
        )
        # check each order's result one more time, as it may have changed after waiting
        return await self._refresh_order_results(old_results)

    def _revert_action(self, action: Action) -> Action:
        symbol = self._exchange.make_symbol(action.pair[0], action.pair[1])
        amount_step = self._symbols_info[symbol]['amount_step']
        amount_revert = (action.quantity * (1 - self._trade_fee)).quantize(amount_step, rounding=ROUND_DOWN)
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

    async def _cancel_order(self, ores: BaseExchange.OrderResult) -> Decimal:
        """Returns amount filled prior to cancellation"""

        if ores.status in ['NEW', 'PARTIALLY_FILLED']:
            # cancel the order
            logger.info(f'Cancelling order {ores.symbol}:{ores.order_id}...')
            amount_filled = Decimal(0)
            try:
                cancel_res = await self._exchange.cancel_order(ores.symbol, ores.order_id)
            except BaseExchange.OrderNotFound:
                logger.info(f'Order {ores.symbol}:{ores.order_id} not found, already completed?')
                # check if order is already completed
                try:
                    r = await self._exchange.get_order_result(ores.symbol, ores.order_id)
                except BaseExchange.Error as e:
                    logger.error(f'Checking failed-to-cancel order status failed: {e.message}')
                else:
                    if r.status == 'FILLED':
                        amount_filled = r.amount_executed
                    else:
                        logger.error(f'Unexpected failed-to-cancel order status: {r.status}')
            except BaseExchange.Error as e:
                logger.error(f'Order cancellation failed: {e.message}')
            else:
                amount_filled = cancel_res.amount_executed
                logger.info(f'Order cancelled, executed amount: {amount_filled:f}')
        else:
            logger.info(f'Not cancelling order {ores.symbol}:{ores.order_id}, status: {ores.status}')
            amount_filled = ores.amount_executed
        return amount_filled

    async def _cancel_and_revert(self, ores: BaseExchange.OrderResult, action: Action) -> List[Action]:
        """Returns a list of emergency actions or an empty list"""

        # cancel the order
        amount_filled = await self._cancel_order(ores)
        # revert what's been filled
        if amount_filled > 0:
            logger.info(f'Order has been filled for {amount_filled:f} {action.base}, it will be reverted')
            symbol = self._exchange.make_symbol(action.pair[0], action.pair[1])
            amount_step = self._symbols_info[symbol]['amount_step']
            amount_revert = (amount_filled * (1 - self._trade_fee)).quantize(amount_step, rounding=ROUND_DOWN)
            return [
                Action(
                    pair=action.pair,
                    side='BUY' if action.side == 'SELL' else 'SELL',
                    quantity=amount_revert,
                    order_type='MARKET'
                )
            ]
        return []

    async def _cancel_and_finalize(self, ores: BaseExchange.OrderResult, action: Action) -> List[Action]:
        """Returns a list of emergency actions or an empty list"""

        # cancel the order
        amount_filled = await self._cancel_order(ores)
        # finalize what's unfilled
        if amount_filled < action.quantity:
            symbol = self._exchange.make_symbol(action.pair[0], action.pair[1])
            amount_step = self._symbols_info[symbol]['amount_step']
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
    from config import config

    # it will try to execute a demonstratory set of actions
    # they won't give actual profit, it's just to test that it all works

    exchange_class = get_exchange_class()
    exchange = await exchange_class.create(
        config.get('Exchange', 'APIKey'),
        config.get('Exchange', 'APISecret')
    )
    acc = await AccountInfo.create(exchange, auto_update_interval=10)
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

    executor = ActionExecutor(exchange=exchange, actions=actions, account_info=acc)
    dispatcher.connect(test_on_execution_finished, signal='execution_finished', sender=executor)
    await executor.run()

    await asyncio.sleep(5)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
