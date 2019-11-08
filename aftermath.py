from datetime import datetime
from typing import List
from decimal import Decimal
from pydispatch import dispatcher
from config import config
from exchanges.base_exchange import BaseExchange
from action_executor import ActionExecutor, Action
from database import DBArbResult
from logger import get_logger
logger = get_logger(__name__)


class Aftermath:
    def __init__(self, exchange: BaseExchange, actions: List[Action], result: ActionExecutor.Result):
        self._exchange = exchange
        self._actions = actions
        self._result = result

    async def run(self):
        if self._result is None:
            dispatcher.send(signal='aftermath_done', sender=self)
            return

        assets = set()
        pairs = {}
        for action in self._actions:
            assets.add(action.base)
            assets.add(action.quote)
            symbol = self._exchange.make_symbol(action.base, action.quote)
            pairs[symbol] = (action.base, action.quote)
        assets = sorted(assets)
        triangle = ', '.join(assets)

        profits = {
            assets[0]: Decimal(0),  # profit A
            assets[1]: Decimal(0),  # profit B
            assets[2]: Decimal(0)   # profit C
        }
        fillings = [-1, -1, -1]
        for ores in self._result.order_results:
            try:
                ores = await self._exchange.get_order_result(ores.symbol, ores.order_id)
            except BaseExchange.Error as e:
                logger.error(f'Failed to get order result: {ores.symbol}:{ores.order_id}. Reason: {e.message}')
            base, quote = pairs[ores.symbol]
            # calculate profit if information is available
            if ores.amount_quote is not None:
                trade_fee = config.getdecimal('Exchange', 'TradeFee')
                if ores.side == 'BUY':
                    profits[base] += ores.amount_executed * (1 - trade_fee)
                    profits[quote] -= ores.amount_quote
                elif ores.side == 'SELL':
                    profits[base] -= ores.amount_executed
                    profits[quote] += ores.amount_quote * (1 - trade_fee)
                else:
                    logger.error(f'Bad side: {ores.side}')
                    return
            # if it's an acton order (not an emergency order), calculate its filling
            if (ores.symbol, ores.order_id) in self._result.action_orders:
                idx = self._result.action_orders.index((ores.symbol, ores.order_id))
                if ores.status == 'NEW':
                    logger.warning(f'Aftermath on an order that is still open: {ores.symbol}:{ores.order_id}')
                    fillings[idx] = 0.0
                elif ores.status in ['PARTIALLY_FILLED', 'CANCELLED']:
                    fillings[idx] = float(ores.amount_executed / ores.amount_original)
                elif ores.status == 'FILLED':
                    fillings[idx] = 1.0

        DBArbResult.create(
            dt = datetime.utcnow(),
            triangle = triangle,
            parallels = self._result.parallels,
            scenario = self._result.scenario,
            profit_a = profits[assets[0]],
            profit_b = profits[assets[1]],
            profit_c = profits[assets[2]],
            filling_1 = fillings[0],
            filling_2 = fillings[1],
            filling_3 = fillings[2],
            all_placed_in = self._result.timings['all_placed'],
            placed_1_in = self._result.timings['orders_placed'][0],
            placed_2_in = self._result.timings['orders_placed'][1],
            placed_3_in = self._result.timings['orders_placed'][2],
            done_1_in = self._result.timings['orders_done'][0],
            done_2_in = self._result.timings['orders_done'][1],
            done_3_in = self._result.timings['orders_done'][2],
            completed_in = self._result.timings['completed']
        )

        dispatcher.send(signal='aftermath_done', sender=self)
