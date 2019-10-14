import asyncio
from pydispatch import dispatcher
from config import config, get_exchange_class
from account_info import AccountInfo
from exchanges.base_exchange import BaseExchange
from arbitrage_detector import ArbitrageDetector, Arbitrage
from action_executor import ActionExecutor, Action
from logger import get_logger
logger = get_logger(__name__)


class TriangularArbitrage:
    def __init__(self, exchange: BaseExchange, account_info: AccountInfo):
        self._exchange = exchange
        self._account_info = account_info
        self._is_processing = False
        self._detector = ArbitrageDetector(
            exchange=self._exchange,
            fee=config.getdecimal('Exchange', 'TradeFee'),
            min_profit=config.getdecimal('Arbitrage', 'MinProfit'),
            min_depth=config.getint('Arbitrage', 'MinArbDepth'),
            min_age=config.getint('Arbitrage', 'MinArbAge'),
            reduce_factor=config.getdecimal('Arbitrage', 'AmountReduceFactor')
        )
        dispatcher.connect(self._process_arbitrage, signal='arbitrage_detected', sender=self._detector)

    @classmethod
    async def create(cls):
        exchange_class = get_exchange_class()
        exchange = await exchange_class.create(
            config.get('Exchange', 'APIKey'),
            config.get('Exchange', 'APISecret')
        )
        acc = await AccountInfo.create(exchange, auto_update_interval=10)
        return cls(exchange, acc)

    def _on_arbitrage_processed(self, sender):
        logger.info('Arbitrage processed')
        self._is_processing = False

    def _process_arbitrage(self, arb: Arbitrage):
        if self._is_processing:
            return
        else:
            self._is_processing = True
        logger.info(f'Processing arbitrage: {arb}')
        actions = []
        for action in arb.actions:
            actions.append(
                Action(
                    pair=action.pair,
                    side=action.action.upper(),
                    quantity=action.amount,
                    price=action.price,
                    order_type='LIMIT'
                )
            )
        executor = ActionExecutor(
            exchange=self._exchange,
            actions=actions,
            account_info=self._account_info,
            detector=self._detector,
            arbitrage=arb
        )
        dispatcher.connect(self._on_arbitrage_processed, signal='execution_finished', sender=executor)
        asyncio.ensure_future(executor.run())


async def main():
    logger.info('Starting...')
    triarb = await TriangularArbitrage.create()

    while True:
        await asyncio.sleep(1)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
