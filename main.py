import asyncio
import signal
from pydispatch import dispatcher
from config import config, get_exchange_class
from account_info import AccountInfo
from exchanges.base_exchange import BaseExchange
from arbitrage_detector import ArbitrageDetector, Arbitrage
from action_executor import ActionExecutor, Action
from aftermath import Aftermath
from logger import get_logger
logger = get_logger(__name__)


class TriangularArbitrage:
    def __init__(self, exchange: BaseExchange, account_info: AccountInfo):
        self.stopped = asyncio.Event()
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
        self._executor = None
        self._aftermath_done = asyncio.Event()
        self._were_not_normal = 0   # counter for the circuit breaker
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

    async def stop(self):
        dispatcher.disconnect(self._process_arbitrage, signal='arbitrage_detected', sender=self._detector)
        self._detector.stop()
        logger.info('Detector stopped')
        if self._executor is not None:
            await self._executor.stop()
            logger.info('Executor stopped')
            await self._aftermath_done.wait()
        self._account_info.stop()
        logger.info('Account Info stopped')
        await self._exchange.stop()
        logger.info('Exchange stopped')
        self.stopped.set()

    def _on_aftermath_done(self, sender: Aftermath):
        logger.info('Aftermath done')
        self._aftermath_done.set()

    def _on_arbitrage_processed(self, sender: ActionExecutor):
        logger.info('Arbitrage processed')
        self._is_processing = False
        actions = sender.get_raw_action_list()
        result = sender.get_result()
        if result is None:
            logger.info('No result, no aftermath')
            return
        aftermath = Aftermath(self._exchange, actions, result)
        dispatcher.connect(self._on_aftermath_done, signal='aftermath_done', sender=aftermath)
        asyncio.ensure_future(aftermath.run())
        self._executor = None
        # circuit breaker
        if result.scenario == 'normal':
            self._were_not_normal = 0
        else:
            self._were_not_normal += 1
            if self._were_not_normal >= config.getint('CircuitBreaker', 'NoNormalsInARow'):
                logger.warning(f'{self._were_not_normal} arbs were not completed normally, stopping...')
                asyncio.ensure_future(self.stop())


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
        self._executor = ActionExecutor(
            exchange=self._exchange,
            actions=actions,
            account_info=self._account_info,
            detector=self._detector,
            arbitrage=arb
        )
        dispatcher.connect(self._on_arbitrage_processed, signal='execution_finished', sender=self._executor)
        asyncio.ensure_future(self._executor.run())
        self._aftermath_done = asyncio.Event()


async def main():
    logger.info('Starting...')

    triarb = await TriangularArbitrage.create()

    # Graceful termination on SIGINT/SIGTERM. Supervisord sends SIGTERM when you click Stop.
    def graceful_stop(signum, frame):
        signame = signal.Signals(signum).name
        logger.info(f'Received signal: {signame}. Terminating...')
        asyncio.ensure_future(triarb.stop())

    signal.signal(signal.SIGINT, graceful_stop)
    signal.signal(signal.SIGTERM, graceful_stop)

    await triarb.stopped.wait()

    logger.info('Stopped')


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
