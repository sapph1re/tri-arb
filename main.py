import asyncio
from pydispatch import dispatcher
from config import API_KEY, API_SECRET, TRADE_FEE, MIN_PROFIT, MIN_ARBITRAGE_DEPTH, MIN_ARBITRAGE_AGE
from binance_api import BinanceApi
from binance_account_info import BinanceAccountInfo
from arbitrage_detector import ArbitrageDetector, Arbitrage
from action_executor import BinanceActionExecutor, Action
from logger import get_logger
logger = get_logger(__name__)


class TriangularArbitrage:
    def __init__(self, api: BinanceApi, account_info: BinanceAccountInfo, symbols_info: dict):
        self._api = api
        self._account_info = account_info
        self._symbols_info = symbols_info
        self._is_processing = False
        self._detector = ArbitrageDetector(
            api=self._api,
            symbols_info=symbols_info,
            fee=TRADE_FEE,
            min_profit=MIN_PROFIT,
            min_depth=MIN_ARBITRAGE_DEPTH,
            min_age=MIN_ARBITRAGE_AGE
        )
        dispatcher.connect(self._process_arbitrage, signal='arbitrage_detected', sender=self._detector)

    @classmethod
    async def create(cls):
        api = await BinanceApi.create(API_KEY, API_SECRET)
        acc = await BinanceAccountInfo.create(api, auto_update_interval=10)
        symbols_info = await api.get_symbols_info()
        return cls(api, acc, symbols_info)

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
        executor = BinanceActionExecutor(
            api=self._api,
            actions=actions,
            symbols_info=self._symbols_info,
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
