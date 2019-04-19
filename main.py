import sys
from config import API_KEY, API_SECRET, TRADE_FEE, MIN_PROFIT
from binance_api import BinanceApi
from arbitrage_detector import ArbitrageDetector, Arbitrage
from binance_actions_executor import BinanceActionsExecutor, BinanceSingleAction
from PyQt5.QtCore import QCoreApplication, QObject
from custom_logging import get_logger
logger = get_logger(__name__)


class TriangularArbitrage(QObject):

    def __init__(self, api: BinanceApi, parent=None):
        super(TriangularArbitrage, self).__init__(parent)
        self.__api = api
        self.__is_processing = False
        self.__symbols_info = self.__api.get_symbols_info()
        self.__detector = ArbitrageDetector(
            api=self.__api,
            symbols_info=self.__symbols_info,
            fee=TRADE_FEE,
            min_profit=MIN_PROFIT
        )
        self.__detector.arbitrage_detected.connect(self.__process_arbitrage)

    def __on_arbitrage_processed(self):
        logger.info('Arbitrage processed')
        self.__is_processing = False

    def __process_arbitrage(self, arb: Arbitrage):
        if self.__is_processing:
            return
        else:
            self.__is_processing = True
        actions = []
        for action in arb.actions:
            actions.append(
                BinanceSingleAction(
                    pair=action.pair,
                    side=action.action.upper(),
                    quantity=action.amount,
                    price=action.price,
                    order_type='LIMIT',
                    timeInForce='GTC'
                )
            )
        self.__executor = BinanceActionsExecutor(
            # api=self.__api,
            api_key=API_KEY,
            api_secret=API_SECRET,
            actions_list=actions,
            detector=self.__detector,
            arbitrage=arb
        )
        self.__executor.execution_finished.connect(self.__on_arbitrage_processed)
        self.__executor.start()
        logger.info('Arbitrage executor called')


if __name__ == '__main__':
    logger.info('Starting...')
    app = QCoreApplication(sys.argv)
    api = BinanceApi(API_KEY, API_SECRET)
    triarb = TriangularArbitrage(api)
    sys.exit(app.exec_())
