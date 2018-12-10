import sys
from config import API_KEY, API_SECRET, TRADE_FEE, MIN_PROFIT
from binance_api import BinanceApi
from arbitrage_detector import ArbitrageDetector
from binance_actions_executor import BinanceActionsExecutor, BinanceSingleAction
from PyQt5.QtCore import QCoreApplication, QObject
from custom_logging import get_logger
logger = get_logger(__name__)


class TriangularArbitrage(QObject):

    def __init__(self):
        super(TriangularArbitrage, self).__init__()
        self.__api = BinanceApi(API_KEY, API_SECRET)
        self.__is_processing = False
        symbols_info = self.__api.get_symbols_info()
        detector = ArbitrageDetector(
            api=self.__api,
            symbols_info=symbols_info,
            fee=TRADE_FEE,
            min_profit=MIN_PROFIT
        )
        detector.arbitrage_detected.connect(self.__process_arbitrage)

    def __on_arbitrage_processed(self):
        logger.info('Arbitrage processed')
        self.__is_processing = False

    def __process_arbitrage(self, arb):
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
            api=self.__api,
            actions_list=actions
        )
        self.__executor.execution_finished.connect(self.__on_arbitrage_processed)
        self.__executor.start()
        logger.info('Arbitrage executor called')


if __name__ == '__main__':
    logger.info('Starting...')
    app = QCoreApplication(sys.argv)

    sys.exit(app.exec_())
