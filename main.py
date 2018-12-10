import sys
from config import API_KEY, API_SECRET, TRADE_FEE, MIN_PROFIT
from binance_api import BinanceApi
from arbitrage_detector import ArbitrageDetector
from binance_actions_executor import BinanceActionsExecutor, BinanceSingleAction
from PyQt5.QtCore import QCoreApplication
from custom_logging import get_logger
logger = get_logger(__name__)


if __name__ == '__main__':
    logger.info('Starting...')
    app = QCoreApplication(sys.argv)
    api = BinanceApi(API_KEY, API_SECRET)
    is_processing = False

    def on_arbitrage_processed():
        global is_processing
        is_processing = False

    def process_arbitrage(arb):
        global is_processing
        if is_processing:
            return
        else:
            is_processing = True
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
        executor = BinanceActionsExecutor(
            api=api,
            actions_list=actions
        )
        executor.execution_finished.connect(on_arbitrage_processed)
        executor.start()
        logger.info('Arbitrage executor called')

    symbols_info = api.get_symbols_info()
    logger.debug('All Symbols Info: {}', symbols_info)
    detector = ArbitrageDetector(
        api=api,
        symbols_info=symbols_info,
        fee=TRADE_FEE,
        min_profit=MIN_PROFIT
    )
    detector.arbitrage_detected.connect(process_arbitrage)
    sys.exit(app.exec_())
