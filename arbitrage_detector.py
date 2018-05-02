import sys
from decimal import Decimal
from config import API_KEY, API_SECRET
from binance_api import BinanceApi
from binance_orderbook import BinanceOrderBook, BinanceDepthWebsocket
from PyQt5.QtCore import QCoreApplication
import logging
from custom_logging import GracefulFormatter, StyleAdapter
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)
log_formatter_debug = GracefulFormatter('{asctime} {levelname} [{threadName}] [{name}:{funcName}] {message}', '%H:%M:%S')
handler_console = logging.StreamHandler()
handler_console.setLevel(logging.DEBUG)
handler_console.setFormatter(log_formatter_debug)
logger.addHandler(handler_console)
logger = StyleAdapter(logger)


fee = Decimal('0.0005')
pairs = ['ethbtc', 'eosbtc', 'eoseth']
bapi = BinanceApi(API_KEY, API_SECRET)
orderbooks = {}


def on_orderbook_updated(symbol: str, bids: list, asks: list):
	symbol = symbol.lower()
	if symbol not in pairs:
		return
	# getting best bids/asks
	try:
		ethbtc_bid = Decimal(orderbooks['ethbtc'].get_bids()[0][0])
		ethbtc_ask = Decimal(orderbooks['ethbtc'].get_asks()[0][0])
		eosbtc_bid = Decimal(orderbooks['eosbtc'].get_bids()[0][0])
		eosbtc_ask = Decimal(orderbooks['eosbtc'].get_asks()[0][0])
		eoseth_bid = Decimal(orderbooks['eoseth'].get_bids()[0][0])
		eoseth_ask = Decimal(orderbooks['eoseth'].get_asks()[0][0])
	except IndexError:
		return
	# checking triangle in one direction: sell ETH/BTC -> buy EOS/BTC -> sell EOS/ETH
	profit = ethbtc_bid / eosbtc_ask * eoseth_bid * (1 - fee)**3 - 1
	if profit > 0:
		logger.info('Arbitrage Found: sell ETH/BTC @ {} -> buy EOS/BTC @ {} -> sell EOS/ETH @ {}. Profit: {}%',
					ethbtc_bid, eosbtc_ask, eoseth_bid, profit*100)
		return
	# checking triangle in another direction: buy EOS/ETH -> sell EOS/BTC -> buy ETH/BTC
	profit = eosbtc_bid / eoseth_ask * ethbtc_ask * (1 - fee)**3 - 1
	if profit > 0:
		logger.info('Arbitrage Found: buy EOS/ETH @ {} -> sell EOS/BTC @ {} -> buy ETH/BTC @ {}. Profit: {}%',
					eoseth_ask, eosbtc_bid, ethbtc_ask, profit*100)
		return
	logger.debug('No arbitrage found')


for pair in pairs:
	ob = BinanceOrderBook(bapi, pair)
	ws = BinanceDepthWebsocket(ob)
	ob.ob_updated.connect(on_orderbook_updated)
	orderbooks[pair] = ob


logger.info('Starting...')

app = QCoreApplication(sys.argv)
sys.exit(app.exec_())
