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
triangle = {
	'yz': pairs[0],
	'xz': pairs[1],
	'xy': pairs[2]
}
bapi = BinanceApi(API_KEY, API_SECRET)
orderbooks = {}


def pair_pretty(pair):
	return '{}/{}'.format(pair[:3].upper(), pair[3:].upper())


def on_orderbook_updated(symbol: str, bids: list, asks: list):
	symbol = symbol.lower()
	if symbol not in pairs:
		return
	# getting best bids/asks
	try:
		yz_bid = orderbooks[triangle['yz']].get_bids()[0][0]
		yz_ask = orderbooks[triangle['yz']].get_asks()[0][0]
		xz_bid = orderbooks[triangle['xz']].get_bids()[0][0]
		xz_ask = orderbooks[triangle['xz']].get_asks()[0][0]
		xy_bid = orderbooks[triangle['xy']].get_bids()[0][0]
		xy_ask = orderbooks[triangle['xy']].get_asks()[0][0]
	except IndexError:
		logger.debug('Orderbooks are not ready yet')
		return
	# checking triangle in one direction: sell Y/Z -> buy X/Z -> sell X/Y
	profit = yz_bid / xz_ask * xy_bid * (1 - fee)**3 - 1
	if profit > 0:
		logger.info('Arbitrage Found: sell {} @ {} -> buy {} @ {} -> sell {} @ {}. Profit: {}%',
					pair_pretty(triangle['yz']), pair_pretty(triangle['xz']), pair_pretty(triangle['xy']),
					yz_bid, xz_ask, xy_bid, profit * 100)
		return
	# checking triangle in one direction: buy X/Y -> sell X/Z -> buy Y/Z
	profit = xz_bid / xy_ask * yz_ask * (1 - fee)**3 - 1
	if profit > 0:
		logger.info('Arbitrage Found: buy {} @ {} -> sell {} @ {} -> buy {} @ {}. Profit: {}%',
					pair_pretty(triangle['xy']), pair_pretty(triangle['xz']), pair_pretty(triangle['yz']),
					xy_ask, xz_bid, yz_ask, profit * 100)
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
