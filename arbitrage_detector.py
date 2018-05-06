import sys
from decimal import Decimal
from config import API_KEY, API_SECRET
from binance_api import BinanceApi
from binance_orderbook import BinanceOrderBook, BinanceDepthWebsocket
from PyQt5.QtCore import QCoreApplication, pyqtSignal
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


bapi = BinanceApi(API_KEY, API_SECRET)


BUY = 'buy'
SELL = 'sell'


class Money:
	def __init__(self, amount, currency):
		if not isinstance(amount, Decimal):
			amount = Decimal(amount)
		self.amount = amount
		self.currency = currency

	def __str__(self):
		return '{} {}'.format(self.amount, self.currency.upper())

	def __repr__(self):
		return self.__str__()


class MarketAction:
	def __init__(self, pair: str, action: str, price: Decimal, amount: Decimal):
		self.pair = pair,
		self.action = action
		self.price = price
		self.amount = amount

	def __str__(self):
		pair_pretty = '{}/{}'.format(self.pair[:3].upper(), self.pair[3:].upper())
		return '{} {} {} @ {}'.format(self.action, self.amount, pair_pretty, self.price)

	def __repr__(self):
		return self.__str__()


class Arbitrage:
	def __init__(self, actions, profit_rel, profit_abs):
		self.actions = actions
		self.profit_rel = profit_rel
		self.profit_abs = profit_abs

	def __str__(self):
		actions_str = ' -> '.join([str(action) for action in self.actions])
		logger.info('{}, profit: {} ({}%)', actions_str, self.profit_abs, self.profit_rel * 100)


class ArbitrageDetector:
	arbitrage_detected = pyqtSignal(Arbitrage)

	def __init__(self, pairs, fee):
		self.fee = fee
		self.pairs = pairs
		self.orderbooks = {}
		for pair in pairs:
			ob = BinanceOrderBook(bapi, pair)
			ws = BinanceDepthWebsocket(ob)
			ob.ob_updated.connect(self.on_orderbook_updated)
			self.orderbooks[pair] = ob
		self.triangle = {
			'yz': pairs[0],
			'xz': pairs[1],
			'xy': pairs[2]
		}

	def report_arbitrage(self, arbitrage: Arbitrage):
		logger.info('Arbitrage found: {}', arbitrage)
		self.arbitrage_detected.emit(arbitrage)

	def on_orderbook_updated(self, symbol: str, bids: list, asks: list):
		symbol = symbol.lower()
		if symbol not in self.pairs:
			return
		yz = self.triangle['yz']
		xz = self.triangle['xz']
		xy = self.triangle['xy']
		# getting best bids/asks
		try:
			yz_bid = self.orderbooks[yz].get_bids()[0][0]
			yz_ask = self.orderbooks[yz].get_asks()[0][0]
			xz_bid = self.orderbooks[xz].get_bids()[0][0]
			xz_ask = self.orderbooks[xz].get_asks()[0][0]
			xy_bid = self.orderbooks[xy].get_bids()[0][0]
			xy_ask = self.orderbooks[xy].get_asks()[0][0]
		except IndexError:
			logger.debug('Orderbooks are not ready yet')
			return
		# checking triangle in one direction: sell Y/Z -> buy X/Z -> sell X/Y
		profit = yz_bid / xz_ask * xy_bid * (1 - self.fee)**3 - 1
		if profit > 0:
			self.report_arbitrage(Arbitrage(
				actions=[
					MarketAction(yz, SELL, yz_bid, Decimal(0)),
					MarketAction(xz, BUY, xz_ask, Decimal(0)),
					MarketAction(xy, SELL, yz_bid, Decimal(0))
				],
				profit_rel=profit,
				profit_abs=Money(0, 'btc')
			))
			return
		# checking triangle in another direction: buy X/Y -> sell X/Z -> buy Y/Z
		profit = xz_bid / xy_ask * yz_ask * (1 - self.fee)**3 - 1
		if profit > 0:
			self.report_arbitrage(Arbitrage(
				actions=[
					MarketAction(xz, BUY, yz_bid, Decimal(0)),
					MarketAction(xy, SELL, xz_ask, Decimal(0)),
					MarketAction(yz, BUY, yz_bid, Decimal(0))
				],
				profit_rel=profit,
				profit_abs=Money(0, 'btc')
			))
			return
		logger.debug('No arbitrage found')


pairs = ['ethbtc', 'eosbtc', 'eoseth']
detector = ArbitrageDetector(pairs, fee=Decimal('0.0005'))

logger.info('Starting...')

app = QCoreApplication(sys.argv)
sys.exit(app.exec_())
