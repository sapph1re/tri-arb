import sys
from decimal import Decimal
from config import API_KEY, API_SECRET
from binance_api import BinanceApi
from binance_orderbook import BinanceOrderBook, BinanceDepthWebsocket
from PyQt5.QtCore import QCoreApplication, QObject, pyqtSignal
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


def pair_to_currencies(pair):
	return pair[:3], pair[3:]


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
		self.pair = pair
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
		return '{}, profit: {} ({}%)'.format(actions_str, self.profit_abs, self.profit_rel * 100)


class ArbitrageDetector(QObject):
	arbitrage_detected = pyqtSignal(Arbitrage)

	def __init__(self, pairs, fee, min_profit):
		super(ArbitrageDetector, self).__init__()
		self.fee = fee
		self.pairs = pairs
		self.min_profit = min_profit
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

	def calculate_amount_on_price_level(self, yz: tuple, xz: tuple, xy: tuple) -> tuple:
		"""
		Calculates available trade amount on one depth level in the triangle
		:param yz: (price: Decimal, amount: Decimal) on Y/Z
		:param xz: (price: Decimal, amount: Decimal) on X/Z
		:param xy: (price: Decimal, amount: Decimal) on X/Y
		:return: (amount_x: Decimal, amount_y: Decimal) - available amounts in X and in Y
		"""
		amount_x = min(xy[1], xz[1])
		amount_y = amount_x * xy[0]
		if yz[1] < amount_y:
			amount_y = yz[1]
			amount_x = amount_y / xy[0]
		return amount_x, amount_y

	def find_arbitrage_in_triangle(self, yz: str, xz: str, xy: str) -> Arbitrage or None:
		"""
		Looks for arbitrage in the triangle: Y/Z, X/Z, X/Y.
		X, Y, Z are three currencies for which exist the three currency pairs above
		:param yz: Y/Z pair, e.g. 'ethbtc'
		:param xz: X/Z pair, e.g. 'eosbtc'
		:param xy: X/Y pair, e.g. 'eoseth'
		:return: Arbitrage instance or None
		"""
		bids = {
			'yz': self.orderbooks[yz].get_bids(),
			'xz': self.orderbooks[xz].get_bids(),
			'xy': self.orderbooks[xy].get_bids()
		}
		asks = {
			'yz': self.orderbooks[yz].get_asks(),
			'xz': self.orderbooks[xz].get_asks(),
			'xy': self.orderbooks[xy].get_asks()
		}
		# checking that orderbooks are not empty
		for side in bids, asks:
			for pair in side:
				if len(side[pair]) == 0:
					logger.debug('Orderbooks are not ready yet')
					return None
		currency_x = pair_to_currencies(xy)[0]
		# checking triangle in one direction: sell Y/Z -> buy X/Z -> sell X/Y
		amount_total_x = Decimal(0)
		amount_total_y = Decimal(0)
		profit_total_x = Decimal(0)
		while True:
			# check profitability
			profit_rel = bids['yz'][0][0] / asks['xz'][0][0] * bids['xy'][0][0] * (1 - self.fee) ** 3 - 1
			if profit_rel < self.min_profit:
				break
			# calculate trade amounts available on this level
			amount_x, amount_y = self.calculate_amount_on_price_level(
				bids['yz'][0], asks['xz'][0], bids['xy'][0]
			)
			# save the counted amounts and price levels
			amount_total_x += amount_x
			amount_total_y += amount_y
			profit_total_x += profit_rel * amount_x
			prices = {'yz': bids['yz'][0][0], 'xz': asks['xz'][0][0], 'xy': bids['xy'][0][0]}
			# subtract the counted amounts from the orderbooks and try to go deeper on the next iteration
			bids['yz'][0] = (bids['yz'][0][0], bids['yz'][0][1] - amount_y)
			asks['xz'][0] = (asks['xz'][0][0], asks['xz'][0][1] - amount_x)
			bids['xy'][0] = (bids['xy'][0][0], bids['xy'][0][1] - amount_x)
			for ob in [bids['yz'], asks['xz'], bids['xy']]:
				if ob[0][1] < 0:
					raise Exception('Critical calculation error')
				if ob[0][1] == 0:
					ob.pop(0)
		if amount_total_x > 0:
			return Arbitrage(
				actions=[
					MarketAction(yz, SELL, prices['yz'], amount_total_y),
					MarketAction(xz, BUY, prices['xz'], amount_total_x),
					MarketAction(xy, SELL, prices['xy'], amount_total_x)
				],
				profit_rel=(profit_total_x / amount_total_x),
				profit_abs=Money(profit_total_x, currency_x)
			)

		# checking triangle in another direction: buy X/Y -> sell X/Z -> buy Y/Z
		amount_total_x = Decimal(0)
		amount_total_y = Decimal(0)
		profit_total_x = Decimal(0)
		while True:
			# check profitability
			profit_rel = bids['xz'][0][0] / asks['xy'][0][0] * asks['yz'][0][0] * (1 - self.fee) ** 3 - 1
			if profit_rel < self.min_profit:
				break
			# calculate trade amounts available on this level
			amount_x, amount_y = self.calculate_amount_on_price_level(
				asks['yz'][0], bids['xz'][0], asks['xy'][0]
			)
			# save the counted amounts and price levels
			amount_total_x += amount_x
			amount_total_y += amount_y
			profit_total_x += profit_rel * amount_x
			prices = {'yz': asks['yz'][0][0], 'xz': bids['xz'][0][0], 'xy': asks['xy'][0][0]}
			# subtract the counted amounts from the orderbooks and try to go deeper on the next iteration
			asks['yz'][0] = (asks['yz'][0][0], asks['yz'][0][1] - amount_y)
			bids['xz'][0] = (bids['xz'][0][0], bids['xz'][0][1] - amount_x)
			asks['xy'][0] = (asks['xy'][0][0], asks['xy'][0][1] - amount_x)
			for ob in [asks['yz'], bids['xz'], asks['xy']]:
				if ob[0][1] < 0:
					raise Exception('Critical calculation error')
				if ob[0][1] == 0:
					ob.pop(0)
		if amount_total_x > 0:
			return Arbitrage(
				actions=[
					MarketAction(xy, BUY, prices['xy'], amount_total_x),
					MarketAction(xz, SELL, prices['xz'], amount_total_x),
					MarketAction(yz, BUY, prices['yz'], amount_total_y)
				],
				profit_rel=(profit_total_x / amount_total_x),
				profit_abs=Money(profit_total_x, currency_x)
			)

		return None

	def on_orderbook_updated(self, symbol: str, bids: list, asks: list):
		symbol = symbol.lower()
		if symbol not in self.pairs:
			return
		yz = self.triangle['yz']
		xz = self.triangle['xz']
		xy = self.triangle['xy']
		arbitrage = self.find_arbitrage_in_triangle(yz, xz, xy)
		if arbitrage is not None:
			self.report_arbitrage(arbitrage)
		else:
			logger.debug('No arbitrage found')


detector = ArbitrageDetector(
	['ethbtc', 'eosbtc', 'eoseth'],
	fee=Decimal('0.005'),
	min_profit=Decimal('0.001')
)

logger.info('Starting...')

app = QCoreApplication(sys.argv)
sys.exit(app.exec_())
