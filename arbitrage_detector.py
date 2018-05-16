import sys
from decimal import Decimal
from typing import Dict, Tuple
from config import API_KEY, API_SECRET, TRADE_FEE, MIN_PROFIT
from binance_api import BinanceApi, BinanceSymbolInfo
from binance_orderbook import BinanceOrderBook, BinanceDepthWebsocket
from triangles_finder import TrianglesFinder
from PyQt5.QtCore import QCoreApplication, QObject, QThread, pyqtSignal
from custom_logging import get_logger
logger = get_logger(__name__)


BUY = 'buy'
SELL = 'sell'


class MarketAction:
    def __init__(self, pair: Tuple[str, str], action: str, price: Decimal, amount: Decimal):
        self.pair = pair
        self.action = action
        self.price = price
        self.amount = amount

    def __str__(self):
        return '{} {} {}/{} @ {}'.format(self.action, self.amount, self.pair[0], self.pair[1], self.price)

    def __repr__(self):
        return self.__str__()


class Arbitrage:
    def __init__(self, actions, currency_z, amount_z, profit_z, profit_rel):
        self.actions = actions
        self.currency_z = currency_z
        self.amount_z = amount_z
        self.profit_z = profit_z
        self.profit_rel = profit_rel

    def __str__(self):
        actions_str = ' -> '.join([str(action) for action in self.actions])
        return '{}, trade amount: {} {}, profit: {} {} ({}%)'.format(
            actions_str, self.amount_z, self.currency_z, self.profit_z, self.currency_z, self.profit_rel * 100
        )

    def __repr__(self):
        return self.__str__()


class ArbitrageDetector(QThread):
    arbitrage_detected = pyqtSignal(Arbitrage)
    arbitrage_disappeared = pyqtSignal(str, str)  # e.g. 'ethbtc eosbtc eoseth', 'sell buy sell'

    def __init__(self, api: BinanceApi, symbols_info: Dict[str, BinanceSymbolInfo], fee: Decimal, min_profit: Decimal):
        """
        Launches Arbitrage Detector
        :param api: BinanceApi instance
        :param symbols_info: {symbol: BinanceSymbolInfo, ...}
        :param fee: trade fee on the exchange
        :param min_profit: detect arbitrage with this profit or higher
        """
        super(ArbitrageDetector, self).__init__()
        self.api = api
        self.fee = fee
        self.min_profit = min_profit
        self.orderbooks = {}
        self.existing_arbitrages = {}  # {'pair pair pair': {'buy sell buy': ..., 'sell buy sell': ...}}
        self.triangles = TrianglesFinder().make_triangles(symbols_info)
        self.triangles, self.symbols = self._verify_triangles(self.triangles)
        logger.debug('Triangles: {}', self.triangles)
        logger.debug('Symbols: {}', self.symbols)
        ws = BinanceDepthWebsocket()
        for symbol, details in self.symbols.items():
            ob = BinanceOrderBook(api=self.api, base=details['base'], quote=details['quote'], websocket=ws)
            ob.ob_updated.connect(self.on_orderbook_updated)
            self.orderbooks[symbol] = ob
        ws.start()

    @staticmethod
    def _order_symbols_in_triangle(triangle: tuple):
        """
        Orders triangle properly
        :param triangle: ((str, str), (str, str), (str, str))
        :return: triangle in proper order (YZ, XZ, XY)
        """
        # yz, xz, xy
        a, b, c = triangle
        if a[1] == c[1]:
            b, c = c, b
        elif b[1] == c[1]:
            a, b, c = b, c, a
        if a[0] != c[1]:
            a, b = b, a
        return a, b, c

    def _verify_triangles(self, triangles: set) -> Tuple[set, dict]:
        """
        Checks every triangle for the correct format & sequence: ((Y, Z), (X, Z), (X, Y)).
        :return: (triangles: set, symbols: dict)
        Returns a set of triangles with all incorrect triangles removed or corrected,
        and a dict of symbols used in the triangles.
        """
        symbols = {}
        triangles_verified = set()
        for triangle in triangles:
            triangle = self._order_symbols_in_triangle(triangle)
            if triangle[0][0] == triangle[2][1] and triangle[0][1] == triangle[1][1] and triangle[1][0] == triangle[2][0]:
                triangles_verified.add(triangle)
                for pair in triangle:
                    symbol = pair[0]+pair[1]
                    if symbol not in symbols:
                        symbols[symbol] = {
                            'base': pair[0],
                            'quote': pair[1],
                            'triangles': set()
                        }
                    symbols[symbol]['triangles'].add(triangle)
        return triangles_verified, symbols

    def report_arbitrage(self, arbitrage: Arbitrage):
        logger.info('Arbitrage found: {}', arbitrage)
        self.arbitrage_detected.emit(arbitrage)

    def calculate_amounts_on_price_level(self, direction: str, yz: tuple, xz: tuple, xy: tuple) -> tuple:
        """
        Calculates available trade amount on one depth level in the triangle
        :param direction: 'sell buy sell' or 'buy sell buy'
        :param yz: (price: Decimal, amount: Decimal) on Y/Z
        :param xz: (price: Decimal, amount: Decimal) on X/Z
        :param xy: (price: Decimal, amount: Decimal) on X/Y
        :return: (amount_y, amount_x_buy, amount_x_sell) - amounts to use for the orders
        """
        amount_x, limiter = min((xy[1], 'xy'), (xz[1], 'xz'))
        amount_y = amount_x * xy[0]
        amount_x_buy = amount_x_sell = amount_x
        if direction == 'sell buy sell':
            amount_y *= 1 - self.fee
            if limiter == 'xz':
                amount_y *= 1 - self.fee
        elif direction == 'buy sell buy':
            amount_y /= 1 - self.fee
            if limiter == 'xz':
                amount_y /= 1 - self.fee
        if yz[1] < amount_y:
            amount_y = yz[1]
            amount_x = amount_y / xy[0]
            limiter = 'yz'
            if direction == 'sell buy sell':
                amount_x /= 1 - self.fee
            elif direction == 'buy sell buy':
                amount_x *= 1 - self.fee
        if limiter == 'xz' and direction == 'sell buy sell' or limiter != 'xz' and direction == 'buy sell buy':
            amount_x_buy = amount_x
            amount_x_sell = amount_x * (1 - self.fee)
        elif limiter != 'xz' and direction == 'sell buy sell' or limiter == 'xz' and direction == 'buy sell buy':
            amount_x_buy = amount_x / (1 - self.fee)
            amount_x_sell = amount_x
        return amount_y, amount_x_buy, amount_x_sell

    def find_arbitrage_in_triangle(self, triangle: Tuple[Tuple[str, str], Tuple[str, str], Tuple[str, str]]) -> Arbitrage or None:
        """
        Looks for arbitrage in the triangle: Y/Z, X/Z, X/Y.
        X, Y, Z are three currencies for which exist the three currency pairs above
        :param triangle: ((Y, Z), (X, Z), (X, Y)) example: (('ETH', 'BTC'), ('EOS', 'BTC'), ('EOS', 'ETH'))
        :return: Arbitrage instance or None
        """
        yz = triangle[0][0]+triangle[0][1]
        xz = triangle[1][0]+triangle[1][1]
        xy = triangle[2][0]+triangle[2][1]
        currency_z = triangle[0][1]
        # initializing existing_arbitrages storage
        pairs = '{} {} {}'.format(yz, xz, xy)
        if pairs not in self.existing_arbitrages:
            self.existing_arbitrages[pairs] = {}
        for actions in ['sell buy sell', 'buy sell buy']:
            if actions not in self.existing_arbitrages[pairs]:
                self.existing_arbitrages[pairs][actions] = False
        # getting orderbooks
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
        for side in [bids, asks]:
            for pair in side:
                if len(side[pair]) == 0:
                    logger.debug('Orderbooks are not ready yet')
                    return None
        # checking triangle in one direction: sell Y/Z, buy X/Z, sell X/Y
        amount_x_buy_total = Decimal(0)
        amount_x_sell_total = Decimal(0)
        amount_y_total = Decimal(0)
        amount_z_spend_total = Decimal(0)
        profit_z_total = Decimal(0)
        while True:
            # check profitability
            profit_rel = bids['yz'][0][0] / asks['xz'][0][0] * bids['xy'][0][0] * (1 - self.fee) ** 3 - 1
            if profit_rel < self.min_profit:
                break
            # calculate trade amounts available on this level
            amount_y, amount_x_buy, amount_x_sell = self.calculate_amounts_on_price_level(
                'sell buy sell', bids['yz'][0], asks['xz'][0], bids['xy'][0]
            )
            # calculate the profit on this level
            profit_z = amount_y * bids['yz'][0][0] * (1 - self.fee) - amount_x_buy * asks['xz'][0][0]
            # save the counted amounts and price levels
            amount_x_buy_total += amount_x_buy
            amount_x_sell_total += amount_x_sell
            amount_y_total += amount_y
            amount_z_spend_total += amount_x_buy * asks['xz'][0][0]
            profit_z_total += profit_z
            prices = {'yz': bids['yz'][0][0], 'xz': asks['xz'][0][0], 'xy': bids['xy'][0][0]}
            # subtract the counted amounts from the orderbooks and try to go deeper on the next iteration
            bids['yz'][0] = (bids['yz'][0][0], bids['yz'][0][1] - amount_y)
            asks['xz'][0] = (asks['xz'][0][0], asks['xz'][0][1] - amount_x_buy)
            bids['xy'][0] = (bids['xy'][0][0], bids['xy'][0][1] - amount_x_sell)
            for ob in [bids['yz'], asks['xz'], bids['xy']]:
                if ob[0][1] < 0:
                    raise Exception('Critical calculation error')
                if ob[0][1] == 0:
                    ob.pop(0)
        if profit_z_total > 0:
            if not self.existing_arbitrages[pairs]['sell buy sell']:
                self.existing_arbitrages[pairs]['sell buy sell'] = True
            return Arbitrage(
                actions=[
                    MarketAction(triangle[0], SELL, prices['yz'], amount_y_total),
                    MarketAction(triangle[1], BUY, prices['xz'], amount_x_buy_total),
                    MarketAction(triangle[2], SELL, prices['xy'], amount_x_sell_total)
                ],
                currency_z=currency_z,
                amount_z=amount_z_spend_total,
                profit_z=profit_z_total,
                profit_rel=(profit_z_total / amount_z_spend_total)
            )

        # checking triangle in another direction: buy Y/Z, sell X/Z, buy X/Y
        amount_x_buy_total = Decimal(0)
        amount_x_sell_total = Decimal(0)
        amount_y_total = Decimal(0)
        amount_z_spend_total = Decimal(0)
        profit_z_total = Decimal(0)
        while True:
            # check profitability
            profit_rel = bids['xz'][0][0] / asks['xy'][0][0] / asks['yz'][0][0] * (1 - self.fee) ** 3 - 1
            if profit_rel < self.min_profit:
                break
            # calculate trade amounts available on this level
            amount_y, amount_x_buy, amount_x_sell = self.calculate_amounts_on_price_level(
                'buy sell buy', asks['yz'][0], bids['xz'][0], asks['xy'][0]
            )
            # calculate the profit on this level
            profit_z = amount_x_sell * bids['xz'][0][0] * (1 - self.fee) - amount_y * asks['yz'][0][0]
            # save the counted amounts and price levels
            amount_x_buy_total += amount_x_buy
            amount_x_sell_total += amount_x_sell
            amount_y_total += amount_y
            amount_z_spend_total += amount_y * asks['yz'][0][0]
            profit_z_total += profit_z
            prices = {'yz': asks['yz'][0][0], 'xz': bids['xz'][0][0], 'xy': asks['xy'][0][0]}
            # subtract the counted amounts from the orderbooks and try to go deeper on the next iteration
            asks['yz'][0] = (asks['yz'][0][0], asks['yz'][0][1] - amount_y)
            bids['xz'][0] = (bids['xz'][0][0], bids['xz'][0][1] - amount_x_sell)
            asks['xy'][0] = (asks['xy'][0][0], asks['xy'][0][1] - amount_x_buy)
            for ob in [asks['yz'], bids['xz'], asks['xy']]:
                if ob[0][1] < 0:
                    raise Exception('Critical calculation error')
                if ob[0][1] == 0:
                    ob.pop(0)
        if profit_z_total > 0:
            if not self.existing_arbitrages[pairs]['buy sell buy']:
                self.existing_arbitrages[pairs]['buy sell buy'] = True
            return Arbitrage(
                actions=[
                    MarketAction(triangle[0], BUY, prices['yz'], amount_y_total),
                    MarketAction(triangle[1], SELL, prices['xz'], amount_x_sell_total),
                    MarketAction(triangle[2], BUY, prices['xy'], amount_x_buy_total),
                ],
                currency_z=currency_z,
                amount_z=amount_z_spend_total,
                profit_z=profit_z_total,
                profit_rel=(profit_z_total / amount_z_spend_total)
            )

        # no arbitrage found
        logger.debug('No arbitrage found')
        for actions in ['sell buy sell', 'buy sell buy']:
            if self.existing_arbitrages[pairs][actions]:
                self.existing_arbitrages[pairs][actions] = False
                self.arbitrage_disappeared.emit(pairs, actions)
        return None

    def on_orderbook_updated(self, symbol: str):
        if symbol not in self.symbols:
            return
        for triangle in self.symbols[symbol]['triangles']:
            arbitrage = self.find_arbitrage_in_triangle(triangle)
            if arbitrage is not None:
                self.report_arbitrage(arbitrage)


if __name__ == '__main__':
    logger.info('Starting...')
    api = BinanceApi(API_KEY, API_SECRET)
    symbols_info = api.get_symbols_info()
    symbols_info_slice = {}
    i = 0
    for symbol, symbol_info in symbols_info.items():
        symbols_info_slice[symbol] = symbol_info
        i += 1
        if i >= 20:
            break
    logger.debug('All Symbols Info: {}', symbols_info_slice)
    detector = ArbitrageDetector(
        api=api,
        symbols_info=symbols_info_slice,
        fee=TRADE_FEE,
        min_profit=MIN_PROFIT
    )

    app = QCoreApplication(sys.argv)
    sys.exit(app.exec_())
