import sys
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Tuple
from config import API_KEY, API_SECRET, TRADE_FEE, MIN_PROFIT
from binance_api import BinanceApi, BinanceSymbolInfo
from binance_orderbook import BinanceOrderBook, BinanceDepthWebsocket
from triangles_finder import TrianglesFinder
from PyQt5.QtCore import QCoreApplication, QObject, QThread, pyqtSignal
from custom_logging import get_logger
logger = get_logger(__name__)


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
    def __init__(self, actions, currency_z, amount_z, profit_z, profit_z_rel, profit_x, currency_x, profit_y, currency_y):
        self.actions = actions
        self.currency_z = currency_z
        self.amount_z = amount_z
        self.profit_z = profit_z
        self.profit_z_rel = profit_z_rel
        self.profit_y = profit_y
        self.currency_y = currency_y
        self.profit_x = profit_x
        self.currency_x = currency_x

    def __str__(self):
        actions_str = ' -> '.join([str(action) for action in self.actions])
        return '{}, trade amount: {} {}, profit: +{} {} ({}%), +{} {}, +{} {}'.format(
            actions_str, self.amount_z, self.currency_z, self.profit_z, self.currency_z, self.profit_z_rel * 100,
            self.profit_x, self.currency_x, self.profit_y, self.currency_y
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
        self.symbols_info = symbols_info
        self.triangles = TrianglesFinder().make_triangles(symbols_info)
        self.triangles, self.symbols = self._verify_triangles(self.triangles)
        logger.debug('Triangles: {}', self.triangles)
        logger.debug('Symbols: {}', self.symbols)

        # load order amount requirements
        for symbol in self.symbols:
            for sym_filter in self.symbols_info[symbol]['filters']:
                if sym_filter['filterType'] == 'LOT_SIZE':
                    self.symbols_info[symbol]['min_amount'] = Decimal(sym_filter['minQty'])
                    self.symbols_info[symbol]['max_amount'] = Decimal(sym_filter['maxQty'])
                    self.symbols_info[symbol]['amount_step'] = Decimal(sym_filter['stepSize'])
                if sym_filter['filterType'] == 'MIN_NOTIONAL':
                    self.symbols_info[symbol]['min_notional'] = Decimal(sym_filter['minNotional'])

        # start watching the orderbooks
        self.threads = []
        self.websockets = []
        i = 999
        for symbol, details in self.symbols.items():
            # starting a thread and a websocket per every 50 symbols
            if i >= 50:
                th = QThread()
                self.threads.append(th)
                ws = BinanceDepthWebsocket()
                ws.moveToThread(th)
                self.websockets.append(ws)
                i = 0
            # starting an orderbook watcher for every symbol
            ob = BinanceOrderBook(api=self.api, base=details['base'], quote=details['quote'], websocket=ws)
            ob.moveToThread(th)
            ob.ob_updated.connect(self.on_orderbook_updated)
            self.orderbooks[symbol] = ob
            i += 1
        for thread in self.threads:
            thread.start()
        for ws in self.websockets:
            ws.connect()

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

        :return: (triangles: set, symbols: dict) - set of correct triangles and dict of symbols used in the triangles.
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
        Calculates available trade amount on one depth level in the triangle.

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

    def normalize_amounts(self, amounts, amounts_to_symbols, prices):
        amounts_new = {}
        for amount_type, symbol in amounts_to_symbols.items():
            # make sure that min_amount <= order amount <= max_amount
            if amounts[amount_type] < self.symbols_info[symbol]['min_amount']:
                return None
            elif amounts[amount_type] > self.symbols_info[symbol]['max_amount']:
                amounts_new[amount_type] = self.symbols_info[symbol]['max_amount']
            else:
                # round order amount precision to amount_step
                amounts_new[amount_type] = amounts[amount_type].quantize(
                    self.symbols_info[symbol]['amount_step'], rounding=ROUND_DOWN
                )
            # check that amount * price >= min_notional
            if amounts_new[amount_type] * prices[symbol] < self.symbols_info[symbol]['min_notional']:
                return None  # amount is too little
        return amounts_new

    def normalize_amounts_and_recalculate(
        self,
        symbols: Tuple[str, str, str], direction: str,
        amounts: Dict[str, Decimal], prices: Tuple[Decimal, Decimal, Decimal]
    ) -> Dict[str: Decimal] or None:
        """
        Takes arbitrage amounts and normalizes them to comply with correct order amounts on the exchange.

        :param symbols: (YZ, XZ, XY) tuple of symbols we're trading at, e.g. ('ETHBTC', 'EOSBTC', 'EOSETH')
        :param direction: 'sell buy sell' or 'buy sell buy'
        :param amounts: {y, x_buy, x_sell, z_spend, z_profit} amounts of the arbitrage
        :param prices: {yz, xz, xy} prices for the three symbols
        :return: {y, x_buy, x_sell, z_spend, x_profit, y_profit, z_profit, z_profit_rel}
            Amounts normalized and recalculated, also reports expected y_profit and x_profit.
            Returns None when misused or amounts are too little.
        """
        yz, xz, xy = symbols
        prices = {yz: prices[0], xz: prices[1], xy: prices[2]}
        # check that we have all symbols info
        for symbol in symbols:
            for field in ['min_amount', 'max_amount', 'amount_step', 'min_notional']:
                if field not in self.symbols_info[symbol]:
                    logger.warning('Missing {} symbol info ({}). Normalization failed.', symbol, field)
                    return None
        z_got = amounts['z_spend'] + amounts['z_profit']
        # normalize amounts to comply with min/max order amounts and min amount step
        if direction == 'sell buy sell':
            amounts_new = self.normalize_amounts(amounts, {'y': yz, 'x_buy': xz, 'x_sell': xy}, prices)
            if amounts_new is None:
                return None
            # make sure x_profit >= 0
            while True:
                amounts_new['x_profit'] = amounts_new['x_buy'] * (1 - self.fee) - amounts_new['x_sell']
                if amounts_new['x_profit'] >= 0:
                    break
                amounts_new['x_sell'] -= self.symbols_info[xy]['amount_step']
                if amounts_new['x_sell'] < self.symbols_info[xy]['min_amount']:
                    return None
            # make sure y_profit >= 0
            while True:
                amounts_new['y_profit'] = amounts['y'] * amounts_new['x_sell'] / amounts['x_sell'] - amounts_new['y']
                if amounts_new['y_profit'] >= 0:
                    break
                amounts_new['y'] -= self.symbols_info[yz]['amount_step']
                if amounts_new['y'] < self.symbols_info[yz]['min_amount']:
                    return None
            # recalculate z_spend and z_profit with new amounts
            amounts_new['z_spend'] = amounts_new['x_buy'] / amounts['x_buy'] * amounts['z_spend']
            amounts_new['z_profit'] = amounts_new['y'] / amounts['y'] * z_got - amounts_new['z_spend']
        elif direction == 'buy sell buy':
            amounts_new = self.normalize_amounts(amounts, {'y': yz, 'x_sell': xz, 'x_buy': xy}, prices)
            if amounts_new is None:
                return None
            # make sure y_profit >= 0
            while True:
                amounts_new['y_profit'] = amounts_new['y'] * (1 - self.fee) - amounts['y'] * amounts_new['x_buy'] / amounts['x_buy']
                if amounts_new['y_profit'] >= 0:
                    break
                amounts_new['x_buy'] -= self.symbols_info[xy]['amount_step']
                if amounts_new['x_buy'] < self.symbols_info[xy]['min_amount']:
                    return None
            # make sure x_profit >= 0
            while True:
                amounts_new['x_profit'] = amounts_new['x_buy'] * (1 - self.fee) - amounts_new['x_sell']
                if amounts_new['x_profit'] >= 0:
                    break
                amounts_new['x_sell'] -= self.symbols_info[xz]['amount_step']
                if amounts_new['x_sell'] < self.symbols_info[xz]['min_amount']:
                    return None
            # recalculate z_spend and z_profit with new amounts
            amounts_new['z_spend'] = amounts_new['y'] / amounts['y'] * amounts['z_spend']
            amounts_new['z_profit'] = amounts_new['x_sell'] / amounts['x_sell'] * z_got - amounts_new['z_spend']
        else:
            logger.warning('Bad direction: {}', direction)
            return None
        # make sure it's still profitable
        if amounts_new['z_profit'] < 0:
            return None
        amounts_new['profit_rel'] = amounts_new['z_profit'] / amounts_new['z_spend']
        if amounts_new['profit_rel'] < self.min_profit:
            return None
        return amounts_new

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
        currency_x = triangle[1][0]
        currency_y = triangle[0][0]
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
        prices = None
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
        if prices is not None:  # potential arbitrage exists
            # make amounts comply with order size requirements
            normalized = self.normalize_amounts_and_recalculate(
                symbols=(yz, xz, xy),
                direction='sell buy sell',
                amounts={
                    'y': amount_y_total,
                    'x_buy': amount_x_buy_total,
                    'x_sell': amount_x_sell_total,
                    'z_spend': amount_z_spend_total,
                    'z_profit': profit_z_total
                },
                prices=(prices['yz'], prices['xz'], prices['xy'])
            )
            if normalized is not None:  # if arbitrage still exists after normalization & recalculation
                if not self.existing_arbitrages[pairs]['sell buy sell']:
                    self.existing_arbitrages[pairs]['sell buy sell'] = True
                return Arbitrage(
                    actions=[
                        MarketAction(triangle[0], 'sell', prices['yz'], normalized['y']),
                        MarketAction(triangle[1], 'buy', prices['xz'], normalized['x_buy']),
                        MarketAction(triangle[2], 'sell', prices['xy'], normalized['x_sell'])
                    ],
                    currency_z=currency_z,
                    amount_z=normalized['z_spend'],
                    profit_z=normalized['z_profit'],
                    profit_z_rel=normalized['z_profit_rel'],
                    profit_y=normalized['y_profit'],
                    currency_y=currency_y,
                    profit_x=normalized['x_profit'],
                    currency_x=currency_x
                )

        # checking triangle in another direction: buy Y/Z, sell X/Z, buy X/Y
        amount_x_buy_total = Decimal(0)
        amount_x_sell_total = Decimal(0)
        amount_y_total = Decimal(0)
        amount_z_spend_total = Decimal(0)
        profit_z_total = Decimal(0)
        prices = None
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
        if prices is not None:  # potential arbitrage exists
            # make amounts comply with order size requirements
            normalized = self.normalize_amounts_and_recalculate(
                symbols=(yz, xz, xy),
                direction='buy sell buy',
                amounts={
                    'y': amount_y_total,
                    'x_buy': amount_x_buy_total,
                    'x_sell': amount_x_sell_total,
                    'z_spend': amount_z_spend_total,
                    'z_profit': profit_z_total
                },
                prices=(prices['yz'], prices['xz'], prices['xy'])
            )
            if normalized is not None:  # if arbitrage still exists after normalization & recalculation
                if not self.existing_arbitrages[pairs]['buy sell buy']:
                    self.existing_arbitrages[pairs]['buy sell buy'] = True
                return Arbitrage(
                    actions=[
                        MarketAction(triangle[0], 'buy', prices['yz'], normalized['y']),
                        MarketAction(triangle[1], 'sell', prices['xz'], normalized['x_sell']),
                        MarketAction(triangle[2], 'buy', prices['xy'], normalized['x_buy'])
                    ],
                    currency_z=currency_z,
                    amount_z=normalized['z_spend'],
                    profit_z=normalized['z_profit'],
                    profit_z_rel=normalized['z_profit_rel'],
                    profit_y=normalized['y_profit'],
                    currency_y=currency_y,
                    profit_x=normalized['x_profit'],
                    currency_x=currency_x
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
