import time
import asyncio
from pydispatch import dispatcher
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Tuple, List
from config import API_KEY, API_SECRET, TRADE_FEE, MIN_PROFIT, AMOUNT_REDUCE_FACTOR, MIN_ARBITRAGE_DEPTH, MIN_ARBITRAGE_AGE
from binance_api import BinanceApi, BinanceSymbolInfo
from binance_websocket import BinanceWebsocket
from binance_orderbook import BinanceOrderbook
from triangles_finder import TrianglesFinder
from helpers import dispatcher_connect_threadsafe
from logger import get_logger
logger = get_logger(__name__)


class MarketAction:
    def __init__(self, pair: Tuple[str, str], action: str, price: Decimal, amount: Decimal):
        self.pair = pair
        self.action = action
        self.price = price
        self.amount = amount

    def __str__(self):
        return f'{self.action} {self.amount:f} {self.pair[0]}/{self.pair[1]} @ {self.price:f}'

    def __repr__(self):
        return self.__str__()


class Arbitrage:
    def __init__(
            self, actions, currency_z, amount_z, profit_z, profit_z_rel,
            profit_x, currency_x, profit_y, currency_y, orderbooks
    ):
        self.actions = actions
        self.currency_z = currency_z
        self.amount_z = amount_z
        self.profit_z = profit_z
        self.profit_z_rel = profit_z_rel
        self.profit_y = profit_y
        self.currency_y = currency_y
        self.profit_x = profit_x
        self.currency_x = currency_x
        self.orderbooks = orderbooks

    def __str__(self):
        actions_str = ' -> '.join([str(action) for action in self.actions])
        return (
            f'{actions_str}, trade amount: {self.amount_z:f} {self.currency_z}, '
            f'profit: +{self.profit_z:f} {self.currency_z} (+{self.profit_z_rel*100}%), '
            f'+{self.profit_x:f} {self.currency_x}, +{self.profit_y:f} {self.currency_y}'
        )

    def __repr__(self):
        return self.__str__()


class ArbitrageDetector:
    def __init__(self, api: BinanceApi, symbols_info: Dict[str, BinanceSymbolInfo], fee: Decimal, min_profit: Decimal, min_depth: int, min_age: int):
        """
        Launches Arbitrage Detector

        :param api: BinanceApi instance
        :param symbols_info: {symbol: BinanceSymbolInfo, ...}
        :param fee: trade fee on the exchange
        :param min_profit: detect arbitrage with this profit or higher
        """
        self.api = api
        self.fee = fee
        self.min_profit = min_profit
        self.min_depth = min_depth
        self.min_age = min_age * 1000  # converting to milliseconds
        self.orderbooks = {}
        # existing arbitrages is a map of millisecond-timestamps of when the arbitrage was found, to then check its age
        self.existing_arbitrages = {}  # {'pair pair pair': {'buy sell buy': ..., 'sell buy sell': ...}}
        self.symbols_info = symbols_info
        self.triangles = TrianglesFinder().make_triangles(symbols_info)
        self.triangles, self.symbols = self._verify_triangles(self.triangles)
        # logger.debug(f'Triangles: {self.triangles}')
        # logger.debug(f'Symbols: {self.symbols}')

        # load order amount requirements
        self.symbols_filters = {}
        for symbol in self.symbols:
            qty_filter = self.symbols_info[symbol].get_qty_filter()
            self.symbols_filters[symbol] = {
                'min_amount': Decimal(qty_filter[0]).normalize(),
                'max_amount': Decimal(qty_filter[1]).normalize(),
                'amount_step': Decimal(qty_filter[2]).normalize(),
                'min_notional': Decimal(self.symbols_info[symbol].get_min_notional()).normalize()
            }

        # start watching the orderbooks
        self.websockets = []
        i = 999
        ws = None
        for symbol, details in self.symbols.items():
            # starting a websocket per every 50 symbols
            if i >= 50:
                ws = BinanceWebsocket()
                self.websockets.append(ws)
                i = 0
            # starting an orderbook watcher for every symbol
            ob = BinanceOrderbook(api=self.api, base=details['base'], quote=details['quote'], websocket=ws)
            self.orderbooks[symbol] = ob
            i += 1

        dispatcher_connect_threadsafe(self.on_orderbook_changed, signal='orderbook_changed', sender=dispatcher.Any)

        for ws in self.websockets:
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
        logger.info(f'Arbitrage found: {arbitrage}')
        dispatcher.send(signal='arbitrage_detected', sender=self, arb=arbitrage)

    def calculate_amounts_on_price_level(self, direction: str, yz: tuple, xz: tuple, xy: tuple) -> tuple:
        """
        Calculates available trade amount on one depth level in the triangle.

        :param direction: 'sell buy sell' or 'buy sell buy'
        :param yz: (price: Decimal, amount: Decimal) on Y/Z
        :param xz: (price: Decimal, amount: Decimal) on X/Z
        :param xy: (price: Decimal, amount: Decimal) on X/Y
        :return: (amount_y, amount_x_buy, amount_x_sell) - amounts to use for the orders
        """
        amount_x = min(xz[1], xy[1])
        amount_y = amount_x * xy[0]
        amount_x_sell = amount_x  # this much we can sell for sure
        amount_x_buy = amount_x_sell / (1 - self.fee)  # plus the fee, that's how much X we must buy
        if direction == 'sell buy sell':
            if amount_x_buy > xz[1]:  # if we can't buy enough X on X/Z
                amount_x_buy = xz[1]  # then we buy as much X as we can on X/Z
                amount_x_sell = amount_x_buy * (1 - self.fee)  # => minus the fee, that's how much X we can sell on X/Y
            amount_y = amount_x_sell * xy[0] * (1 - self.fee)  # Y we get from selling X, that we can sell on Y/Z
        elif direction == 'buy sell buy':
            if amount_x_buy > xy[1]:  # if we can't buy enough X on X/Y
                amount_x_buy = xy[1]  # then buy as much X as we can on X/Y
                amount_x_sell = amount_x_buy * (1 - self.fee)  # => minus the fee, that's how much X we can sell on X/Z
            amount_y = amount_x_buy * xy[0] / (1 - self.fee)  # Y we spend to buy X, plus the fee, we must buy on Y/Z
        if amount_y > yz[1]:  # if we can't trade that much Y on Y/Z
            amount_y = yz[1]  # then trade as much Y as we can on Y/Z
            if direction == 'sell buy sell':
                amount_x_sell = amount_y / xy[0] / (1 - self.fee)  # this much X we must sell on X/Y to have enough Y
                amount_x_buy = amount_x_sell / (1 - self.fee)  # plus the fee, this much X we must buy on X/Z
            elif direction == 'buy sell buy':
                amount_x_buy = amount_y * (1 - self.fee) / xy[0]  # this much X we must buy on X/Y to spend our Y
                amount_x_sell = amount_x_buy * (1 - self.fee)  # minus the fee, this much X we can sell
        # integrity check, have we calculated everything correctly?
        if (amount_y > yz[1] or ((amount_x_buy > xz[1] or amount_x_sell > xy[1]) and direction == 'sell buy sell')
                             or ((amount_x_sell > xz[1] or amount_x_buy > xy[1]) and direction == 'buy sell buy')):
            raise Exception('Bad calculation!')
        return amount_y, amount_x_buy, amount_x_sell

    @staticmethod
    def calculate_counter_amount(amount: Decimal, orderbook: List[Tuple[Decimal, Decimal]]) -> Tuple[Decimal, Decimal]:
        """
        Goes through the orderbook and calculates the amount of counter currency.

        :param amount: amount to sell or buy on the given orderbook
        :param orderbook: [(price, amount), (price, amount), ...] the orderbook in the needed direction
        :return: amount of counter currency (fee not counted)
        """
        counter_amount = Decimal(0)
        amount_left = amount
        for level_price, level_amount in orderbook:
            if amount_left > level_amount:
                trade_amount = level_amount
            else:
                trade_amount = amount_left
            counter_amount += level_price * trade_amount
            amount_left -= trade_amount
            if amount_left <= 0:
                break
        if amount_left < 0:
            logger.critical(f'calculate_counter_amount() is bad: amount_left is negative: {amount_left}')
            raise Exception('Critical calculation error')
        return counter_amount

    def normalize_amounts(self, amounts: Dict[str, Decimal], amounts_to_symbols: Dict[str, str], prices: Dict[str, Decimal]):
        """
        Changes the amounts to fit into order amount requirements.

        :param amounts: {y, x_buy, x_sell}  Raw order amounts.
        :param amounts_to_symbols: {'y': symbol, 'x_buy': symbol, 'x_sell': symbol}
            Mapping to indicate on which symbol you perform a corresponding operation.
        :param prices: {symbol: price, symbol: price, symbol: price}
            Order prices, for each symbol
        :return: {y, x_buy, x_sell} Normalized order amounts
        """
        amounts_new = {}
        for amount_type, symbol in amounts_to_symbols.items():
            # check that we have all symbols info
            if symbol not in self.symbols_filters:
                logger.warning(f'Missing {symbol} symbol filters. Normalization failed.')
                return None
            # make sure that min_amount <= order amount <= max_amount
            if amounts[amount_type] < self.symbols_filters[symbol]['min_amount']:
                return None
            elif amounts[amount_type] > self.symbols_filters[symbol]['max_amount']:
                amounts_new[amount_type] = self.symbols_filters[symbol]['max_amount']
            else:
                # round order amount precision to amount_step
                amounts_new[amount_type] = amounts[amount_type].quantize(
                    self.symbols_filters[symbol]['amount_step'], rounding=ROUND_DOWN
                )
            # check that amount * price >= min_notional
            if amounts_new[amount_type] * prices[symbol] < self.symbols_filters[symbol]['min_notional']:
                return None  # amount is too little
        return amounts_new

    def normalize_amounts_and_recalculate(
        self,
        symbols: Tuple[str, str, str], direction: str,
        amounts: Dict[str, Decimal], prices: Tuple[Decimal, Decimal, Decimal],
        orderbooks: Tuple[List[Tuple], List[Tuple], List[Tuple]]
    ) -> Dict[str, Decimal] or None:
        """
        Takes arbitrage amounts and normalizes them to comply with correct order amounts on the exchange.
        Recalculates the resulting amounts to still maintain a proper arbitrage.

        :param symbols: (YZ, XZ, XY) tuple of symbols we're trading at, e.g. ('ETHBTC', 'EOSBTC', 'EOSETH')
        :param direction: 'sell buy sell' or 'buy sell buy'
        :param amounts: {y, x_buy, x_sell, z_spend, z_profit} amounts of the arbitrage
        :param prices: {yz, xz, xy} prices for the three symbols
        :return: {y, x_buy, x_sell, z_spend, x_profit, y_profit, z_profit, profit_rel}
            Amounts normalized and recalculated, also reports expected y_profit and x_profit.
            Returns None when misused or amounts are too little.
        """
        yz, xz, xy = symbols
        prices = {yz: prices[0], xz: prices[1], xy: prices[2]}
        orderbooks = {yz: orderbooks[0], xz: orderbooks[1], xy: orderbooks[2]}
        # logger.debug(f'Amounts before normalizing: {amounts}. Prices: {prices}. Symbols: {symbols}. Direction: {direction}.')
        # normalize amounts to comply with min/max order amounts and min amount step
        if direction == 'sell buy sell':
            amounts_new = self.normalize_amounts(amounts, {'y': yz, 'x_buy': xz, 'x_sell': xy}, prices)
            if amounts_new is None:
                return None
            # make sure x_profit >= 0
            while 1:
                amounts_new['x_profit'] = amounts_new['x_buy'] * (1 - self.fee) - amounts_new['x_sell']
                if amounts_new['x_profit'] >= 0:
                    break
                amounts_new['x_sell'] -= self.symbols_filters[xy]['amount_step']
                if amounts_new['x_sell'] < self.symbols_filters[xy]['min_amount']:
                    return None
            # make sure y_profit >= 0
            while 1:
                y_got = self.calculate_counter_amount(amounts_new['x_sell'], orderbooks[xy]) * (1 - self.fee)
                y_spend = amounts_new['y']
                amounts_new['y_profit'] = y_got - y_spend
                if amounts_new['y_profit'] >= 0:
                    break
                amounts_new['y'] -= self.symbols_filters[yz]['amount_step']
                if amounts_new['y'] < self.symbols_filters[yz]['min_amount']:
                    return None
            # recalculate z_spend and z_profit with new amounts
            z_got = self.calculate_counter_amount(amounts_new['y'], orderbooks[yz]) * (1 - self.fee)
            amounts_new['z_spend'] = self.calculate_counter_amount(amounts_new['x_buy'], orderbooks[xz])
            amounts_new['z_profit'] = z_got - amounts_new['z_spend']
        elif direction == 'buy sell buy':
            amounts_new = self.normalize_amounts(amounts, {'y': yz, 'x_sell': xz, 'x_buy': xy}, prices)
            if amounts_new is None:
                return None
            # make sure y_profit >= 0
            while 1:
                y_got = amounts_new['y'] * (1 - self.fee)
                y_spend = self.calculate_counter_amount(amounts_new['x_buy'], orderbooks[xy])
                amounts_new['y_profit'] = y_got - y_spend
                if amounts_new['y_profit'] >= 0:
                    break
                amounts_new['x_buy'] -= self.symbols_filters[xy]['amount_step']
                if amounts_new['x_buy'] < self.symbols_filters[xy]['min_amount']:
                    return None
            # make sure x_profit >= 0
            while 1:
                amounts_new['x_profit'] = amounts_new['x_buy'] * (1 - self.fee) - amounts_new['x_sell']
                if amounts_new['x_profit'] >= 0:
                    break
                amounts_new['x_sell'] -= self.symbols_filters[xz]['amount_step']
                if amounts_new['x_sell'] < self.symbols_filters[xz]['min_amount']:
                    return None
            # recalculate z_spend and z_profit with new amounts
            z_got = self.calculate_counter_amount(amounts_new['x_sell'], orderbooks[xz]) * (1 - self.fee)
            amounts_new['z_spend'] = self.calculate_counter_amount(amounts_new['y'], orderbooks[yz])
            amounts_new['z_profit'] = z_got - amounts_new['z_spend']
        else:
            logger.warning(f'Bad direction: {direction}')
            return None
        # make sure it's still profitable
        if amounts_new['z_profit'] < 0:
            return None
        amounts_new['profit_rel'] = amounts_new['z_profit'] / amounts_new['z_spend']
        if amounts_new['profit_rel'] < self.min_profit:
            return None
        return amounts_new

    def limit_amounts(self, amounts: Dict[str, Decimal], reduce_factor: Decimal) -> Dict[str, Decimal]:
        """
        Reduces amounts to stay away from the edge
        :param amounts: dict of amounts
        :param reduce_factor: the multiplier
        :return: new amounts
        """
        new_amounts = {k: v*reduce_factor for k, v in amounts.items()}
        return new_amounts

    def reduce_arbitrage(self, arb: Arbitrage, reduce_factor: Decimal) -> Arbitrage or None:
        """
        Reduces amounts of a given arbitrage by a given factor
        :param arb: initial arbitrage
        :param reduce_factor: the multiplier
        :return: new arbitrage (only amounts are changed) or None if there is no arbitrage
        available with lower amounts
        """
        amounts = {
            'y': arb.actions[0].amount,
            'z_spend': arb.amount_z,
            'z_profit': arb.profit_z
        }
        # we assume here that it's either "sell buy sell" or "buy sell buy"
        # exactly in the YZ, XZ, XY sequence
        if arb.actions[0].action == 'sell':
            direction = 'sell buy sell'
            amounts['x_buy'] = arb.actions[1].amount
            amounts['x_sell'] = arb.actions[2].amount
        else:
            direction = 'buy sell buy'
            amounts['x_buy'] = arb.actions[2].amount
            amounts['x_sell'] = arb.actions[1].amount
        yz = ''.join(arb.actions[0].pair)
        xz = ''.join(arb.actions[1].pair)
        xy = ''.join(arb.actions[2].pair)
        # logger.debug(f'Amounts before reduction: {amounts}')
        new_amounts = self.limit_amounts(amounts, reduce_factor)
        # logger.debug(f'Amounts reduced: {new_amounts}')
        normalized = self.normalize_amounts_and_recalculate(
            symbols=(yz, xz, xy),
            direction=direction,
            amounts=new_amounts,
            prices=(arb.actions[0].price, arb.actions[1].price, arb.actions[2].price),
            orderbooks=arb.orderbooks
        )
        if normalized is None:
            logger.debug('No reduced arbitrage available')
            return None
        # logger.debug(f'Reduced amounts normalized and recalculated: {normalized}')
        if direction == 'sell buy sell':
            new_actions = [
                MarketAction(arb.actions[0].pair, 'sell', arb.actions[0].price, normalized['y']),
                MarketAction(arb.actions[1].pair, 'buy', arb.actions[1].price, normalized['x_buy']),
                MarketAction(arb.actions[2].pair, 'sell', arb.actions[2].price, normalized['x_sell'])
            ]
        else:  # buy sell buy
            new_actions = [
                MarketAction(arb.actions[0].pair, 'buy', arb.actions[0].price, normalized['y']),
                MarketAction(arb.actions[1].pair, 'sell', arb.actions[1].price, normalized['x_sell']),
                MarketAction(arb.actions[2].pair, 'buy', arb.actions[2].price, normalized['x_buy'])
            ]
        return Arbitrage(
            actions=new_actions,
            currency_z=arb.currency_z,
            amount_z=normalized['z_spend'],
            profit_z=normalized['z_profit'],
            profit_z_rel=normalized['profit_rel'],
            profit_y=normalized['y_profit'],
            currency_y=arb.currency_y,
            profit_x=normalized['x_profit'],
            currency_x=arb.currency_x,
            orderbooks=arb.orderbooks
        )

    def find_arbitrage_in_triangle(self, triangle: Tuple[Tuple[str, str], Tuple[str, str], Tuple[str, str]]) -> Arbitrage or None:
        """
        Looks for arbitrage in the triangle: Y/Z, X/Z, X/Y.

        X, Y, Z are three currencies for which exist the three currency pairs above

        :param triangle: ((Y, Z), (X, Z), (X, Y)) example: (('ETH', 'BTC'), ('EOS', 'BTC'), ('EOS', 'ETH'))
        :return: Arbitrage instance or None
        """
        yz = triangle[0][0] + triangle[0][1]
        xz = triangle[1][0] + triangle[1][1]
        xy = triangle[2][0] + triangle[2][1]
        currency_z = triangle[0][1]
        currency_x = triangle[1][0]
        currency_y = triangle[0][0]
        # initializing existing_arbitrages storage
        pairs = '{} {} {}'.format(yz, xz, xy)
        if pairs not in self.existing_arbitrages:
            self.existing_arbitrages[pairs] = {}
        for actions in ['sell buy sell', 'buy sell buy']:
            if actions not in self.existing_arbitrages[pairs]:
                self.existing_arbitrages[pairs][actions] = 0
        # getting orderbooks
        for symbol in [yz, xz, xy]:
            if not self.orderbooks[symbol].is_valid():
                    # logger.debug('Orderbooks are not valid right now')
                    return None
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
        bids_saved = {'yz': bids['yz'].copy(), 'xz': bids['xz'].copy(), 'xy': bids['xy'].copy()}
        asks_saved = {'yz': asks['yz'].copy(), 'xz': asks['xz'].copy(), 'xy': asks['xy'].copy()}
        # checking that orderbooks are not empty
        for side in [bids, asks]:
            for pair in side:
                if not side[pair]:
                    # logger.debug('Orderbooks are empty (not ready yet?)')
                    return None
        # checking triangle in one direction: sell Y/Z, buy X/Z, sell X/Y
        amount_x_buy_total = Decimal(0)
        amount_x_sell_total = Decimal(0)
        amount_y_total = Decimal(0)
        amount_z_spend_total = Decimal(0)
        profit_z_total = Decimal(0)
        prices = None
        arb_depth = 0
        while 1:
            # check profitability
            try:
                profit_rel = bids['yz'][0][0] / asks['xz'][0][0] * bids['xy'][0][0] * (1 - self.fee) ** 3 - 1
            except IndexError:
                # orderbook is too short
                break
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
                if not ob[0][1]:
                    del ob[0]
            arb_depth += 1
        if prices is not None:  # potential arbitrage exists
            orderbooks = (bids_saved['yz'], asks_saved['xz'], bids_saved['xy'])
            # make amounts comply with order size requirements
            amounts = {
                'y': amount_y_total,
                'x_buy': amount_x_buy_total,
                'x_sell': amount_x_sell_total,
                'z_spend': amount_z_spend_total,
                'z_profit': profit_z_total
            }
            # logger.debug(f'Amounts before recalculation: {amounts}')
            amounts = self.limit_amounts(amounts, AMOUNT_REDUCE_FACTOR)
            # logger.debug(f'Amounts limited: {amounts}')
            normalized = self.normalize_amounts_and_recalculate(
                symbols=(yz, xz, xy),
                direction='sell buy sell',
                amounts=amounts,
                prices=(prices['yz'], prices['xz'], prices['xy']),
                orderbooks=orderbooks
            )
            # logger.debug(f'Amounts normalized and recalculated: {normalized}')
            if normalized is not None:  # if arbitrage still exists after normalization & recalculation
                now = int(time.time()*1000)
                if self.existing_arbitrages[pairs]['sell buy sell'] == 0:
                    self.existing_arbitrages[pairs]['sell buy sell'] = now
                    logger.info(f'New arb: {pairs} sell buy sell')
                else:
                    logger.info(
                        f'Repeating arb: {pairs} sell buy sell, '
                        f'age: {(now - self.existing_arbitrages[pairs]["sell buy sell"])/1000}s'
                    )
                if now - self.existing_arbitrages[pairs]['sell buy sell'] >= self.min_age and arb_depth >= self.min_depth:
                    return Arbitrage(
                        actions=[
                            MarketAction(triangle[0], 'sell', prices['yz'], normalized['y']),
                            MarketAction(triangle[1], 'buy', prices['xz'], normalized['x_buy']),
                            MarketAction(triangle[2], 'sell', prices['xy'], normalized['x_sell'])
                        ],
                        currency_z=currency_z,
                        amount_z=normalized['z_spend'],
                        profit_z=normalized['z_profit'],
                        profit_z_rel=normalized['profit_rel'],
                        profit_y=normalized['y_profit'],
                        currency_y=currency_y,
                        profit_x=normalized['x_profit'],
                        currency_x=currency_x,
                        orderbooks=orderbooks
                    )

        # checking triangle in another direction: buy Y/Z, sell X/Z, buy X/Y
        amount_x_buy_total = Decimal(0)
        amount_x_sell_total = Decimal(0)
        amount_y_total = Decimal(0)
        amount_z_spend_total = Decimal(0)
        profit_z_total = Decimal(0)
        prices = None
        arb_depth = 0
        while 1:
            # check profitability
            try:
                profit_rel = bids['xz'][0][0] / asks['xy'][0][0] / asks['yz'][0][0] * (1 - self.fee) ** 3 - 1
            except IndexError:
                # orderbook is too short
                break
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
                if not ob[0][1]:
                    del ob[0]
            arb_depth += 1
        if prices is not None:  # potential arbitrage exists
            orderbooks = (asks_saved['yz'], bids_saved['xz'], asks_saved['xy'])
            # make amounts comply with order size requirements
            amounts = {
                'y': amount_y_total,
                'x_buy': amount_x_buy_total,
                'x_sell': amount_x_sell_total,
                'z_spend': amount_z_spend_total,
                'z_profit': profit_z_total
            }
            # logger.debug(f'Amounts before recalculation: {amounts}')
            amounts = self.limit_amounts(amounts, AMOUNT_REDUCE_FACTOR)
            # logger.debug(f'Amounts limited: {amounts}')
            normalized = self.normalize_amounts_and_recalculate(
                symbols=(yz, xz, xy),
                direction='buy sell buy',
                amounts=amounts,
                prices=(prices['yz'], prices['xz'], prices['xy']),
                orderbooks=orderbooks
            )
            # logger.debug(f'Amounts normalized and recalculated: {normalized}')
            if normalized is not None:  # if arbitrage still exists after normalization & recalculation
                now = int(time.time()*1000)
                if self.existing_arbitrages[pairs]['buy sell buy'] == 0:
                    self.existing_arbitrages[pairs]['buy sell buy'] = now
                    logger.info(f'New arb: {pairs} buy sell buy')
                else:
                    logger.info(
                        f'Repeating arb: {pairs} buy sell buy,'
                        f'age: {(now - self.existing_arbitrages[pairs]["sell buy sell"])/1000}s'
                    )
                if now - self.existing_arbitrages[pairs]['buy sell buy'] >= self.min_age and arb_depth >= self.min_depth:
                    return Arbitrage(
                        actions=[
                            MarketAction(triangle[0], 'buy', prices['yz'], normalized['y']),
                            MarketAction(triangle[1], 'sell', prices['xz'], normalized['x_sell']),
                            MarketAction(triangle[2], 'buy', prices['xy'], normalized['x_buy'])
                        ],
                        currency_z=currency_z,
                        amount_z=normalized['z_spend'],
                        profit_z=normalized['z_profit'],
                        profit_z_rel=normalized['profit_rel'],
                        profit_y=normalized['y_profit'],
                        currency_y=currency_y,
                        profit_x=normalized['x_profit'],
                        currency_x=currency_x,
                        orderbooks=orderbooks
                    )

        # no arbitrage found
        # logger.info('No arbitrage found')
        for actions in ['sell buy sell', 'buy sell buy']:
            if self.existing_arbitrages[pairs][actions] > 0:
                logger.info(f'Arb disappeared: {pairs} {actions}')
                self.existing_arbitrages[pairs][actions] = 0
                dispatcher.send(signal='arbitrage_disappeared', sender=self, pairs=pairs, actions=actions)
        return None

    def on_orderbook_changed(self, sender, symbol: str):
        try:
            for triangle in self.symbols[symbol]['triangles']:
                arbitrage = self.find_arbitrage_in_triangle(triangle)
                if arbitrage is not None:
                    self.report_arbitrage(arbitrage)
        except KeyError:
            logger.warning(f'Symbol {symbol} is unknown')
            return

    def get_book_volume_in_front(self, symbol: str, price: Decimal, side: str) -> Decimal:
        if side == 'BUY':
            bids = self.orderbooks[symbol].get_bids()
            return sum([v for p, v in bids if p > price])
        elif side == 'SELL':
            asks = self.orderbooks[symbol].get_asks()
            return sum([v for p, v in asks if p < price])


def test_on_arbitrage_detected(sender: ArbitrageDetector, arb: Arbitrage):
    logger.info(f'Arbitrage detected: {arb}')


def test_on_arbitrage_disappeared(sender: ArbitrageDetector, pairs: str, actions: str):
    logger.info(f'Arbitrage disappeared: {pairs} {actions}')


async def main():
    logger.info('Starting...')
    api = await BinanceApi.create(API_KEY, API_SECRET)
    symbols_info = await api.get_symbols_info()
    # symbols_info_slice = {}
    # i = 0
    # for symbol, symbol_info in symbols_info.items():
    #     symbols_info_slice[symbol] = symbol_info
    #     i += 1
    #     if i >= 20:
    #         break
    # logger.debug(f'All Symbols Info: {symbols_info}')
    detector = ArbitrageDetector(
        api=api,
        symbols_info=symbols_info,
        fee=TRADE_FEE,
        min_profit=MIN_PROFIT,
        min_depth=MIN_ARBITRAGE_DEPTH,
        min_age=MIN_ARBITRAGE_AGE
    )

    dispatcher.connect(test_on_arbitrage_detected, signal='arbitrage_detected', sender=detector)
    dispatcher.connect(test_on_arbitrage_disappeared, signal='arbitrage_disappeared', sender=detector)

    while True:
        await asyncio.sleep(1)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
