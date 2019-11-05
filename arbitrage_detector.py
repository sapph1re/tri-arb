import time
import asyncio
from pydispatch import dispatcher
from decimal import Decimal, ROUND_DOWN
from typing import Dict, Tuple, List
from itertools import combinations
from config import config, get_exchange_class
from exchanges.base_exchange import BaseExchange
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
            profit_x, currency_x, profit_y, currency_y, orderbooks, ts
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
        self.ts = ts

    def __str__(self):
        actions_str = ' -> '.join([str(action) for action in self.actions])
        return (
            f'{actions_str}, trade amount: {self.amount_z:f} {self.currency_z}, '
            f'profit: {self.profit_z:+f} {self.currency_z} ({self.profit_z_rel*100:+.2f}%), '
            f'{self.profit_x:+f} {self.currency_x}, {self.profit_y:+f} {self.currency_y}'
        )

    def __repr__(self):
        return self.__str__()


class ArbitrageDetector:
    def __init__(self, exchange: BaseExchange, fee: Decimal, min_profit: Decimal,
                 min_depth: int, min_age: int, reduce_factor: Decimal):
        """
        Launches Arbitrage Detector

        :param exchange: instance of the exchange class
        :param fee: trade fee on the exchange
        :param min_profit: detect arbitrage with this profit or higher
        :param min_depth: report arbitrage with this minimal depth
        :param min_age: report arbitrage only if it's this old (in seconds)
        :param reduce_factor: reduce the available arbitrage by this proportion
        """
        self._exchange = exchange
        self._fee = fee
        self._min_profit = min_profit
        self._min_depth = min_depth
        self._min_age = min_age * 1000  # converting to milliseconds
        self._reduce_factor = reduce_factor
        self._orderbooks = {}
        # existing arbitrages is a map of millisecond-timestamps of when the arbitrage was found, to then check its age
        self._existing_arbitrages = {}  # {'pair pair pair': {'buy sell buy': ..., 'sell buy sell': ...}}

        symbols_info = self._exchange.get_symbols_info()
        self._triangles = self._make_triangles(symbols_info)
        self._triangles, self._symbols = self._verify_triangles(self._triangles)
        # logger.info(f'Triangles: {self._triangles}')
        # logger.info(f'Symbols: {self._symbols}')

        # order amount requirements
        self._symbol_reqs = {}
        for symbol, info in symbols_info.items():
            self._symbol_reqs[symbol] = {
                'min_amount': info['min_amount'],
                'max_amount': info['max_amount'],
                'amount_step': info['amount_step'],
                'min_total': info['min_total']
            }

        # start watching the orderbooks
        dispatcher_connect_threadsafe(self.on_orderbook_changed, signal='orderbook_changed', sender=dispatcher.Any)
        self._orderbooks = self._exchange.run_orderbooks(self._symbols)

    @staticmethod
    def _make_asset_dicts(symbols_info: Dict[str, Dict[str, str]]) -> Tuple[dict, dict]:
        base_dict = {}
        quote_dict = {}
        for symbol, info in symbols_info.items():
            base_asset = info['base_asset']
            quote_asset = info['quote_asset']
            if base_asset not in base_dict:
                base_dict[base_asset] = set()
            if quote_asset not in quote_dict:
                quote_dict[quote_asset] = set()
            base_dict[base_asset].add(symbol)
            quote_dict[quote_asset].add(symbol)
        return base_dict, quote_dict

    def _make_triangles(self, symbols_info: Dict[str, Dict[str, str]]) -> set:
        """
        Find triangles in a list of symbols
        :param symbols_info: symbols to work with, format: {'base_asset': str, 'quote_asset': str}}
        :return: set of tuples of 3 tuples of base and quote assets
                Example: set{
                                ((ETH, BTC), (EOS, BTC), (EOS, ETH)),
                                ((ETH, BTC), (BNB, BTC), (BNB, ETH))
                            }
        """
        base_dict, quote_dict = self._make_asset_dicts(symbols_info)
        triangles = set()
        for quote, symbols in quote_dict.items():
            for a, b in combinations(symbols, 2):
                base1 = symbols_info[a]['base_asset']
                base2 = symbols_info[b]['base_asset']
                if (base1 not in quote_dict) and (base2 not in quote_dict):
                    continue
                c1 = self._exchange.make_symbol(base2, base1)
                c2 = self._exchange.make_symbol(base1, base2)
                if ((base1 in quote_dict) and (base2 in base_dict) and
                        (c1 in quote_dict[base1]) and (c1 in base_dict[base2])):
                    triangles.add(
                        ((base1, quote),
                         (base2, quote),
                         (base2, base1))
                    )
                elif ((base2 in quote_dict) and (base1 in base_dict) and
                      (c2 in quote_dict[base2]) and (c2 in base_dict[base1])):
                    triangles.add(
                        ((base2, quote),
                         (base1, quote),
                         (base1, base2))
                )
        return triangles

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
                    symbol = self._exchange.make_symbol(pair[0], pair[1])
                    if symbol not in symbols:
                        symbols[symbol] = {
                            'base': pair[0],
                            'quote': pair[1],
                            'triangles': set()
                        }
                    symbols[symbol]['triangles'].add(triangle)
        return triangles_verified, symbols

    def report_arbitrage(self, arbitrage: Arbitrage):
        # logger.info(f'Arbitrage found: {arbitrage}')
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
        amount_x_buy = amount_x_sell / (1 - self._fee)  # plus the fee, that's how much X we must buy
        if direction == 'sell buy sell':
            if amount_x_buy > xz[1]:  # if we can't buy enough X on X/Z
                amount_x_buy = xz[1]  # then we buy as much X as we can on X/Z
                amount_x_sell = amount_x_buy * (1 - self._fee)  # => minus the fee, that's how much X we can sell on X/Y
            amount_y = amount_x_sell * xy[0] * (1 - self._fee)  # Y we get from selling X, that we can sell on Y/Z
        elif direction == 'buy sell buy':
            if amount_x_buy > xy[1]:  # if we can't buy enough X on X/Y
                amount_x_buy = xy[1]  # then buy as much X as we can on X/Y
                amount_x_sell = amount_x_buy * (1 - self._fee)  # => minus the fee, that's how much X we can sell on X/Z
            amount_y = amount_x_buy * xy[0] / (1 - self._fee)  # Y we spend to buy X, plus the fee, we must buy on Y/Z
        if amount_y > yz[1]:  # if we can't trade that much Y on Y/Z
            amount_y = yz[1]  # then trade as much Y as we can on Y/Z
            if direction == 'sell buy sell':
                amount_x_sell = amount_y / xy[0] / (1 - self._fee)  # this much X we must sell on X/Y to have enough Y
                amount_x_buy = amount_x_sell / (1 - self._fee)  # plus the fee, this much X we must buy on X/Z
            elif direction == 'buy sell buy':
                amount_x_buy = amount_y * (1 - self._fee) / xy[0]  # this much X we must buy on X/Y to spend our Y
                amount_x_sell = amount_x_buy * (1 - self._fee)  # minus the fee, this much X we can sell
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
            if symbol not in self._symbol_reqs:
                logger.warning(f'Missing {symbol} symbol filters. Normalization failed.')
                return None
            # make sure that min_amount <= order amount <= max_amount
            if amounts[amount_type] < self._symbol_reqs[symbol]['min_amount']:
                return None
            elif amounts[amount_type] > self._symbol_reqs[symbol]['max_amount']:
                amounts_new[amount_type] = self._symbol_reqs[symbol]['max_amount']
            else:
                # round order amount precision to amount_step
                amounts_new[amount_type] = amounts[amount_type].quantize(
                    self._symbol_reqs[symbol]['amount_step'], rounding=ROUND_DOWN
                )
            # check that amount * price >= min_total
            if amounts_new[amount_type] * prices[symbol] < self._symbol_reqs[symbol]['min_total']:
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
                amounts_new['x_profit'] = amounts_new['x_buy'] * (1 - self._fee) - amounts_new['x_sell']
                if amounts_new['x_profit'] >= 0:
                    break
                amounts_new['x_sell'] -= self._symbol_reqs[xy]['amount_step']
                if amounts_new['x_sell'] < self._symbol_reqs[xy]['min_amount']:
                    return None
            # make sure y_profit >= 0
            while 1:
                y_got = self.calculate_counter_amount(amounts_new['x_sell'], orderbooks[xy]) * (1 - self._fee)
                y_spend = amounts_new['y']
                amounts_new['y_profit'] = y_got - y_spend
                if amounts_new['y_profit'] >= 0:
                    break
                amounts_new['y'] -= self._symbol_reqs[yz]['amount_step']
                if amounts_new['y'] < self._symbol_reqs[yz]['min_amount']:
                    return None
            # recalculate z_spend and z_profit with new amounts
            z_got = self.calculate_counter_amount(amounts_new['y'], orderbooks[yz]) * (1 - self._fee)
            amounts_new['z_spend'] = self.calculate_counter_amount(amounts_new['x_buy'], orderbooks[xz])
            amounts_new['z_profit'] = z_got - amounts_new['z_spend']
        elif direction == 'buy sell buy':
            amounts_new = self.normalize_amounts(amounts, {'y': yz, 'x_sell': xz, 'x_buy': xy}, prices)
            if amounts_new is None:
                return None
            # make sure y_profit >= 0
            while 1:
                y_got = amounts_new['y'] * (1 - self._fee)
                y_spend = self.calculate_counter_amount(amounts_new['x_buy'], orderbooks[xy])
                amounts_new['y_profit'] = y_got - y_spend
                if amounts_new['y_profit'] >= 0:
                    break
                amounts_new['x_buy'] -= self._symbol_reqs[xy]['amount_step']
                if amounts_new['x_buy'] < self._symbol_reqs[xy]['min_amount']:
                    return None
            # make sure x_profit >= 0
            while 1:
                amounts_new['x_profit'] = amounts_new['x_buy'] * (1 - self._fee) - amounts_new['x_sell']
                if amounts_new['x_profit'] >= 0:
                    break
                amounts_new['x_sell'] -= self._symbol_reqs[xz]['amount_step']
                if amounts_new['x_sell'] < self._symbol_reqs[xz]['min_amount']:
                    return None
            # recalculate z_spend and z_profit with new amounts
            z_got = self.calculate_counter_amount(amounts_new['x_sell'], orderbooks[xz]) * (1 - self._fee)
            amounts_new['z_spend'] = self.calculate_counter_amount(amounts_new['y'], orderbooks[yz])
            amounts_new['z_profit'] = z_got - amounts_new['z_spend']
        else:
            logger.warning(f'Bad direction: {direction}')
            return None
        # make sure we still meet minimal profit
        amounts_new['profit_rel'] = amounts_new['z_profit'] / amounts_new['z_spend']
        if amounts_new['profit_rel'] < self._min_profit:
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
        yz = self._exchange.make_symbol(arb.actions[0].pair[0], arb.actions[0].pair[1])
        xz = self._exchange.make_symbol(arb.actions[1].pair[0], arb.actions[1].pair[1])
        xy = self._exchange.make_symbol(arb.actions[2].pair[0], arb.actions[2].pair[1])
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
            orderbooks=arb.orderbooks,
            ts=arb.ts
        )

    def find_arbitrage_in_triangle(self, triangle: Tuple[Tuple[str, str], Tuple[str, str], Tuple[str, str]]) -> Arbitrage or None:
        """
        Looks for arbitrage in the triangle: Y/Z, X/Z, X/Y.

        X, Y, Z are three currencies for which exist the three currency pairs above

        :param triangle: ((Y, Z), (X, Z), (X, Y)) example: (('ETH', 'BTC'), ('EOS', 'BTC'), ('EOS', 'ETH'))
        :return: Arbitrage instance or None
        """
        yz = self._exchange.make_symbol(triangle[0][0], triangle[0][1])
        xz = self._exchange.make_symbol(triangle[1][0], triangle[1][1])
        xy = self._exchange.make_symbol(triangle[2][0], triangle[2][1])
        currency_z = triangle[0][1]
        currency_x = triangle[1][0]
        currency_y = triangle[0][0]
        # initializing existing_arbitrages storage
        pairs = '{} {} {}'.format(yz, xz, xy)
        if pairs not in self._existing_arbitrages:
            self._existing_arbitrages[pairs] = {}
        for actions in ['sell buy sell', 'buy sell buy']:
            if actions not in self._existing_arbitrages[pairs]:
                self._existing_arbitrages[pairs][actions] = 0
        # getting orderbooks
        for symbol in [yz, xz, xy]:
            if not self._orderbooks[symbol].is_valid():
                    # logger.debug('Orderbooks are not valid right now')
                    return None
        bids = {
            'yz': self._orderbooks[yz].get_bids(),
            'xz': self._orderbooks[xz].get_bids(),
            'xy': self._orderbooks[xy].get_bids()
        }
        asks = {
            'yz': self._orderbooks[yz].get_asks(),
            'xz': self._orderbooks[xz].get_asks(),
            'xy': self._orderbooks[xy].get_asks()
        }
        bids_saved = {'yz': bids['yz'].copy(), 'xz': bids['xz'].copy(), 'xy': bids['xy'].copy()}
        asks_saved = {'yz': asks['yz'].copy(), 'xz': asks['xz'].copy(), 'xy': asks['xy'].copy()}
        # checking that orderbooks are not empty
        for side in [bids, asks]:
            for pair in side:
                if not side[pair]:
                    # logger.debug('Orderbooks are empty (not ready yet?)')
                    return None

        arb_found = {
            'sell buy sell': False,
            'buy sell buy': False
        }

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
                profit_rel = bids['yz'][0][0] / asks['xz'][0][0] * bids['xy'][0][0] * (1 - self._fee) ** 3 - 1
            except IndexError:
                # orderbook is too short
                break
            if profit_rel < self._min_profit:
                break
            # calculate trade amounts available on this level
            amount_y, amount_x_buy, amount_x_sell = self.calculate_amounts_on_price_level(
                'sell buy sell', bids['yz'][0], asks['xz'][0], bids['xy'][0]
            )
            # calculate the profit on this level
            profit_z = amount_y * bids['yz'][0][0] * (1 - self._fee) - amount_x_buy * asks['xz'][0][0]
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
            amounts = self.limit_amounts(amounts, self._reduce_factor)
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
                if self._existing_arbitrages[pairs]['sell buy sell'] == 0:
                    self._existing_arbitrages[pairs]['sell buy sell'] = now
                arb_found['sell buy sell'] = True
                if now - self._existing_arbitrages[pairs]['sell buy sell'] >= self._min_age and arb_depth >= self._min_depth:
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
                        orderbooks=orderbooks,
                        ts=int(time.time() * 1000)
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
                profit_rel = bids['xz'][0][0] / asks['xy'][0][0] / asks['yz'][0][0] * (1 - self._fee) ** 3 - 1
            except IndexError:
                # orderbook is too short
                break
            if profit_rel < self._min_profit:
                break
            # calculate trade amounts available on this level
            amount_y, amount_x_buy, amount_x_sell = self.calculate_amounts_on_price_level(
                'buy sell buy', asks['yz'][0], bids['xz'][0], asks['xy'][0]
            )
            # calculate the profit on this level
            profit_z = amount_x_sell * bids['xz'][0][0] * (1 - self._fee) - amount_y * asks['yz'][0][0]
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
            amounts = self.limit_amounts(amounts, self._reduce_factor)
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
                if self._existing_arbitrages[pairs]['buy sell buy'] == 0:
                    self._existing_arbitrages[pairs]['buy sell buy'] = now
                arb_found['buy sell buy'] = True
                if now - self._existing_arbitrages[pairs]['buy sell buy'] >= self._min_age and arb_depth >= self._min_depth:
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
                        orderbooks=orderbooks,
                        ts=int(time.time() * 1000)
                    )

        # no arbitrage found
        # logger.info(f'No arbitrage found in {triangle}')
        for actions in ['sell buy sell', 'buy sell buy']:
            if not arb_found[actions] and self._existing_arbitrages[pairs][actions] > 0:
                self._existing_arbitrages[pairs][actions] = 0
                dispatcher.send(signal='arbitrage_disappeared', sender=self, pairs=pairs, actions=actions)
        return None

    def on_orderbook_changed(self, sender, symbol: str):
        try:
            triangles = self._symbols[symbol]['triangles']
        except KeyError:
            if symbol in self._symbols:
                logger.warning(f'Triangles missing for symbol {symbol}: {self._symbols[symbol]}')
            else:
                logger.warning(f'Symbol unknown: {symbol}')
        else:
            for triangle in triangles:
                arbitrage = self.find_arbitrage_in_triangle(triangle)
                if arbitrage is not None:
                    self.report_arbitrage(arbitrage)

    def get_book_volume_in_front(self, symbol: str, price: Decimal, side: str) -> Decimal:
        if side == 'BUY':
            bids = self._orderbooks[symbol].get_bids()
            return sum([v for p, v in bids if p > price])
        elif side == 'SELL':
            asks = self._orderbooks[symbol].get_asks()
            return sum([v for p, v in asks if p < price])


def test_on_arbitrage_detected(sender: ArbitrageDetector, arb: Arbitrage):
    logger.info(f'Arbitrage detected: {arb}')


def test_on_arbitrage_disappeared(sender: ArbitrageDetector, pairs: str, actions: str):
    logger.info(f'Arbitrage disappeared: {pairs} {actions}')


async def main():
    logger.info('Starting...')
    exchange_class = get_exchange_class()
    exchange = await exchange_class.create(
        config.get('Exchange', 'APIKey'),
        config.get('Exchange', 'APISecret')
    )
    logger.info('Exchange has started')
    detector = ArbitrageDetector(
        exchange=exchange,
        fee=config.getdecimal('Exchange', 'TradeFee'),
        min_profit=config.getdecimal('Arbitrage', 'MinProfit'),
        min_depth=config.getint('Arbitrage', 'MinArbDepth'),
        min_age=config.getint('Arbitrage', 'MinArbAge'),
        reduce_factor=config.getdecimal('Arbitrage', 'AmountReduceFactor')
    )
    logger.info('Arbitrage Detector has started')

    dispatcher.connect(test_on_arbitrage_detected, signal='arbitrage_detected', sender=detector)
    dispatcher.connect(test_on_arbitrage_disappeared, signal='arbitrage_disappeared', sender=detector)

    while True:
        await asyncio.sleep(1)


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
