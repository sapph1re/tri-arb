import sys
from decimal import Decimal, ROUND_DOWN
from config import API_KEY, API_SECRET, TRADE_FEE, MIN_PROFIT
from binance_api import BinanceApi
from arbitrage_detector import ArbitrageDetector
from PyQt5.QtCore import QCoreApplication
from custom_logging import get_logger
logger = get_logger(__name__)


symbols_info = {}
api = BinanceApi(API_KEY, API_SECRET)


def calculate_counter_amount(amount, price, orderbook):
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
    if level_price != price:
        logger.error('Order price {} is not equal to the deepest trade price {}', price, level_price)
        # return None
    return counter_amount


def verify_arbitrage_math(arbitrage):
    check_failed = False
    profits = {}
    z_spend = Decimal(0)
    orderbooks_actual = {}
    profits_actual = {}
    z_spend_actual = Decimal(0)
    i = 0
    for action in arbitrage.actions:
        symbol = action.pair[0]+action.pair[1]
        # checking amounts, if they fit to the requirements of the exchange
        qty_filter = symbols_info[symbol].get_qty_filter()
        price_filter = symbols_info[symbol].get_price_filter()
        min_price = Decimal(price_filter[0]).normalize()
        max_price = Decimal(price_filter[1]).normalize()
        price_step = Decimal(price_filter[2]).normalize()
        min_amount = Decimal(qty_filter[0]).normalize()
        max_amount = Decimal(qty_filter[1]).normalize()
        amount_step = Decimal(qty_filter[2]).normalize()
        min_notional = Decimal(symbols_info[symbol].get_min_notional()).normalize()
        all_good = False
        if action.price < min_price:
            logger.error('{}: Price {} lower than min_price {}!', symbol, action.price, min_price)
        elif action.price > max_price:
            logger.error('{}: Price {} greater than max_price {}!', symbol, action.price, max_price)
        elif action.price.quantize(price_step, rounding=ROUND_DOWN) != action.price:
            logger.error('{}: Price {} precision is higher than price_step {}!', symbol, action.price, price_step)
        elif action.amount < min_amount:
            logger.error('{}: Amount {} lower than min_amount {}!', symbol, action.amount, min_amount)
        elif action.amount > max_amount:
            logger.error('{}: Amount {} greater than max_amount {}!', symbol, action.amount, max_amount)
        elif action.amount.quantize(amount_step, rounding=ROUND_DOWN) != action.amount:
            logger.error('{}: Price {} precision is higher than price_step {}!', symbol, action.amount, amount_step)
        elif action.amount * action.price < min_notional:
            logger.error(
                '{}: Amount * price is less than min_notional! {} * {} < {}!',
                symbol, action.amount, action.price, min_notional
            )
        else:
            all_good = True
        if not all_good:
            check_failed = True
            break
        # checking arbitrage calculations
        counter_amount = calculate_counter_amount(action.amount, action.price, arbitrage.orderbooks[i])
        depth = api.depth(symbol=symbol, limit=100)
        if action.action == 'sell':
            side = 'bids'
        elif action.action == 'buy':
            side = 'asks'
        orderbooks_actual[symbol] = [(Decimal(price), Decimal(value)) for price, value, dummy in depth[side]]
        counter_amount_actual = calculate_counter_amount(action.amount, action.price, orderbooks_actual[symbol])
        if counter_amount is None:
            check_failed = True
            break
        for j in [0, 1]:
            if action.pair[j] not in profits:
                profits[action.pair[j]] = Decimal(0)
                profits_actual[action.pair[j]] = Decimal(0)
        if action.action == 'sell':
            profits[action.pair[0]] -= action.amount
            profits[action.pair[1]] += counter_amount * (1 - TRADE_FEE)
            profits_actual[action.pair[0]] -= action.amount
            profits_actual[action.pair[1]] += counter_amount_actual * (1 - TRADE_FEE)
        elif action.action == 'buy':
            profits[action.pair[0]] += action.amount * (1 - TRADE_FEE)
            profits[action.pair[1]] -= counter_amount
            profits_actual[action.pair[0]] += action.amount * (1 - TRADE_FEE)
            profits_actual[action.pair[1]] -= counter_amount_actual
            if action.pair[1] == arbitrage.currency_z:
                z_spend += counter_amount
                z_spend_actual += counter_amount_actual
        i += 1
    if not check_failed:
        # checking final profits
        for currency, profit in profits.items():
            if profit < 0:
                logger.error('Profit {} is negative: {}', currency, profit)
                check_failed = True
            if currency == arbitrage.currency_x:
                if arbitrage.profit_x != profit:
                    logger.error('Profit {} calculated wrong: reported {} vs real {}!', currency, arbitrage.profit_x, profit)
                    check_failed = True
            if currency == arbitrage.currency_y:
                if arbitrage.profit_y != profit:
                    logger.error('Profit {} calculated wrong: reported {} vs real {}!', currency, arbitrage.profit_y, profit)
                    check_failed = True
            if currency == arbitrage.currency_z:
                if arbitrage.profit_z != profit:
                    logger.error('Profit {} calculated wrong: reported {} vs real {}!', currency, arbitrage.profit_z, profit)
                    check_failed = True
                profit_rel = profit / z_spend
                if arbitrage.profit_z_rel != profit_rel:
                    logger.error('Profit calculated wrong: reported {}% vs real {}!', arbitrage.profit_z_rel*100, profit_rel*100)
                    check_failed = True
                if profit_rel < MIN_PROFIT:
                    logger.error('Profit {}% is less than min_profit {}%', profit_rel*100, MIN_PROFIT*100)
                    check_failed = True
        # checking actual profits
        for currency, profit_actual in profits_actual.items():
            if profit_actual < 0:
                logger.error('Actual profit {} is negative: {}', currency, profit_actual)
                check_failed = True
            logger.info('Actual profit:    {:.6f} {}    Expected profit: {:.6f} {}', profit_actual, currency, profits[currency], currency)
    if check_failed:
        logger.info('Arbitrage: {}. Orderbooks: {}', arbitrage, arbitrage.orderbooks)
    else:
        logger.info('Arbitrage OK')


if __name__ == '__main__':
    logger.info('Starting...')
    app = QCoreApplication(sys.argv)
    symbols_info = api.get_symbols_info()
    logger.debug('All Symbols Info: {}', symbols_info)
    detector = ArbitrageDetector(
        api=api,
        symbols_info=symbols_info,
        fee=TRADE_FEE,
        min_profit=MIN_PROFIT
    )
    detector.arbitrage_detected.connect(verify_arbitrage_math)
    sys.exit(app.exec_())
