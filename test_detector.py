import sys
from decimal import Decimal, ROUND_DOWN
from config import API_KEY, API_SECRET, TRADE_FEE, MIN_PROFIT
from binance_api import BinanceApi
from arbitrage_detector import ArbitrageDetector
from PyQt5.QtCore import QCoreApplication
from logger import get_logger
logger = get_logger(__name__)


symbols_info = {}


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
        logger.error(f'Order price {price} is not equal to the deepest trade price {level_price}')
        return None
    return counter_amount


def verify_arbitrage_math(arbitrage):
    check_failed = False
    profits = {}
    z_spend = Decimal(0)
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
            logger.error(f'{symbol}: Price {action.price} lower than min_price {min_price}!')
        elif action.price > max_price:
            logger.error(f'{symbol}: Price {action.price} greater than max_price {max_price}!')
        elif action.price.quantize(price_step, rounding=ROUND_DOWN) != action.price:
            logger.error(f'{symbol}: Price {action.price} precision is higher than price_step {price_step}!')
        elif action.amount < min_amount:
            logger.error(f'{symbol}: Amount {action.amount} lower than min_amount {min_amount}!')
        elif action.amount > max_amount:
            logger.error(f'{symbol}: Amount {action.amount} greater than max_amount {max_amount}!')
        elif action.amount.quantize(amount_step, rounding=ROUND_DOWN) != action.amount:
            logger.error(f'{symbol}: Price {action.amount} precision is higher than price_step {amount_step}!')
        elif action.amount * action.price < min_notional:
            logger.error(
                f'{symbol}: Amount * price is less than min_notional! '
                f'{action.amount} * {action.price} < {min_notional}!'
            )
        else:
            all_good = True
        if not all_good:
            check_failed = True
            break
        # checking arbitrage calculations
        counter_amount = calculate_counter_amount(action.amount, action.price, arbitrage.orderbooks[i])
        if counter_amount is None:
            check_failed = True
            break
        for j in [0, 1]:
            if action.pair[j] not in profits:
                profits[action.pair[j]] = Decimal(0)
        if action.action == 'sell':
            profits[action.pair[0]] -= action.amount
            profits[action.pair[1]] += counter_amount * (1 - TRADE_FEE)
        elif action.action == 'buy':
            profits[action.pair[0]] += action.amount * (1 - TRADE_FEE)
            profits[action.pair[1]] -= counter_amount
            if action.pair[1] == arbitrage.currency_z:
                z_spend += counter_amount
        i += 1
    if not check_failed:
        # checking final profits
        for currency, profit in profits.items():
            if profit < 0:
                logger.error(f'Profit {currency} is negative: {profit}')
                check_failed = True
            if currency == arbitrage.currency_x:
                if arbitrage.profit_x != profit:
                    logger.error(f'Profit {currency} calculated wrong: reported {arbitrage.profit_x} vs real {profit}!')
                    check_failed = True
            if currency == arbitrage.currency_y:
                if arbitrage.profit_y != profit:
                    logger.error(f'Profit {currency} calculated wrong: reported {arbitrage.profit_y} vs real {profit}!')
                    check_failed = True
            if currency == arbitrage.currency_z:
                if arbitrage.profit_z != profit:
                    logger.error(f'Profit {currency} calculated wrong: reported {arbitrage.profit_z} vs real {profit}!')
                    check_failed = True
                profit_rel = profit / z_spend
                if arbitrage.profit_z_rel != profit_rel:
                    logger.error(f'Profit calculated wrong: reported {arbitrage.profit_z_rel*100}% vs real {profit_rel*100}!')
                    check_failed = True
                if profit_rel < MIN_PROFIT:
                    logger.error(f'Profit {profit_rel*100}% is less than min_profit {MIN_PROFIT*100}%')
                    check_failed = True
    if check_failed:
        logger.info(f'Arbitrage: {arbitrage}. Orderbooks: {arbitrage.orderbooks}')
    else:
        logger.info('Arbitrage OK')


if __name__ == '__main__':
    logger.info('Starting...')
    app = QCoreApplication(sys.argv)
    api = BinanceApi(API_KEY, API_SECRET)
    symbols_info = api.get_symbols_info()
    logger.debug(f'All Symbols Info: {symbols_info}')
    detector = ArbitrageDetector(
        api=api,
        symbols_info=symbols_info,
        fee=TRADE_FEE,
        min_profit=MIN_PROFIT
    )
    detector.arbitrage_detected.connect(verify_arbitrage_math)
    sys.exit(app.exec_())
