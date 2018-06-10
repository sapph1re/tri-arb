import peewee as pw
import argparse
import time
import sys
from decimal import Decimal
from config import TRADE_FEE, MIN_PROFIT, DB_USER, DB_PASS, DB_NAME, API_KEY, API_SECRET
from PyQt5.QtCore import QCoreApplication
from binance_api import BinanceApi
from arbitrage_detector import ArbitrageDetector, Arbitrage
from custom_logging import get_logger
logger = get_logger(__name__)


db = pw.MySQLDatabase(DB_NAME, user=DB_USER, password=DB_PASS, charset='utf8')


class BaseModel(pw.Model):
    class Meta:
        database = db


class DBArbitrageOpportunity(BaseModel):
    pairs = pw.CharField()  # e.g. "eth_btc eos_btc eos_eth"
    actions = pw.CharField()  # e.g. "sell buy sell"
    profit_x_avg = pw.DecimalField(max_digits=20, decimal_places=8, auto_round=True)
    profit_x_max = pw.DecimalField(max_digits=20, decimal_places=8, auto_round=True)
    currency_x = pw.CharField()  # e.g. "EOS"
    profit_y_avg = pw.DecimalField(max_digits=20, decimal_places=8, auto_round=True)
    profit_y_max = pw.DecimalField(max_digits=20, decimal_places=8, auto_round=True)
    currency_y = pw.CharField()  # e.g. "ETH"
    profit_z_avg = pw.DecimalField(max_digits=20, decimal_places=8, auto_round=True)
    profit_z_max = pw.DecimalField(max_digits=20, decimal_places=8, auto_round=True)
    amount_z_avg = pw.DecimalField(max_digits=20, decimal_places=8, auto_round=True)
    amount_z_max = pw.DecimalField(max_digits=20, decimal_places=8, auto_round=True)
    currency_z = pw.CharField()  # e.g. "BTC"
    profit_rel_avg = pw.DecimalField(max_digits=20, decimal_places=8, auto_round=True)
    profit_rel_max = pw.DecimalField(max_digits=20, decimal_places=8, auto_round=True)
    lifetime = pw.BigIntegerField()   # in milliseconds
    appeared_at = pw.BigIntegerField()   # unix timestamp in milliseconds

    class Meta:
        table_name = 'arbitrage_opportunity'


class DBArbitrageOpportunityActual(DBArbitrageOpportunity):
    original_opportunity_id = pw.IntegerField()

    class Meta:
        table_name = 'arbitrage_opportunity_actual'


class ArbitrageMonitor:
    def __init__(self):
        self.arbitrage_opportunities = {}  # {'pair pair pair': {'buy sell buy': ..., 'sell buy sell': ...}}
        self.api = BinanceApi(API_KEY, API_SECRET)
        symbols_info = self.api.get_symbols_info()
        self.detector = ArbitrageDetector(
            api=self.api,
            symbols_info=symbols_info,
            fee=TRADE_FEE,
            min_profit=MIN_PROFIT
        )
        self.detector.arbitrage_detected.connect(self._on_arbitrage_detected)
        self.detector.arbitrage_disappeared.connect(self._on_arbitrage_disappeared)

    @staticmethod
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
            # logger.error('Order price {} is not equal to the deepest trade price {}', price, level_price)
            # return None
            pass
        return counter_amount

    def _check_actual_arbitrage(self, arbitrage: Arbitrage):
        orderbooks = []
        profits = {}
        z_spend = Decimal(0)
        for action in arbitrage.actions:
            symbol = action.pair[0] + action.pair[1]
            depth = self.api.depth(symbol=symbol, limit=100)
            if action.action == 'sell':
                side = 'bids'
            elif action.action == 'buy':
                side = 'asks'
            ob = [(Decimal(price), Decimal(value)) for price, value, dummy in depth[side]]
            orderbooks.append(ob)
            counter_amount = self.calculate_counter_amount(action.amount, action.price, ob)
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
        profit_rel = profits[arbitrage.currency_z] / z_spend
        return Arbitrage(
            actions=arbitrage.actions,
            currency_z=arbitrage.currency_z,
            amount_z=z_spend,
            profit_z=profits[arbitrage.currency_z],
            profit_z_rel=profit_rel,
            profit_y=profits[arbitrage.currency_y],
            currency_y=arbitrage.currency_y,
            profit_x=profits[arbitrage.currency_x],
            currency_x=arbitrage.currency_x,
            orderbooks=tuple(orderbooks)
        )

    @staticmethod
    def _create_opportunity(arb: Arbitrage, original_id: int=0) -> DBArbitrageOpportunity:
        pairs = '{} {} {}'.format(arb.actions[0].pair, arb.actions[1].pair, arb.actions[2].pair)
        actions = '{} {} {}'.format(arb.actions[0].action, arb.actions[1].action, arb.actions[2].action)
        if not original_id:
            return DBArbitrageOpportunity.create(
                pairs=pairs,
                actions=actions,
                profit_x_avg=arb.profit_x,
                profit_x_max=arb.profit_x,
                currency_x=arb.currency_x,
                profit_y_avg=arb.profit_y,
                profit_y_max=arb.profit_y,
                currency_y=arb.currency_y,
                profit_z_avg=arb.profit_z,
                profit_z_max=arb.profit_z,
                amount_z_avg=arb.amount_z,
                amount_z_max=arb.amount_z,
                currency_z=arb.currency_z,
                profit_rel_avg=arb.profit_z_rel,
                profit_rel_max=arb.profit_z_rel,
                lifetime=1,
                appeared_at=int(1000 * time.time())
            )
        else:
            return DBArbitrageOpportunityActual.create(
                pairs=pairs,
                actions=actions,
                profit_x_avg=arb.profit_x,
                profit_x_max=arb.profit_x,
                currency_x=arb.currency_x,
                profit_y_avg=arb.profit_y,
                profit_y_max=arb.profit_y,
                currency_y=arb.currency_y,
                profit_z_avg=arb.profit_z,
                profit_z_max=arb.profit_z,
                amount_z_avg=arb.amount_z,
                amount_z_max=arb.amount_z,
                currency_z=arb.currency_z,
                profit_rel_avg=arb.profit_z_rel,
                profit_rel_max=arb.profit_z_rel,
                lifetime=1,
                appeared_at=int(1000 * time.time()),
                original_opportunity_id=original_id
            )

    @staticmethod
    def _update_opportunity(opp: DBArbitrageOpportunity, arb: Arbitrage):
        if arb.amount_z > opp.amount_z_max:
            opp.amount_z_max = arb.amount_z
        if arb.profit_x > opp.profit_x_max:
            opp.profit_x_max = arb.profit_x
        if arb.profit_y > opp.profit_y_max:
            opp.profit_y_max = arb.profit_y
        if arb.profit_z > opp.profit_z_max:
            opp.profit_z_max = arb.profit_z
        if arb.profit_z_rel > opp.profit_rel_max:
            opp.profit_rel_max = arb.profit_z_rel
        new_lifetime = int(1000 * time.time()) - opp.appeared_at
        if new_lifetime == 0:
            new_lifetime = 1
        lifetime_rel_diff = Decimal(opp.lifetime / new_lifetime)
        opp.profit_x_avg = opp.profit_x_avg * lifetime_rel_diff + arb.profit_x * (1 - lifetime_rel_diff)
        opp.profit_y_avg = opp.profit_y_avg * lifetime_rel_diff + arb.profit_y * (1 - lifetime_rel_diff)
        opp.profit_z_avg = opp.profit_z_avg * lifetime_rel_diff + arb.profit_z * (1 - lifetime_rel_diff)
        opp.amount_z_avg = opp.amount_z_avg * lifetime_rel_diff + arb.amount_z * (1 - lifetime_rel_diff)
        opp.profit_rel_avg = opp.profit_rel_avg * lifetime_rel_diff + arb.profit_z_rel * (1 - lifetime_rel_diff)
        opp.lifetime = new_lifetime
        opp.save()

    def _on_arbitrage_detected(self, arb: Arbitrage):
        pairs = '{} {} {}'.format(arb.actions[0].pair, arb.actions[1].pair, arb.actions[2].pair)
        actions = '{} {} {}'.format(arb.actions[0].action, arb.actions[1].action, arb.actions[2].action)
        arb_actual = self._check_actual_arbitrage(arb)
        if pairs not in self.arbitrage_opportunities:
            self.arbitrage_opportunities[pairs] = {}
        if actions not in self.arbitrage_opportunities[pairs]:
            self.arbitrage_opportunities[pairs][actions] = {}
        if not self.arbitrage_opportunities[pairs][actions]:
            self.arbitrage_opportunities[pairs][actions]['detected'] = self._create_opportunity(arb)
            original_id = self.arbitrage_opportunities[pairs][actions]['detected'].id
            self.arbitrage_opportunities[pairs][actions]['actual'] = self._create_opportunity(arb_actual, original_id)
        else:
            self._update_opportunity(self.arbitrage_opportunities[pairs][actions]['detected'], arb)
            self._update_opportunity(self.arbitrage_opportunities[pairs][actions]['actual'], arb_actual)

    def _on_arbitrage_disappeared(self, pairs: str, actions: str):
        if pairs not in self.arbitrage_opportunities:
            self.arbitrage_opportunities[pairs] = {}
        self.arbitrage_opportunities[pairs][actions] = {}


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--init-database', action='store_true')
    ap.add_argument('--version', action='version', version='Triangular Arbitrage Monitor 0.2')
    args = ap.parse_args()
    if args.init_database:
        db.create_tables([DBArbitrageOpportunity, DBArbitrageOpportunityActual], safe=True)
        logger.info('Database initialized')
        exit()

    logger.info('Starting...')
    app = QCoreApplication(sys.argv)
    monitor = ArbitrageMonitor()
    sys.exit(app.exec_())
