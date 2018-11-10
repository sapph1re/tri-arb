import peewee as pw
import argparse
import time
import sys
from decimal import Decimal
from config import TRADE_FEE, MIN_PROFIT, DB_USER, DB_PASS, DB_NAME, API_KEY, API_SECRET
from PyQt5.QtCore import QCoreApplication, QObject, pyqtSlot
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
    depth_avg = pw.IntegerField()
    depth_max = pw.IntegerField()

    class Meta:
        table_name = 'arbitrage_opportunity'


class ArbitrageMonitor(QObject):
    def __init__(self):
        super(ArbitrageMonitor, self).__init__()
        self.opportunities = {}  # {'pair pair pair': {'buy sell buy': ..., 'sell buy sell': ...}}
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
    def _create_opportunity(arb: Arbitrage) -> DBArbitrageOpportunity:
        pairs = '{} {} {}'.format(arb.actions[0].pair, arb.actions[1].pair, arb.actions[2].pair)
        actions = '{} {} {}'.format(arb.actions[0].action, arb.actions[1].action, arb.actions[2].action)
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
            appeared_at=int(1000 * time.time()),
            depth_avg=arb.arb_depth,
            depth_max=arb.arb_depth
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
        if arb.arb_depth > opp.depth_max:
            opp.depth_max = arb.arb_depth
        new_lifetime = int(1000 * time.time()) - opp.appeared_at
        if new_lifetime == 0:
            new_lifetime = 1
        lifetime_rel_diff = Decimal(opp.lifetime / new_lifetime)
        opp.profit_x_avg = opp.profit_x_avg * lifetime_rel_diff + arb.profit_x * (1 - lifetime_rel_diff)
        opp.profit_y_avg = opp.profit_y_avg * lifetime_rel_diff + arb.profit_y * (1 - lifetime_rel_diff)
        opp.profit_z_avg = opp.profit_z_avg * lifetime_rel_diff + arb.profit_z * (1 - lifetime_rel_diff)
        opp.amount_z_avg = opp.amount_z_avg * lifetime_rel_diff + arb.amount_z * (1 - lifetime_rel_diff)
        opp.profit_rel_avg = opp.profit_rel_avg * lifetime_rel_diff + arb.profit_z_rel * (1 - lifetime_rel_diff)
        opp.depth_avg = opp.depth_avg * lifetime_rel_diff + arb.arb_depth * (1 - lifetime_rel_diff)
        opp.lifetime = new_lifetime
        opp.save()

    @pyqtSlot('PyQt_PyObject')
    def _on_arbitrage_detected(self, arb: Arbitrage):
        pairs = '{} {} {}'.format(arb.actions[0].pair, arb.actions[1].pair, arb.actions[2].pair)
        actions = '{} {} {}'.format(arb.actions[0].action, arb.actions[1].action, arb.actions[2].action)
        if pairs not in self.opportunities:
            self.opportunities[pairs] = {}
        if actions not in self.opportunities[pairs]:
            self.opportunities[pairs][actions] = {}
        if not self.opportunities[pairs][actions]:
            self.opportunities[pairs][actions]['detected'] = self._create_opportunity(arb)
        else:
            self._update_opportunity(self.opportunities[pairs][actions]['detected'], arb)

    @pyqtSlot(str, str)
    def _on_arbitrage_disappeared(self, pairs: str, actions: str):
        if pairs not in self.opportunities:
            self.opportunities[pairs] = {}
        if actions not in self.opportunities[pairs]:
            self.opportunities[pairs][actions] = {}
        if self.opportunities[pairs][actions]:
            # update lifetimes
            new_lifetime = int(1000 * time.time()) - self.opportunities[pairs][actions]['detected'].appeared_at
            self.opportunities[pairs][actions]['detected'].lifetime = new_lifetime
            self.opportunities[pairs][actions]['detected'].save()
            # remove the opportunities
            self.opportunities[pairs][actions] = {}


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--init-database', action='store_true')
    ap.add_argument('--version', action='version', version='Triangular Arbitrage Monitor 0.2')
    args = ap.parse_args()
    if args.init_database:
        db.create_tables([DBArbitrageOpportunity], safe=True)
        logger.info('Database initialized')
        exit()

    logger.info('Starting...')
    app = QCoreApplication(sys.argv)
    monitor = ArbitrageMonitor()
    sys.exit(app.exec_())
