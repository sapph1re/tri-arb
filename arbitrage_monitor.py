import peewee as pw
import argparse
import time
import sys
from decimal import Decimal
from config import TRADE_FEE, MIN_PROFIT, DB_USER, DB_PASS, DB_NAME, API_KEY, API_SECRET
from PyQt5.QtCore import QCoreApplication, QObject, pyqtSlot
from binance_api import BinanceApi
from binance_multiple_api_calls import BinanceMultipleApiCalls, BinanceApiCall
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


class DBArbitrageOpportunityActual(DBArbitrageOpportunity):
    original_opportunity_id = pw.IntegerField()

    class Meta:
        table_name = 'arbitrage_opportunity_actual'


class ArbitrageMonitor(QObject):
    def __init__(self):
        super(ArbitrageMonitor, self).__init__()
        self.opportunities = {}  # {'pair pair pair': {'buy sell buy': ..., 'sell buy sell': ...}}
        self.api = BinanceApi(API_KEY, API_SECRET)
        symbols_info = self.api.get_symbols_info()

        # format: {depth_multicall.id: (original_arbitrage, original_opportunity_id, actions_by_call_id)}
        self.context_for_check_actual_arbitrage = {}

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
        # if level_price != price:
            # logger.error('Order price {} is not equal to the deepest trade price {}', price, level_price)
            # return None
        return counter_amount

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
                appeared_at=int(1000 * time.time()),
                depth_avg=arb.arb_depth,
                depth_max=arb.arb_depth
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
                original_opportunity_id=original_id,
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

    @pyqtSlot(int, dict)
    def _do_check_actual_arbitrage(self, multicall_id: int, depth_results: dict):
        arb, original_opportunity_id, actions_by_call_id = self.context_for_check_actual_arbitrage[multicall_id]
        orderbooks = []
        profits = {}
        for call_id, depth in depth_results.items():
            action = actions_by_call_id[call_id]
            side = {'sell': 'bids', 'buy': 'asks'}[action.action]
            try:
                ob = [(Decimal(price), Decimal(value)) for price, value, dummy in depth[side]]
            except KeyError:
                logger.error('Call {} returned bad result: {}', call_id, depth)
                return
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
        profit_rel = profits[arb.currency_z] / arb.amount_z
        arb_actual = Arbitrage(
            actions=arb.actions,
            currency_z=arb.currency_z,
            amount_z=arb.amount_z,
            profit_z=profits[arb.currency_z],
            profit_z_rel=profit_rel,
            profit_y=profits[arb.currency_y],
            currency_y=arb.currency_y,
            profit_x=profits[arb.currency_x],
            currency_x=arb.currency_x,
            orderbooks=tuple(orderbooks),
            arb_depth=arb.arb_depth
        )
        pairs = '{} {} {}'.format(arb.actions[0].pair, arb.actions[1].pair, arb.actions[2].pair)
        actions = '{} {} {}'.format(arb.actions[0].action, arb.actions[1].action, arb.actions[2].action)
        if 'actual' in self.opportunities[pairs][actions] and self.opportunities[pairs][actions]['actual']:
            self._update_opportunity(self.opportunities[pairs][actions]['actual'], arb_actual)
        else:
            self.opportunities[pairs][actions]['actual'] = self._create_opportunity(arb_actual, original_opportunity_id)

    def _check_actual_arbitrage(self, arb: Arbitrage, original_opportunity_id: int):
        depth_api_calls = []
        actions_by_call_id = {}
        for action in arb.actions:
            call = BinanceApiCall(self.api.depth, {'symbol': action.pair[0]+action.pair[1], 'limit': 5})
            call_id = call.get_id()
            actions_by_call_id[call_id] = action
            depth_api_calls.append(call)
        depth_multicall = BinanceMultipleApiCalls(self.api, depth_api_calls, parent=self)
        self.context_for_check_actual_arbitrage[depth_multicall.get_id()] = (arb, original_opportunity_id, actions_by_call_id)
        depth_multicall.finished.connect(self._do_check_actual_arbitrage)
        depth_multicall.start_calls()

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
        original_id = self.opportunities[pairs][actions]['detected'].id
        self._check_actual_arbitrage(arb, original_id)

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
            new_lifetime = int(1000 * time.time()) - self.opportunities[pairs][actions]['actual'].appeared_at
            self.opportunities[pairs][actions]['actual'].lifetime = new_lifetime
            self.opportunities[pairs][actions]['actual'].save()
            # remove the opportunities
            self.opportunities[pairs][actions] = {}


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