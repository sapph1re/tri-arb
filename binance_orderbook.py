import json
import time
from sortedcontainers import SortedSet
from operator import neg
from decimal import Decimal
from PyQt5.QtCore import (QObject, pyqtSignal)

from binance_api import BinanceApi
from binance_depth_websocket import BinanceDepthWebsocket
from custom_logging import get_logger


logger = get_logger(__name__)


class BinanceOrderBook(QObject):

    ob_updated = pyqtSignal(str)

    def __init__(self, api: BinanceApi, base: str, quote: str,
                 websocket: BinanceDepthWebsocket = None,
                 reinit_timeout: int = 600, parent=None):
        super(BinanceOrderBook, self).__init__(parent)

        self.__api = api
        self.__base = base.upper()
        self.__quote = quote.upper()
        self.__symbol = self.__base + self.__quote

        self.__websocket = websocket
        if self.__websocket:
            self.__websocket.add_symbol(self.__symbol)
            self.__websocket.symbol_updated.connect(self.update_orderbook)
            self.__websocket.disconnected.connect(self.__on_ws_disconnected)

        self.__lastUpdateId = 0
        self.__bids = {}
        self.__asks = {}
        self.__bids_prices = SortedSet(key=neg)
        self.__asks_prices = SortedSet()
        self.__bids_list_cached = []
        self.__asks_list_cached = []
        self.__bids_changed = False
        self.__asks_changed = False

        self.__initializing = False
        self.__valid = False

        self.__start_time = time.time()
        self.__timeout = reinit_timeout  # in seconds

    def is_valid(self) -> bool:
        return self.__valid

    def __on_ws_disconnected(self):
        self.__valid = False

    def get_base(self) -> str:
        return self.__base

    def get_quote(self) -> str:
        return self.__quote

    def get_symbol(self) -> str:
        return self.__symbol

    def get_bids(self) -> list:
        if self.__bids_changed:
            self.__bids_list_cached = [(price, self.__bids[price]) for price in self.__bids_prices]
            self.__bids_changed = False
        return self.__bids_list_cached.copy()

    def get_asks(self) -> list:
        if self.__asks_changed:
            self.__asks_list_cached = [(price, self.__asks[price]) for price in self.__asks_prices]
            self.__asks_changed = False
        return self.__asks_list_cached.copy()

    def set_websocket(self, ws: BinanceDepthWebsocket):
        self.remove_websocket()

        self.__websocket = ws
        self.__websocket.add_symbol(self.__symbol)
        self.__websocket.symbol_updated.connect(self.update_orderbook)

    def remove_websocket(self):
        if not self.__websocket:
            return

        self.__websocket.remove_symbol(self.__symbol)
        self.__websocket.symbol_updated.disconnect(self.update_orderbook)

    def get_websocket(self):
        return self.__websocket

    def save_to(self, filename):
        bids = self.__sorted_copy(self.__bids)
        asks = self.__sorted_copy(self.__asks)
        data = {'lastUpdateId': self.__lastUpdateId,
                'bids': bids,
                'asks': asks}
        with open(filename, 'w') as fp:
            json.dump(data, fp, indent=2, ensure_ascii=False)

    def init_order_book(self):
        if self.__initializing:
            return

        self.__initializing = True
        self.__api.depth(slot=self.init_ob_slot, symbol=self.__symbol, limit=100)

    def init_ob_slot(self):
        # TODO: think about this try-except later
        try:
            reply = self.sender()
            response = bytes(reply.readAll()).decode("utf-8")

            if not response:
                return

            snapshot = json.loads(response)
            if self.__parse_snapshot(snapshot):
                logger.info('OB {} > Initialized OB'.format(self.__symbol))
                self.__valid = True
                self.ob_updated.emit(self.__symbol)
                self.__start_time = time.time()
                self.__initializing = False
            else:
                logger.info('OB {} > OB initialization FAILED! Retrying...'.format(self.__symbol))
                self.__initializing = False
                self.init_order_book()
        except Exception as e:
            logger.exception('OB {} > INIT EXCEPTION: {}'.format(self.__symbol, str(e)))

    def update_orderbook(self, update: dict):
        if self.__initializing or (not update) or (update['s'] != self.__symbol):
            return

        from_id = update['U']
        to_id = update['u']

        # logger.debug('OB {} > Update: Time diff = {}'.format(self.__symbol, time.time() - self.__start_time))
        if time.time() - self.__start_time > self.__timeout:
            logger.debug('OB {} > Update: Snapshot is OUT OF DATE! ### {}'
                         .format(self.__symbol, self.__lastUpdateId))
            self.init_order_book()

        if from_id <= self.__lastUpdateId + 1 <= to_id:
            logger.debug('OB {} > Update: OK ### {} ### {} > {}'
                         .format(self.__symbol, self.__lastUpdateId, from_id, to_id))
            self.__lastUpdateId = to_id
            self.__update_bids(update['b'])
            self.__update_asks(update['a'])
            self.ob_updated.emit(self.__symbol)
        elif self.__lastUpdateId < from_id:
            logger.debug('OB {} > Update: Snapshot is too OLD ### {} ### {} > {}'
                         .format(self.__symbol, self.__lastUpdateId, from_id, to_id))
            self.init_order_book()
        else:
            logger.debug('OB {} > Update: Snapshot is too NEW ### {} ### {} > {}'
                         .format(self.__symbol, self.__lastUpdateId, from_id, to_id))

    def __parse_snapshot(self, snapshot):
        try:
            self.__lastUpdateId = snapshot['lastUpdateId']
            self.__update_bids(snapshot['bids'])
            self.__update_asks(snapshot['asks'])
        except LookupError:
            logger.debug('OB {} > Parse Snapshot ERROR'.format(self.__symbol))
            return False
        return True

    @staticmethod
    def __is_str_zero(value):
        try:
            value = float(value)
            if value == 0:
                return True
            else:
                return False
        except ValueError:
            return False

    def __update_bids(self, bids_list):
        for each in bids_list:
            price = Decimal(each[0])
            if self.__is_str_zero(each[1]):
                try:
                    del self.__bids[price]
                except KeyError:
                    pass
                self.__bids_prices.discard(price)
            else:
                self.__bids[price] = Decimal(each[1])
                if price not in self.__bids_prices:
                    self.__bids_prices.add(price)
        self.__bids_changed = True
        # print('__update_bids() took {:0.3f} µs'.format((time.time() - t)*1000000))

    def __update_asks(self, asks_list):
        for each in asks_list:
            price = Decimal(each[0])
            if self.__is_str_zero(each[1]):
                try:
                    del self.__asks[price]
                except KeyError:
                    pass
                self.__asks_prices.discard(price)
            else:
                self.__asks[price] = Decimal(each[1])
                if price not in self.__asks_prices:
                    self.__asks_prices.add(price)
        self.__asks_changed = True
        # print('__update_asks() took {:0.3f} µs'.format((time.time() - t)*1000000))

    def test_load_snapshot(self, filename):
        with open(filename, 'r') as fp:
            data = json.load(fp)
        self.__parse_snapshot(data)

    def test_load_update(self, filename):
        with open(filename, 'r') as fp:
            data = json.load(fp)
        self.update_orderbook(data)


class _SelfTestReceiver(QObject):

    @staticmethod
    def update_reciever(symbol: str):
        print('UR > Update signal is received! ### {}'.format(symbol))


def _main():
    import sys
    from PyQt5.QtCore import QCoreApplication, QThread
    from config import API_KEY, API_SECRET

    app = QCoreApplication(sys.argv)
    api = BinanceApi(API_KEY, API_SECRET)

    symbols_dict = api.get_symbols_info()
    symbols_list = [(v.get_base_asset(), v.get_quote_asset()) for k, v in symbols_dict.items()]
    threads = []
    websockets = []
    order_books = []
    i = 999

    for each in symbols_list:
        if i >= 50:
            th = QThread()
            threads.append(th)

            ws = BinanceDepthWebsocket(parent=th)
            ws.moveToThread(th)
            websockets.append(ws)

            i = 0

        ob = BinanceOrderBook(api, each[0], each[1], ws, parent=th)
        ob.moveToThread(th)
        order_books.append(ob)

        i += 1

    for each in threads:
        each.start()

    for each in websockets:
        each.connect()

    # ws = BinanceDepthWebsocket()
    # ob_ethbtc = BinanceOrderBook(api, 'eth', 'btc', ws)
    # ob_ltcbtc = BinanceOrderBook(api, 'ltc', 'btc', ws)
    # ob_xrpbtc = BinanceOrderBook(api, 'xrp', 'btc', ws)
    #
    # ws.connect()

    # ur = _TestUpdateReceiver()
    # ob2.ob_updated.connect(ur.update_reciever)

    sys.exit(app.exec_())


if __name__ == '__main__':
    _main()
