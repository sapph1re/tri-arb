import sys
import ssl
import websocket
import json
import gevent
import time
from decimal import *

from PyQt5.QtCore import (QCoreApplication, QThread,
                          QObject, pyqtSignal)

from binance_api import BinanceApi
from binance_depth_websocket import BinanceDepthWebsocket
from config import API_KEY, API_SECRET
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

        self.__lastUpdateId = 0
        self.__bids = {}
        self.__asks = {}
        self.__start_time = time.time()
        self.__timeout = reinit_timeout  # in seconds

    def get_base(self) -> str:
        return self.__base

    def get_quote(self) -> str:
        return self.__quote

    def get_symbol(self) -> str:
        return self.__symbol

    def get_bids(self) -> list:
        return self.__sorted_copy(self.__bids)[::-1]

    def get_asks(self) -> list:
        return self.__sorted_copy(self.__asks)

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
        snapshot = self.__api.depth(symbol=self.__symbol, limit=100)
        if self.__parse_snapshot(snapshot):
            logger.info('OB {} > Initialized OB'.format(self.__symbol))
            self.ob_updated.emit(self.__symbol)
            self.__start_time = time.time()
        else:
            logger.info('OB {} > OB initialization FAILED! Retrying...'.format(self.__symbol))
            gevent.sleep(1)
            self.init_order_book()
        del snapshot

    def update_orderbook(self, update: dict):
        if not update or update['s'] != self.__symbol:
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
            zero_flag = self.__is_str_zero(each[1])
            if zero_flag and (each[0] in self.__bids):
                self.__bids.pop(each[0])
            elif not zero_flag:
                self.__bids[each[0]] = each[1]

    def __update_asks(self, asks_list):
        for each in asks_list:
            zero_flag = self.__is_str_zero(each[1])
            if zero_flag and (each[0] in self.__asks):
                self.__asks.pop(each[0])
            elif not zero_flag:
                self.__asks[each[0]] = each[1]

    @staticmethod
    def __sorted_copy(dictionary: dict):
        dct = dictionary.copy()
        try:
            res = [(Decimal(key), Decimal(dct[key])) for key in sorted(dct.keys())]
        except ValueError:
            return None
        return res

    def test_load_snapshot(self, filename):
        with open(filename, 'r') as fp:
            data = json.load(fp)
        self.__parse_snapshot(data)

    def test_load_update(self, filename):
        with open(filename, 'r') as fp:
            data = json.load(fp)
        self.update_orderbook(data)


class _TestUpdateReceiver(QObject):

    @staticmethod
    def update_reciever(symbol: str):
        print('UR > Update signal is received! ### {}'.format(symbol))


if __name__ == '__main__':
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

            ws = BinanceDepthWebsocket()
            ws.moveToThread(th)
            websockets.append(ws)

            i = 0

        ob = BinanceOrderBook(api, each[0], each[1], ws)
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
