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
from config import API_KEY, API_SECRET
from custom_logging import get_logger

logger = get_logger(__name__)


class BinanceOrderBook(QObject):

    def __init__(self, api: BinanceApi, base: str, quote: str,
                 start_websocket: bool = False, reinit_timeout: int = 600):
        super().__init__()

        self.__api = api
        self.__base = base.upper()
        self.__quote = quote.upper()
        self.__symbol = self.__base + self.__quote

        self.__lastUpdateId = 0
        self.__bids = {}
        self.__asks = {}
        self.__start_time = time.time()
        self.__timeout = reinit_timeout  # in seconds

        self.__websocket = None
        if start_websocket:
            self.start_websocket()

    ob_updated = pyqtSignal(str)

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

    def get_websocket(self):
        return self.__websocket

    def start_websocket(self):
        if not self.__websocket:
            self.__websocket = BinanceDepthWebsocket(self)
        self.__websocket.start()

    def stop_websocket(self):
        self.__websocket.stop()

    def save_to(self, filename):
        bids = self.__sorted_copy(self.__bids)
        asks = self.__sorted_copy(self.__asks)
        data = {'lastUpdateId': self.__lastUpdateId,
                'bids': bids,
                'asks': asks}
        with open(filename, 'w') as fp:
            json.dump(data, fp, indent=2, ensure_ascii=False)

    def init_order_book(self):
        snapshot = self.__api.depth(symbol=self.__symbol, limit=1000)
        if self.__parse_snapshot(snapshot):
            logger.info('OB > Initialized OB')
            self.ob_updated.emit(self.__symbol)
            self.__start_time = time.time()
        else:
            logger.info('OB > OB initialization FAILED! Retrying...')
            gevent.sleep(1)
            self.init_order_book()

    def update_orderbook(self, update: dict):
        from_id = update['U']
        to_id = update['u']
        logger.debug('OB > Update: Time diff = {}'.format(time.time() - self.__start_time))
        if time.time() - self.__start_time > self.__timeout:
            logger.debug('OB > Update: Snapshot is OUT OF DATE! ### Current Id: {}'.format(self.__lastUpdateId))
            self.init_order_book()
        if from_id <= self.__lastUpdateId + 1 <= to_id:
            logger.debug('OB > Update: OK ### Current Id: {} ### {} > {}'.format(self.__lastUpdateId, from_id, to_id))
            self.__lastUpdateId = to_id
            self.__update_bids(update['b'])
            self.__update_asks(update['a'])
            self.ob_updated.emit(self.__symbol)
        elif self.__lastUpdateId < from_id:
            logger.debug('OB > Update: Snapshot is too OLD ### Current Id: {} ### {} > {}'
                         .format(self.__lastUpdateId, from_id, to_id))
            self.init_order_book()
            return False
        else:
            logger.debug('OB > Update: Snapshot is too NEW ### Current Id: {} ### {} > {}'
                         .format(self.__lastUpdateId, from_id, to_id))

    def __parse_snapshot(self, snapshot):
        try:
            self.__lastUpdateId = snapshot['lastUpdateId']
            self.__update_bids(snapshot['bids'])
            self.__update_asks(snapshot['asks'])
        except LookupError:
            logger.debug('OB > Parse Snapshot ERROR')
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


class BinanceDepthWebsocket(QThread):

    def __init__(self, order_book: BinanceOrderBook):
        super(BinanceDepthWebsocket, self).__init__()

        self.__order_book = order_book
        self.__symbol = order_book.get_symbol()

        wss_url = 'wss://stream.binance.com:9443/ws/' + self.__symbol.lower() + '@depth'

        self.__ws = websocket.WebSocketApp(wss_url,
                                           on_message=self.__on_message,
                                           on_error=self.__on_error,
                                           on_close=self.__on_close,
                                           on_open=self.__on_open)

    def start(self, **kwargs):
        super(BinanceDepthWebsocket, self).start(**kwargs)

    def stop(self):
        self.__ws.close(status=1001, timeout=5)  # status=1001: STATUS_GOING_AWAY
        if self.__ws.sock:
            self.__ws.sock = None
        self.quit()
        self.wait()

    def run(self):
        while True:
            try:
                self.__ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE}, ping_interval=5, ping_timeout=3)
            except websocket.WebSocketException:
                logger.error('WS > WebSocketException ### Interrupted?')
                break

    def __on_message(self, ws, message):
        json_data = json.loads(message)
        self.__order_book.update_orderbook(json_data)

    def __on_error(self, ws, error):
        logger.error("WS > Error: {}".format(str(error)))
        self.sleep(1)

    def __on_close(self, ws):
        logger.info("WS > Closed")

    def __on_open(self, ws):
        logger.info("WS > Opened")


class _TestUpdateReceiver(QObject):

    def update_reciever(self, symbol: str):
        print('UR > Update signal is received! ### {}'.format(symbol))


if __name__ == '__main__':
    bapi = BinanceApi(API_KEY, API_SECRET)
    base, quote = 'ltc', 'eth'
    ob = BinanceOrderBook(api=bapi, base=base, quote=quote,
                          start_websocket=True,  # optional, default: False
                          reinit_timeout=300  # optional, default: 600
                          )
    print('<> Websocket STARTED')
    gevent.sleep(5)
    print('<> 5 SEC PASSED')
    ob.stop_websocket()
    print('<> Websocket STOPPED')
    gevent.sleep(2)
    print('<> 2 SEC PASSED')
    ob.start_websocket()
    print('<> Websocket STARTED AGAIN')

    ur = _TestUpdateReceiver()
    ob.ob_updated.connect(ur.update_reciever)

    app = QCoreApplication(sys.argv)
    sys.exit(app.exec_())
