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


class BinanceDepthWebsocket(QThread):

    symbol_updated = pyqtSignal(dict)

    def __init__(self):
        super(BinanceDepthWebsocket, self).__init__()
        self.__symbols = set()
        self.__wss_url = ''
        self.__ws = None

    def __del__(self):
        self.stop()

    def add_symbol(self, symbol: str):
        self.__symbols.add(symbol)
        self.__wss_url = 'wss://stream.binance.com:9443/stream?streams='
        for each in self.__symbols:
            wss_name = each.lower() + '@depth/'
            self.__wss_url += wss_name

    def add_symbols_list(self, symbols_list: list):
        for each in symbols_list:
            self.add_symbol(each)

    def remove_symbol(self, symbol: str):
        self.__symbols.remove(symbol)
        self.__wss_url = 'wss://stream.binance.com:9443/stream?streams='
        for each in self.__symbols:
            wss_name = each.lower() + '@depth/'
            self.__wss_url += wss_name

    def remove_symbols_list(self, symbols_list: list):
        for each in symbols_list:
            self.remove_symbol(each)

    def start(self, **kwargs):
        if self.__ws:
            self.stop()

        if not self.__wss_url:
            return

        self.__ws = websocket.WebSocketApp(self.__wss_url,
                                           on_message=self.__on_message,
                                           on_error=self.__on_error,
                                           on_close=self.__on_close,
                                           on_open=self.__on_open)

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
                if self.__ws:
                    self.__ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE}, ping_interval=5, ping_timeout=3)
                else:
                    logger.error('WS > No symbols added to start websocket!')
                    self.sleep(1)
            except websocket.WebSocketException:
                logger.error('WS > WebSocketException ### Interrupted?')
                break

    def __on_message(self, ws, message):
        json_data = json.loads(message)
        logger.debug('WS > Message RECEIVED ### {}', json.dumps(json_data))
        try:
            data = json_data['data']
        except KeyError:
            data = {}
        self.symbol_updated.emit(data)

    def __on_error(self, ws, error):
        logger.error("WS > Error: {}".format(str(error)))
        self.sleep(1)

    def __on_close(self, ws):
        logger.info("WS > Closed")

    def __on_open(self, ws):
        logger.info("WS > Opened")


class BinanceOrderBook(QObject):

    ob_updated = pyqtSignal(str)

    def __init__(self, api: BinanceApi, base: str, quote: str,
                 websocket: BinanceDepthWebsocket = None, reinit_timeout: int = 600):
        super().__init__()

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
        snapshot = self.__api.depth(symbol=self.__symbol, limit=1000)
        if self.__parse_snapshot(snapshot):
            logger.info('OB {} > Initialized OB'.format(self.__symbol))
            self.ob_updated.emit(self.__symbol)
            self.__start_time = time.time()
        else:
            logger.info('OB {} > OB initialization FAILED! Retrying...'.format(self.__symbol))
            gevent.sleep(1)
            self.init_order_book()

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
            return False
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
    bapi = BinanceApi(API_KEY, API_SECRET)

    ws1 = BinanceDepthWebsocket()
    ob1 = BinanceOrderBook(api=bapi, base='ltc', quote='eth',
                           websocket=ws1,  # optional, default: None
                           reinit_timeout=300  # optional, default: 600
                           )

    print('<> 1-st Websocket STARTED')
    ws1.start()
    gevent.sleep(5)

    print('<> 5 SEC PASSED')
    ws1.stop()
    print('<> 1-st Websocket STOPPED')

    ws2 = BinanceDepthWebsocket()

    ob1.set_websocket(ws2)

    ob2 = BinanceOrderBook(api=bapi, base='bnb', quote='btc',
                           websocket=ws2, reinit_timeout=300)

    ob3 = BinanceOrderBook(bapi, 'eos', 'btc')
    ob3.set_websocket(ws2)

    print('<> 2-nd Websocket STARTED')
    ws2.start()

    # ur = _TestUpdateReceiver()
    # ob2.ob_updated.connect(ur.update_reciever)

    app = QCoreApplication(sys.argv)
    sys.exit(app.exec_())
