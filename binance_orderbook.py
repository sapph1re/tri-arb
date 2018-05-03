import sys
import ssl
import websocket
import json
import threading
import gevent
import time
from decimal import *

from PyQt5.QtCore import QCoreApplication, QObject, pyqtSignal

from binance_api import BinanceApi
from config import API_KEY, API_SECRET


def is_str_zero(value):
    try:
        value = float(value)
        if value == 0:
            return True
        else:
            return False
    except ValueError:
        return False


class BinanceOrderBook(QObject):

    def __init__(self, api: BinanceApi, symbol: str):
        super().__init__()
        self.__api = api
        self.__symbol = symbol.upper()
        self.__lastUpdateId = 0
        self.__bids = {}
        self.__asks = {}
        self.__start_time = time.time()
        self.__timeout = 600  # in seconds

    ob_updated = pyqtSignal(str, list, list)

    def get_symbol(self) -> str:
        return self.__symbol

    def get_bids(self) -> list:
        return self.__sorted_copy(self.__bids)[::-1]

    def get_asks(self) -> list:
        return self.__sorted_copy(self.__asks)

    def init_order_book(self):
        snapshot = self.__api.depth(symbol=self.__symbol, limit='1000')
        if self.__parse_snapshot(snapshot):
            print('OB > Initialized OB')
            self.__start_time = time.time()
        else:
            print('OB > OB initialization FAILED! Retrying...')
            gevent.sleep(1)
            self.init_order_book()

    def __parse_snapshot(self, snapshot):
        try:
            self.__lastUpdateId = snapshot['lastUpdateId']
            self.__update_bids(snapshot['bids'])
            self.__update_asks(snapshot['asks'])
        except LookupError:
            print('OB > Parse Snapshot ERROR')
            return False
        return True

    def __update_bids(self, bids_list):
        for each in bids_list:
            zero_flag = is_str_zero(each[1])
            if zero_flag and (each[0] in self.__bids):
                self.__bids.pop(each[0])
            elif not zero_flag:
                self.__bids[each[0]] = each[1]

    def __update_asks(self, asks_list):
        for each in asks_list:
            zero_flag = is_str_zero(each[1])
            if zero_flag and (each[0] in self.__asks):
                self.__asks.pop(each[0])
            elif not zero_flag:
                self.__asks[each[0]] = each[1]

    def update_orderbook(self, update: dict):
        from_id = update['U']
        to_id = update['u']
        print('OB > Update: Time diff = {}'.format(time.time() - self.__start_time))
        if time.time() - self.__start_time > self.__timeout:
            print('OB > Update: Snapshot is OUT OF DATE! ### Current Id: {}'.format(self.__lastUpdateId))
            self.init_order_book()
        if from_id <= self.__lastUpdateId + 1 <= to_id:
            print('OB > Update: OK ### Current Id: {} ### {} > {}'.format(self.__lastUpdateId, from_id, to_id))
            self.__lastUpdateId = to_id
            self.__update_bids(update['b'])
            self.__update_asks(update['a'])
            self.ob_updated.emit(self.__symbol,
                                 self.get_bids(),
                                 self.get_asks())
        elif self.__lastUpdateId < from_id:
            print('OB > Update: Snapshot is too OLD ### Current Id: {} ### {} > {}'
                  .format(self.__lastUpdateId, from_id, to_id))
            self.init_order_book()
            return False
        else:
            print('OB > Update: Snapshot is too NEW ### Current Id: {} ### {} > {}'
                  .format(self.__lastUpdateId, from_id, to_id))

    def __sorted_copy(self, dictionary: dict):
        dct = dictionary.copy()
        try:
            res = [(Decimal(key), Decimal(dct[key])) for key in sorted(dct.keys())]
        except ValueError:
            return None
        return res

    def save_to(self, filename):
        bids = self.__sorted_copy(self.__bids)
        asks = self.__sorted_copy(self.__asks)
        data = {'lastUpdateId': self.__lastUpdateId,
                'bids': bids,
                'asks': asks}
        with open(filename, 'w') as fp:
            json.dump(data, fp, indent=2, ensure_ascii=False)

    def test_load_snapshot(self, filename):
        with open(filename, 'r') as fp:
            data = json.load(fp)
        #print(data)
        self.__parse_snapshot(data)

    def test_load_update(self, filename):
        with open(filename, 'r') as fp:
            data = json.load(fp)
        #print(data)
        self.update_orderbook(data)


class BinanceDepthWebsocket:

    def __init__(self, order_book: BinanceOrderBook):
        # websocket.enableTrace(True)
        self.__order_book = order_book
        self.__symbol = order_book.get_symbol()

        wss_url = 'wss://stream.binance.com:9443/ws/' + self.__symbol.lower() + '@depth'
        self.__ws = websocket.WebSocketApp(wss_url,
                                           on_message=self.__on_message,
                                           on_error=self.__on_error,
                                           on_close=self.__on_close,
                                           on_open=self.__on_open)
        self.__wst = threading.Thread(target=self.__runner)
        self.__wst.daemon = True
        self.run()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__ws.keep_running = False
        self.__wst.join()
        self.__ws.close()

    def stop(self):
        self.__ws.keep_running = False
        self.__wst.join()

    def run(self):
        self.__ws.keep_running = True
        self.__wst.start()

    def __runner(self):
        self.__ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})

    def __on_message(self, ws, message):
        #print(message)
        json_data = json.loads(message)
        self.__order_book.update_orderbook(json_data)

    def __on_error(self, ws, error):
        print("WS > Error: " + error)

    def __on_close(self, ws):
        print("WS > Closed")

    def __on_open(self, ws):
        print("WS > Opened")


class TestUpdateReceiver(QObject):

    def update_reciever(self, symbol: str, bids: list, asks: list):
        print('UR > Update signal is received! ### {} <> {} <> {}'.format(symbol, bids[0], asks[0]))


if __name__ == '__main__':
    bapi = BinanceApi(API_KEY, API_SECRET)
    symbol = 'ltceth'
    ob = BinanceOrderBook(bapi, symbol)
    ws = BinanceDepthWebsocket(ob)
    ur = TestUpdateReceiver()
    ob.ob_updated.connect(ur.update_reciever)

    app = QCoreApplication(sys.argv)
    sys.exit(app.exec_())
