import sys
import json

from PyQt5.QtCore import (QCoreApplication, QUrl, QObject, pyqtSignal)
from PyQt5.QtWebSockets import QWebSocket

from custom_logging import get_logger

logger = get_logger(__name__)


class BinanceDepthWebsocket(QObject):

    symbol_updated = pyqtSignal(dict)

    def __init__(self, parent=None):
        super(BinanceDepthWebsocket, self).__init__(parent)
        self.__symbols = set()
        self.__wss_url = ''
        self.__ws_client = QWebSocket()

        self.__ws_client.textMessageReceived.connect(self.__on_message)
        self.__ws_client.error.connect(self.__on_error)
        self.__ws_client.sslErrors.connect(self.__on_error)
        self.__ws_client.connected.connect(self.__on_open)
        self.__ws_client.disconnected.connect(self.__on_close)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.__ws_client.close()
        super(BinanceDepthWebsocket, self).__exit__()

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

    def connect(self):
        if not self.__wss_url:
            return

        try:
            wss_qurl = QUrl(self.__wss_url)
            self.__ws_client.open(wss_qurl)
        except Exception as e:
            print('WS > Connect exception: {}'.format(e))
            self.connect()

    def close(self):
        self.__ws_client.close(reason='WS > Manually closed!')

    def __on_message(self, message):
        # logger.debug('WS > Message RECEIVED ### {}', message)
        json_data = json.loads(message)

        try:
            data = json_data['data']
        except KeyError:
            data = {}
        self.symbol_updated.emit(data)
        #
        # del data
        # del json_data

    def __on_error(self, error):
        logger.error("WS > Error: {}".format(str(error)))
        self.connect()

    def __on_close(self):
        logger.info("WS > Closed")
        self.connect()

    def __on_open(self):
        logger.info("WS > Opened")


if __name__ == '__main__':
    app = QCoreApplication(sys.argv)

    # ws = BinanceDepthWebsocket(app)
    # ws.add_symbol('ETHBTC')
    # ws.connect()

    from binance_api import BinanceApi
    from config import API_KEY, API_SECRET

    bapi = BinanceApi(API_KEY, API_SECRET)

    symbols_dict = bapi.get_symbols_info()
    symbols_list = [(v.get_base_asset(), v.get_quote_asset()) for k, v in symbols_dict.items()]
    threads = []
    websockets = []
    order_books = []
    i = 999

    for each in symbols_list:
        if i >= 50:
            ws = BinanceDepthWebsocket()
            websockets.append(ws)
            i = 0
        ws.add_symbol(each[0]+each[1])

    for each in websockets:
        each.connect()

    # import threading
    # import gc
    #
    # def collect_garbage():
    #     threading.Timer(60, collect_garbage).start()
    #     print('GC > Garbage collection has been started!')
    #     gc.collect()
    #
    # gc.disable()
    # collect_garbage()

    sys.exit(app.exec_())
