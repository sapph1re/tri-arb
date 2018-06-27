import sys
import json

from PyQt5.QtCore import (QCoreApplication, QUrl, QTimer,
                          QObject, Qt, pyqtSignal)
from PyQt5.QtNetwork import QAbstractSocket, QSslError  # don't delete here anything if IDE says something about unused
from PyQt5.QtWebSockets import QWebSocket

from custom_logging import get_logger

logger = get_logger(__name__)


class BinanceDepthWebsocket(QObject):

    symbol_updated = pyqtSignal(dict)
    connected = pyqtSignal()
    disconnected = pyqtSignal()

    def __init__(self, ping_timeout=5000, thread=None, parent=None):
        super(BinanceDepthWebsocket, self).__init__(parent=parent)

        if thread:
            self.moveToThread(thread)

        self.__symbols = set()
        self.__wss_url = ''

        self.__ws_client = QWebSocket()
        self.__ws_client.textMessageReceived.connect(self.__on_message)
        self.__ws_client.error.connect(self.__on_error)
        self.__ws_client.sslErrors.connect(self.__on_sslerrors)
        self.__ws_client.connected.connect(self.__on_open)
        self.__ws_client.disconnected.connect(self.__on_close)
        self.__ws_client.stateChanged.connect(self.__on_state_changed, Qt.DirectConnection)
        self.__ws_client.pong.connect(self.__on_pong)

        self.__ping_timer = QTimer()
        self.__ping_timeout = ping_timeout

        self.__ws_states = {}
        for key in dir(QAbstractSocket):
            value = getattr(QAbstractSocket, key)
            if isinstance(value, QAbstractSocket.SocketState):
                self.__ws_states[key] = value
                self.__ws_states[value] = key

        self.__ws_errors = {}
        for key in dir(QAbstractSocket):
            value = getattr(QAbstractSocket, key)
            if isinstance(value, QAbstractSocket.SocketError):
                self.__ws_errors[key] = value
                self.__ws_errors[value] = key

        self.__ws_sslerrors = {}
        for key in dir(QSslError):
            value = getattr(QSslError, key)
            if isinstance(value, QSslError.SslError):
                self.__ws_sslerrors[key] = value
                self.__ws_sslerrors[value] = key

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
            # logger.debug('WS > Connecting...')
            self.__ws_client.open(wss_qurl)
        except Exception as e:
            logger.exception('WS > Connect exception: {}'.format(e))
            self.connect()

    def start_ping(self):
        logger.debug('WS > PING')
        self.__ws_client.ping()
        self.__ping_timer.singleShot(self.__ping_timeout, self.start_ping)

    def stop_ping(self):
        self.__ping_timer.stop()

    def close(self):
        self.__ws_client.close(reason='WS > Manually closed!')

    def __on_state_changed(self, state):
        """
        :param state:
            0 - QAbstractSocket::UnconnectedState   - The socket is not connected.
            1 - QAbstractSocket::HostLookupState    - The socket is performing a host name lookup.
            2 - QAbstractSocket::ConnectingState    - The socket has started establishing a connection.
            3 - QAbstractSocket::ConnectedState     - A connection is established.
            4 - QAbstractSocket::BoundState         - The socket is bound to an address and port.
            6 - QAbstractSocket::ClosingState       - The socket is about to close (data may still be waiting to be written).
            5 - QAbstractSocket::ListeningState     - For internal use only.
        :return:
        """
        logger.debug('WS > State changed to "{}"'.format(self.__ws_states[state]))
        if state == QAbstractSocket.UnconnectedState:  # Unconnected state == 0
            self.stop_ping()
            self.connect()
            self.disconnected.emit()
        elif state == QAbstractSocket.ConnectedState:  # Connected state
            self.start_ping()
            self.connected.emit()

    def __on_message(self, message):
        # logger.debug('WS > Message RECEIVED ### {}', message)
        try:
            json_data = json.loads(message)
            data = json_data['data']
        except json.JSONDecodeError:
            logger.error('WS > JSON Decode FAILED: {}', message)
            data = {'error': 'Response is not JSON: {}'.format(message)}
        except KeyError:
            logger.error('WS > JSON Structure WRONG, no "data" field: {}', json_data)
            data = {'error': 'Response structure WRONG, no "data" field: {}'.format(json_data)}
        self.symbol_updated.emit(data)

    @staticmethod
    def __on_pong(elapsed_time, payload):
        logger.debug("WS > PONG: {} ms ### {}".format(elapsed_time, payload))
        pass

    def __on_error(self, error):
        logger.error("WS > Error: {}".format(self.__ws_errors[error]))

    def __on_sslerrors(self, errors):
        try:
            for error in errors:
                logger.error("WS > SSL Error: {}".format(self.__ws_sslerrors[error]))
        except Exception as e:
            logger.exception("WS > __on_sslerrors EXCEPTION: {}".format(str(e)))

    def __on_close(self):
        # logger.info("WS > Closed")
        pass

    def __on_open(self):
        # logger.info("WS > Opened")
        pass


if __name__ == '__main__':
    app = QCoreApplication(sys.argv)

    # ws = BinanceDepthWebsocket(parent=app)
    # ws.add_symbol('ETHBTC')
    # ws.connect()

    # ws = BinanceDepthWebsocket()
    # ws.add_symbol('XZCBNB')
    # ws.connect()

    from PyQt5.QtCore import QThread
    from binance_api import BinanceApi
    from config import API_KEY, API_SECRET

    bapi = BinanceApi(API_KEY, API_SECRET)

    symbols_dict = bapi.get_symbols_info()
    symbols_list = [(v.get_base_asset(), v.get_quote_asset()) for k, v in symbols_dict.items()]

    threads = []
    websockets = []
    i = 999

    ws = None
    for each in symbols_list:
        if i >= 50:
            th = QThread()
            threads.append(th)

            ws = BinanceDepthWebsocket(thread=th)
            websockets.append(ws)
            i = 0

        ws.add_symbol(each[0] + each[1])
        i += 1

    for each in threads:
        each.start()

    for each in websockets:
        each.connect()

    sys.exit(app.exec_())
