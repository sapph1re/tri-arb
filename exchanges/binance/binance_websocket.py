import websocket
import threading
import ssl
import time
import json
from pydispatch import dispatcher
from logger import get_logger
logger = get_logger(__name__)


class BinanceWebsocket:
    def __init__(self):
        self._ws = None
        self._thread = None
        self._symbols = set()
        self._stop_now = False

    def add_symbol(self, symbol: str):
        self._symbols.add(symbol)

    def start(self):
        self._thread = threading.Thread(target=self.run, name='BinanceWebsocket')
        self._thread.setDaemon(True)
        self._thread.start()

    def on_ws_message(self, message: str):
        # logger.info(f'Websocket message: {message}')
        message_parsed = json.loads(message)
        symbol, stream = message_parsed['stream'].split('@', 1)
        if stream == 'depth20@100ms':
            dispatcher.send(
                signal=f'ws_depth_{symbol}',
                sender=self,
                symbol=symbol.upper(),
                data=message_parsed['data']
            )

    def on_ws_error(self, error=None):
        logger.info(f'Websocket error: {error}')

    def on_ws_close(self):
        logger.info(f'Websocket closed: {", ".join(self._symbols)}')
        dispatcher.send(signal='ws_closed', sender=self)

    def on_ws_open(self):
        logger.info(f'Websocket open: {", ".join(self._symbols)}')

    def run(self):
        logger.info('BinanceWebsocket starting...')
        # websocket.enableTrace(True)   # will print detailed connection info
        streams = '/'.join([f'{symbol.lower()}@depth20@100ms' for symbol in self._symbols])
        wss_url = f'wss://stream.binance.com:9443/stream?streams={streams}'
        while not self._stop_now:
            self._ws = websocket.WebSocketApp(
                wss_url,
                on_message=self.on_ws_message,
                on_error=self.on_ws_error,
                on_close=self.on_ws_close
            )
            self._ws.on_open = self.on_ws_open
            self._ws.run_forever(sslopt={"cert_reqs": ssl.CERT_NONE})
            logger.info('Restarting the websocket')
        logger.info('BinanceWebsocket stopped')

    def stop(self):
        self._stop_now = True
        self._ws.close()
        self._thread.join()


def test_on_ws_depth(sender, symbol: str, data: dict):
    logger.info(f'Symbol: {symbol}. Data: {data}')


def test_on_websocket_disconnected(sender):
    logger.info('Websocket disconnected!')


if __name__ == "__main__":
    bws = BinanceWebsocket()
    bws.add_symbol('ETHBTC')
    bws.add_symbol('XRPBTC')
    bws.add_symbol('BTCUSDT')
    dispatcher.connect(test_on_ws_depth, signal='ws_depth_ethbtc', sender=dispatcher.Any)
    dispatcher.connect(test_on_websocket_disconnected, signal='ws_closed', sender=dispatcher.Any)
    bws.start()
    time.sleep(10)
    bws.stop()
