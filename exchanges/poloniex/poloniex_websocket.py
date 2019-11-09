import websocket
import threading
import ssl
import time
import json
from pydispatch import dispatcher
from logger import get_logger
logger = get_logger(__name__)


channels = {
    '1000': 'account',
    '1002': 'ticker',
    '1003': '24hvolume',
    '1010': 'heartbeat'
}


class PoloniexWebsocket:
    def __init__(self):
        self._ws = None
        self._thread = None
        self._symbols = set()
        self._stop_now = False

    def add_symbol(self, symbol: str):
        self._symbols.add(symbol)

    def start(self):
        self._thread = threading.Thread(target=self.run, name='PoloniexWebsocket')
        self._thread.setDaemon(True)
        self._thread.start()

    def run(self):
        logger.info('PoloniexWebsocket starting...')
        # websocket.enableTrace(True)   # will print detailed connection info
        while not self._stop_now:
            self._ws = websocket.WebSocketApp(
                'wss://api2.poloniex.com/',
                on_open=self._on_ws_open,
                on_message=self._on_ws_message,
                on_error=self._on_ws_error,
                on_close=self._on_ws_close
            )
            self._ws.run_forever(
                sslopt={"cert_reqs": ssl.CERT_NONE},
                ping_interval=10,
                ping_timeout=5
            )
            if not self._stop_now:
                time.sleep(1)
                logger.info('Restarting the websocket')
        logger.info('PoloniexWebsocket stopped')

    def stop(self):
        self._stop_now = True
        self._ws.close()
        self._thread.join()

    def _on_ws_open(self):
        for symbol in self._symbols:
            self._ws.send(json.dumps({'command': 'subscribe', 'channel': symbol}))
        logger.info(f'Websocket open and subscribed on: {", ".join(self._symbols)}')

    def _on_ws_message(self, message: str):
        # logger.info(f'Websocket message: {message}')
        message = json.loads(message)
        # catch errors
        if 'error' in message:
            return logger.info(f'Websocket error: {message["error"]}')
        chan = None
        try:
            chan = channels[str(message[0])]
        except KeyError:
            # try extracting currency pair if it's a symbol's channel for the first time
            try:
                if message[2][0][0] == 'i':
                    chan = message[2][0][1]['currencyPair']
                    channels[str(message[0])] = chan
            except (KeyError, IndexError, TypeError):
                pass
        except (IndexError, TypeError):
            return logger.warning(f'Bad message: {message}')
        if chan is None:
            return logger.warning(f'Unknown websocket channel: {message[0]}')
        # handle only symbol channels
        if chan in ['account', 'ticker', '24hvolume', 'heartbeat']:
            return
        symbol = chan
        # logger.info(f'{symbol}: {message[2]}')
        for update in message[2]:
            if update[0] == 'i':
                # logger.info(f'{symbol} init')
                try:
                    data = {
                        'asks': update[1]['orderBook'][0],
                        'bids': update[1]['orderBook'][1]
                    }
                except (KeyError, IndexError):
                    return logger.warning(f'Bad orderbook init on {symbol}: {update}')
                dispatcher.send(
                    signal=f'ws_init_{symbol}',
                    sender=self,
                    symbol=symbol,
                    data=data
                )
            if update[0] == 'o':
                # logger.info(f'{symbol} update')
                try:
                    data = {
                        'side': ['ask', 'bid'][update[1]],
                        'price': update[2],
                        'size': update[3]
                    }
                except IndexError:
                    return logger.warning(f'Bad orderbook update on {symbol}: {update}')
                dispatcher.send(
                    signal=f'ws_update_{symbol}',
                    sender=self,
                    symbol=symbol,
                    data=data
                )

    def _on_ws_error(self, error=None):
        logger.info(f'Websocket error: {error}')

    def _on_ws_close(self):
        logger.info(f'Websocket closed: {", ".join(self._symbols)}')
        dispatcher.send(signal='ws_closed', sender=self)


def test_on_ws_init(sender, symbol: str, data: dict):
    logger.info(f'Symbol: {symbol}. Init: {data}')


def test_on_ws_update(sender, symbol: str, data: dict):
    logger.info(f'Symbol: {symbol}. Update: {data}')


def test_on_ws_closed(sender):
    logger.info('Websocket disconnected!')


if __name__ == "__main__":
    pws = PoloniexWebsocket()
    pws.add_symbol('BTC_ETH')
    pws.add_symbol('USDT_ETH')
    pws.add_symbol('USDT_BTC')
    dispatcher.connect(test_on_ws_init, signal='ws_init_BTC_ETH', sender=dispatcher.Any)
    dispatcher.connect(test_on_ws_update, signal='ws_update_BTC_ETH', sender=dispatcher.Any)
    dispatcher.connect(test_on_ws_closed, signal='ws_closed', sender=dispatcher.Any)
    pws.start()
    time.sleep(10)
    pws.stop()
