from decimal import Decimal
from pydispatch import dispatcher
from exchanges.base_orderbook import BaseOrderbook
from .poloniex_websocket import PoloniexWebsocket
from logger import get_logger
logger = get_logger(__name__)


class PoloniexOrderbook(BaseOrderbook):
    def __init__(self, symbol: str, websocket: PoloniexWebsocket):
        super().__init__(symbol)
        self._websocket = websocket
        self._websocket.add_symbol(self._symbol)
        dispatcher.connect(
            self._on_ws_init,
            signal=f'ws_init_{self._symbol}',
            sender=self._websocket
        )
        dispatcher.connect(
            self._on_ws_update,
            signal=f'ws_update_{self._symbol}',
            sender=self._websocket
        )
        dispatcher.connect(
            self._on_ws_closed,
            signal='ws_closed',
            sender=self._websocket
        )

    def stop(self):
        dispatcher.disconnect(
            self._on_ws_init,
            signal=f'ws_init_{self._symbol}',
            sender=self._websocket
        )
        dispatcher.disconnect(
            self._on_ws_update,
            signal=f'ws_update_{self._symbol}',
            sender=self._websocket
        )
        dispatcher.disconnect(
            self._on_ws_closed,
            signal='ws_closed',
            sender=self._websocket
        )

    def _on_ws_closed(self):
        self._valid = False

    def _on_ws_init(self, sender, symbol: dict, data: dict):
        if symbol != self._symbol:
            return
        self._init_orderbook(data)
        self._valid = True
        dispatcher.send(signal='orderbook_changed', sender=self, symbol=self._symbol)

    def _on_ws_update(self, sender, symbol: dict, data: dict):
        if symbol != self._symbol:
            return
        if data['side'] == 'bid':
            self._update_bid(data)
        if data['side'] == 'ask':
            self._update_ask(data)
        self._valid = True
        dispatcher.send(signal='orderbook_changed', sender=self, symbol=self._symbol)

    def _init_orderbook(self, snapshot):
        self._bids.clear()
        self._asks.clear()
        self._bids_prices.clear()
        self._asks_prices.clear()
        try:
            for bid_price, bid_vol in snapshot['bids'].items():
                bid_price = Decimal(bid_price)
                bid_vol = Decimal(bid_vol)
                self._bids[bid_price] = bid_vol
                self._bids_prices.add(bid_price)
            self._bids_changed = True
            for ask_price, ask_vol in snapshot['asks'].items():
                ask_price = Decimal(ask_price)
                ask_vol = Decimal(ask_vol)
                self._asks[ask_price] = ask_vol
                self._asks_prices.add(ask_price)
            self._asks_changed = True
        except (LookupError, TypeError) as e:
            logger.warning(f'Orderbook {self._symbol} failed to update. Error: {e}. Snapshot: {snapshot}')
        return

    def _update_bid(self, update: dict):
        price = Decimal(update['price'])
        vol = Decimal(update['size'])
        if vol == 0:
            self._bids.pop(price, None)
            self._bids_prices.discard(price)
        else:
            self._bids[price] = vol
            self._bids_prices.add(price)
        self._bids_changed = True

    def _update_ask(self, update: dict):
        price = Decimal(update['price'])
        vol = Decimal(update['size'])
        if vol == 0:
            self._asks.pop(price, None)
            self._asks_prices.discard(price)
        else:
            self._asks[price] = vol
            self._asks_prices.add(price)
        self._asks_changed = True
