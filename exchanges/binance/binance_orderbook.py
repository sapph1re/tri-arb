from decimal import Decimal
from pydispatch import dispatcher
from exchanges.base_orderbook import BaseOrderbook
from exchanges.binance.binance_websocket import BinanceWebsocket
from logger import get_logger
logger = get_logger(__name__)


class BinanceOrderbook(BaseOrderbook):
    def __init__(self, symbol: str, websocket: BinanceWebsocket):
        super().__init__(symbol)
        self._websocket = websocket
        self._websocket.add_symbol(self._symbol)
        dispatcher.connect(
            self._on_ws_depth,
            signal=f'ws_depth_{self._symbol.lower()}',
            sender=self._websocket
        )
        dispatcher.connect(
            self._on_ws_closed,
            signal='ws_closed',
            sender=self._websocket
        )

    def stop(self):
        dispatcher.disconnect(
            self._on_ws_depth,
            signal=f'ws_depth_{self._symbol.lower()}',
            sender=self._websocket
        )
        dispatcher.disconnect(
            self._on_ws_closed,
            signal='ws_closed',
            sender=self._websocket
        )

    def _on_ws_closed(self):
        self._valid = False

    def _on_ws_depth(self, sender, symbol: dict, data: dict):
        if symbol != self._symbol:
            return
        ob_changed = self._update_orderbook(data)
        self._valid = True
        if ob_changed:
            dispatcher.send(signal='orderbook_changed', sender=self, symbol=self._symbol)

    def _update_orderbook(self, snapshot):
        changed = False
        try:
            bids_changed = self._update_bids(snapshot['bids'])
            asks_changed = self._update_asks(snapshot['asks'])
            changed = bids_changed or asks_changed
        except LookupError:
            logger.warning(f'Orderbook {self._symbol} failed to update. Snapshot: {snapshot}')
        return changed

    def _update_bids(self, bids_list) -> bool:
        not_mentioned = set(self._bids)
        for each in bids_list:
            price = Decimal(each[0])
            amount = Decimal(each[1])
            not_mentioned.discard(price)
            if price in self._bids and self._bids[price] == amount:
                continue
            self._bids[price] = amount
            if price not in self._bids_prices:
                self._bids_prices.add(price)
            self._bids_changed = True
        for price in not_mentioned:
            self._bids_prices.discard(price)
            self._bids.pop(price, None)
            self._bids_changed = True
        return self._bids_changed

    def _update_asks(self, asks_list):
        not_mentioned = set(self._asks)
        for each in asks_list:
            price = Decimal(each[0])
            amount = Decimal(each[1])
            not_mentioned.discard(price)
            if price in self._asks and self._asks[price] == amount:
                continue
            self._asks[price] = amount
            if price not in self._asks_prices:
                self._asks_prices.add(price)
            self._asks_changed = True
        for price in not_mentioned:
            self._asks_prices.discard(price)
            self._asks.pop(price, None)
            self._asks_changed = True
        return self._asks_changed
