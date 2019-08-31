import time
import asyncio
from sortedcontainers import SortedSet
from operator import neg
from decimal import Decimal
from pydispatch import dispatcher
from binance_api import BinanceApi
from binance_websocket import BinanceWebsocket
from logger import get_logger
logger = get_logger(__name__)


class BinanceOrderbook:
    def __init__(self, api: BinanceApi, base: str, quote: str, websocket: BinanceWebsocket):
        self.__api = api
        self.__base = base.upper()
        self.__quote = quote.upper()
        self.__symbol = self.__base + self.__quote

        self.__websocket = websocket
        self.__websocket.add_symbol(self.__symbol)
        dispatcher.connect(
            self.on_ws_depth,
            signal=f'ws_depth_{self.__symbol.lower()}',
            sender=self.__websocket
        )
        dispatcher.connect(
            self.__on_ws_closed,
            signal='ws_closed',
            sender=self.__websocket
        )

        self.__last_update_id = 0
        self.__bids = {}
        self.__asks = {}
        self.__bids_prices = SortedSet(key=neg)
        self.__asks_prices = SortedSet()
        self.__bids_list_cached = []
        self.__asks_list_cached = []
        self.__bids_changed = False
        self.__asks_changed = False
        self.__valid = False

    def is_valid(self) -> bool:
        return self.__valid

    def __on_ws_closed(self):
        self.__valid = False

    def get_update_id(self) -> int:
        return self.__last_update_id

    def get_base(self) -> str:
        return self.__base

    def get_quote(self) -> str:
        return self.__quote

    def get_symbol(self) -> str:
        return self.__symbol

    def get_bids(self) -> list:
        if self.__bids_changed:
            self.__bids_list_cached = [(price, self.__bids[price]) for price in self.__bids_prices]
            self.__bids_changed = False
        return self.__bids_list_cached.copy()

    def get_asks(self) -> list:
        if self.__asks_changed:
            self.__asks_list_cached = [(price, self.__asks[price]) for price in self.__asks_prices]
            self.__asks_changed = False
        return self.__asks_list_cached.copy()

    def on_ws_depth(self, sender, symbol: dict, data: dict):
        if symbol != self.__symbol:
            return
        ob_changed = self.update_orderbook(data)
        self.__valid = True
        if ob_changed:
            dispatcher.send(signal='orderbook_changed', sender=self, symbol=self.__symbol)

    def update_orderbook(self, snapshot):
        changed = False
        try:
            bids_changed = self.__update_bids(snapshot['bids'])
            asks_changed = self.__update_asks(snapshot['asks'])
            changed = bids_changed or asks_changed
            self.__last_update_id = snapshot['lastUpdateId']
        except LookupError:
            logger.warning(f'Orderbook {self.__symbol} failed to update. Snapshot: {snapshot}')
        return changed

    def __update_bids(self, bids_list) -> bool:
        changed = False
        not_mentioned = set(self.__bids)
        for each in bids_list:
            price = Decimal(each[0])
            amount = Decimal(each[1])
            not_mentioned.discard(price)
            if price in self.__bids and self.__bids[price] == amount:
                continue
            self.__bids[price] = amount
            if price not in self.__bids_prices:
                self.__bids_prices.add(price)
            changed = True
        for price in not_mentioned:
            self.__bids.pop(price, None)
            self.__bids_prices.discard(price)
            changed = True
        self.__bids_changed = self.__bids_changed or changed
        return changed

    def __update_asks(self, asks_list):
        changed = False
        not_mentioned = set(self.__asks)
        for each in asks_list:
            price = Decimal(each[0])
            amount = Decimal(each[1])
            not_mentioned.discard(price)
            if price in self.__asks and self.__asks[price] == amount:
                continue
            self.__asks[price] = amount
            if price not in self.__asks_prices:
                self.__asks_prices.add(price)
            changed = True
        for price in not_mentioned:
            self.__asks.pop(price, None)
            self.__asks_prices.discard(price)
            changed = True
        self.__asks_changed = self.__asks_changed or changed
        return changed


def test_on_orderbook_changed(sender: BinanceOrderbook, symbol: str):
    logger.info(f'Orderbook changed: {symbol}, last update ID: {sender.get_update_id()}')


async def main():
    from config import API_KEY, API_SECRET

    api = await BinanceApi.create(API_KEY, API_SECRET)
    symbols_dict = await api.get_symbols_info()
    symbols_list = [(v.get_base_asset(), v.get_quote_asset()) for k, v in symbols_dict.items()]

    websockets = []
    orderbooks = []
    i = 999
    ws = None
    for base, quote in symbols_list[:5]:
        if i >= 2:
            ws = BinanceWebsocket()
            websockets.append(ws)
            i = 0
        ob = BinanceOrderbook(api, base, quote, ws)
        orderbooks.append(ob)
        i += 1

    dispatcher.connect(test_on_orderbook_changed, signal='orderbook_changed', sender=dispatcher.Any)

    for ws in websockets:
        ws.start()

    time.sleep(5)

    for ws in websockets:
        ws.stop()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
