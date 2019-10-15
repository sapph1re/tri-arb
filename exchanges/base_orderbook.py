from sortedcontainers import SortedSet
from operator import neg
from logger import get_logger
logger = get_logger(__name__)


class BaseOrderbook:
    def __init__(self, symbol: str):
        self._symbol = symbol
        self._bids_list_cached = []
        self._asks_list_cached = []
        # implementation must maintain the variables below:
        self._bids = {}     #  {price: volume}
        self._asks = {}
        self._bids_prices = SortedSet(key=neg)  # sorted set of prices
        self._asks_prices = SortedSet()
        self._bids_changed = False      # set True when they have changed
        self._asks_changed = False
        self._valid = False     # set True when data is up to date, set False when it's probably not
        # implementation must also send pydispatch signal: orderbook_changed(symbol)

    def is_valid(self) -> bool:
        return self._valid

    def get_bids(self) -> list:
        if self._bids_changed:
            self._bids_list_cached = []
            for price in self._bids_prices:
                try:
                    self._bids_list_cached.append((price, self._bids[price]))
                except KeyError:
                    logger.warning(f'Price is missing in {self._symbol} bids: {price}')
            self._bids_changed = False
        return self._bids_list_cached.copy()

    def get_asks(self) -> list:
        if self._asks_changed:
            self._asks_list_cached = []
            for price in self._asks_prices:
                try:
                    self._asks_list_cached.append((price, self._asks[price]))
                except KeyError:
                    logger.warning(f'Price is missing in {self._symbol} asks: {price}')
            self._asks_changed = False
        return self._asks_list_cached.copy()
