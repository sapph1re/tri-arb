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
            bids = self._bids.copy()    # in case it changes while iterating
            self._bids_list_cached = [(price, bids[price]) for price in self._bids_prices.copy()]
            self._bids_changed = False
        return self._bids_list_cached.copy()

    def get_asks(self) -> list:
        if self._asks_changed:
            asks = self._asks.copy()    # in case it changes while iterating
            self._asks_list_cached = [(price, asks[price]) for price in self._asks_prices.copy()]
            self._asks_changed = False
        return self._asks_list_cached.copy()
