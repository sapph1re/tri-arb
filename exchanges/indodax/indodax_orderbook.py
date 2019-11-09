import asyncio
from decimal import Decimal
from pydispatch import dispatcher
from exchanges.base_orderbook import BaseOrderbook
from .indodax_api import IndodaxAPI
from helpers import run_async_repeatedly
from logger import get_logger
logger = get_logger(__name__)


class IndodaxOrderbook(BaseOrderbook):
    def __init__(self, api: IndodaxAPI, symbol: str):
        super().__init__(symbol)
        self._api = api
        self._update_stop = run_async_repeatedly(
            self._update_orderbook,
            0,
            asyncio.get_event_loop(),
            thread_name=f'Orderbook {self._symbol}'
        )

    def estimate_market_buy_total(self, amount: Decimal) -> Decimal:
        total = Decimal(0)
        for price in self._bids_prices:
            if price in self._bids:
                if amount > self._bids[price]:
                    total += self._bids[price] * price
                    amount -= self._bids[price]
                else:
                    total += amount * price
                    break
        return total

    def stop(self):
        self._update_stop.set()

    async def _update_orderbook(self):
        try:
            ob = await self._api.depth(self._symbol)
        except IndodaxAPI.Error as e:
            self._valid = False
            logger.warning(f'Orderbook {self._symbol} failed to update: {e.message}')
        else:
            changed = self._update_bids(ob['buy']) or self._update_asks(ob['sell'])
            self._valid = True
            if changed:
                dispatcher.send(signal='orderbook_changed', sender=self, symbol=self._symbol)

    def _update_bids(self, bids: list or None) -> bool:
        not_mentioned = set(self._bids)
        if bids is None:
            bids = []
        for level in bids:
            price = Decimal(level[0])
            volume = Decimal(level[1])
            not_mentioned.discard(price)
            if price in self._bids and self._bids[price] == volume:
                continue
            self._bids[price] = volume
            if price not in self._bids_prices:
                self._bids_prices.add(price)
            self._bids_changed = True
        for price in not_mentioned:
            self._bids_prices.discard(price)
            self._bids.pop(price, None)
            self._bids_changed = True
        return self._bids_changed

    def _update_asks(self, asks: list or None) -> bool:
        not_mentioned = set(self._asks)
        if asks is None:
            asks = []
        for level in asks:
            price = Decimal(level[0])
            volume = Decimal(level[1])
            not_mentioned.discard(price)
            if price in self._asks and self._asks[price] == volume:
                continue
            self._asks[price] = volume
            if price not in self._asks_prices:
                self._asks_prices.add(price)
            self._asks_changed = True
        for price in not_mentioned:
            self._asks_prices.discard(price)
            self._asks.pop(price, None)
            self._asks_changed = True
        return self._asks_changed
