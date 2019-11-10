from decimal import Decimal
from typing import Dict, Tuple
from .base_orderbook import BaseOrderbook


class BaseExchange:

    class OrderResult:
        def __init__(self, symbol: str, order_id: str, side: str, price: Decimal or None,
                     amount_original: Decimal, amount_executed: Decimal, amount_quote: Decimal or None,
                     status: str, placed_at: int = 0, done_at: int = 0):
            self.symbol = symbol
            self.order_id = order_id
            self.side = side    # BUY, SELL
            self.price = price
            self.amount_original = amount_original
            self.amount_executed = amount_executed
            self.amount_quote = amount_quote    # quote amount executed
            self.status = status    # NEW, PARTIALLY_FILLED, FILLED, CANCELLED, OTHER
            self.placed_at = placed_at  # timestamp in milliseconds
            self.done_at = done_at  # timestamp in milliseconds

    class Error(BaseException):
        def __init__(self, message: str = ''):
            self.message = message

    class OrderNotFound(Error):
        pass

    def __init__(self):
        pass

    @classmethod
    async def create(cls, api_key: str, api_secret: str):
        return cls()

    async def load_balances(self) -> Dict[str, Decimal]:
        return {}

    def get_symbols_info(self) -> Dict[str, dict]:
        # must return a dict, where key is a symbol and value is a dict containing:
        #   base_asset
        #   quote_asset
        #   min_amount
        #   max_amount
        #   amount_step
        #   min_total
        return {}

    def make_symbol(self, base: str, quote: str) -> str:
        return ''

    def run_orderbooks(self, symbols: Dict[str, dict]) -> Dict[str, BaseOrderbook]:
        return {}

    async def create_order(self, symbol: str, side: str, order_type: str,
                           amount: Decimal, price: Decimal or None = None) -> OrderResult:
        # must return BaseExchange.OrderResult or raise BaseExchange.Error
        raise self.Error('Not implemented')

    async def get_order_result(self, symbol: str, order_id: str) -> OrderResult:
        # must return BaseExchange.OrderResult or raise BaseExchange.Error
        raise self.Error('Not implemented')

    async def cancel_order(self, symbol: str, order_id: str) -> OrderResult:
        # must return BaseExchange.OrderResult or raise BaseExchange.Error
        raise self.Error('Not implemented')

    async def measure_ping(self) -> Tuple[int, int, int]:
        # must return min, max, avg ping in milliseconds or raise BaseExchange.Error
        raise self.Error('Not implemented')

    async def stop(self):
        # graceful stop, if needed
        return
