from decimal import Decimal
from typing import Dict
from .base_orderbook import BaseOrderbook


class BaseExchange:

    class OrderResult:
        def __init__(self, symbol: str, order_id: str, side: str, price: Decimal or None,
                     amount_original: Decimal, amount_executed: Decimal, status: str):
            self.symbol = symbol
            self.order_id = order_id
            self.side = side    # BUY, SELL
            self.price = price
            self.amount_original = amount_original
            self.amount_executed = amount_executed
            self.status = status    # NEW, PARTIALLY_FILLED, FILLED, CANCELLED, OTHER

    class Error(BaseException):
        def __init__(self, message):
            self.message = message

    def __init__(self):
        pass

    @classmethod
    async def create(cls, api_key: str, api_secret: str):
        return cls()

    async def load_balances(self) -> Dict[str, Decimal]:
        return {}

    def get_symbols_info(self) -> Dict[str, dict]:
        return {}

    def run_orderbooks(self, symbols: Dict[str, dict]) -> Dict[str, BaseOrderbook]:
        return {}

    async def create_order(self, symbol: str, side: str, type: str,
                           amount: Decimal, price: Decimal or None = None) -> OrderResult:
        # must return BaseExchange.OrderResult or raise BaseExchange.Error
        raise self.Error('Not implemented')

    async def get_order_result(self, symbol: str, order_id: str) -> OrderResult:
        # must return BaseExchange.OrderResult or raise BaseExchange.Error
        raise self.Error('Not implemented')

    async def cancel_order(self, symbol: str, order_id: str) -> OrderResult:
        # must return BaseExchange.OrderResult or raise BaseExchange.Error
        raise self.Error('Not implemented')
