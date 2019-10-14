import asyncio
from decimal import Decimal
from exchanges.base_exchange import BaseExchange
from helpers import run_async_repeatedly
from logger import get_logger

logger = get_logger(__name__)


class AccountInfo:
    def __init__(self, exchange: BaseExchange, auto_update_interval: int = 60):
        self._exchange = exchange
        self._balances = {}
        self._update_stop = run_async_repeatedly(
            self.update_info,
            auto_update_interval,
            asyncio.get_event_loop(),
            thread_name='Account Info'
        )

    @classmethod
    async def create(cls, *args, **kwargs):
        self = cls(*args, **kwargs)
        await self.update_info()
        return self

    def get_balance(self, asset: str) -> Decimal:
        try:
            return self._balances[asset]
        except KeyError:
            return Decimal('0')

    async def update_info(self):
        try:
            self._balances = await self._exchange.load_balances()
        except BaseExchange.Error as e:
            logger.error(f'Failed to load account info: {e.message}')

    def stop(self):
        self._update_stop.set()
