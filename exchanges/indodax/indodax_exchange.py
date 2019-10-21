from decimal import Decimal, ROUND_UP
from typing import Dict
from exchanges.base_exchange import BaseExchange
from exchanges.base_orderbook import BaseOrderbook
from .indodax_api import IndodaxAPI
from .indodax_orderbook import IndodaxOrderbook
from logger import get_logger
logger = get_logger(__name__)


class IndodaxExchange(BaseExchange):
    def __init__(self, api: IndodaxAPI):
        super().__init__()
        self._api = api
        self._symbols_info = {}
        self._orderbooks = {}

    @classmethod
    async def create(cls, api_key: str, api_secret: str):
        api = IndodaxAPI(api_key, api_secret)
        self = cls(api)
        await self._load_symbols_info()
        return self

    async def load_balances(self) -> Dict[str, Decimal]:
        try:
            r = await self._api.account_info()
        except IndodaxAPI.Error as e:
            raise self.Error(f'Failed to load balances: {e.message}')
        balances = {
            asset.upper(): Decimal(balance)
            for asset, balance in r['balance'].items()
        }
        # update symbols info as well, as rates change an min total requirements changes as well
        await self._load_symbols_info()
        return balances

    def get_symbols_info(self) -> Dict[str, str]:
        return self._symbols_info

    def make_symbol(self, base: str, quote: str) -> str:
        return f'{base}_{quote}'

    def run_orderbooks(self, symbols: Dict[str, dict]) -> Dict[str, BaseOrderbook]:
        if not self._orderbooks:
            self._orderbooks = {
                symbol: IndodaxOrderbook(self._api, symbol)
                for symbol in symbols
            }
        return self._orderbooks

    async def create_order(self, symbol: str, side: str, order_type: str,
                           amount: Decimal, price: Decimal or None = None) -> BaseExchange.OrderResult:
        if symbol not in self._orderbooks:
            raise self.Error(f'Unknown symbol: {symbol}')
        try:
            if order_type == 'LIMIT':
                if side == 'BUY':
                    amount *= price
                r = await self._api.create_order(symbol.lower(), side.lower(), str(price), str(amount))
            elif order_type == 'MARKET':
                if side == 'BUY':
                    price = self._orderbooks[symbol].get_best_ask() * 2
                    amount = self._orderbooks[symbol].estimate_market_buy_total(amount)
                else:
                    price = self._orderbooks[symbol].get_best_bid() / 2
                r = await self._api.create_order(symbol.lower(), side.lower(), str(price), str(amount))
            else:
                raise self.Error(f'Unsupported order type: {order_type}')
        except (IndodaxAPI.Error, IndodaxOrderbook.Error) as e:
            raise self.Error(f'Create order failed: {e.message}')
        return await self.get_order_result(symbol, r['order_id'])

    async def get_order_result(self, symbol: str, order_id: str) -> BaseExchange.OrderResult:
        try:
            r = await self._api.order_info(symbol, int(order_id))
        except IndodaxAPI.Error as e:
            raise self.Error(f'Order info failed: {e.message}')
        return self._parse_order_result(symbol, r)

    async def cancel_order(self, symbol: str, order_id: str) -> BaseExchange.OrderResult:
        try:
            order_id = int(order_id)
            r = await self._api.order_info(symbol, order_id)
            r = await self._api.cancel_order(symbol, order_id, r['order']['type'])
            r = await self._api.order_info(symbol, order_id)
        except IndodaxAPI.Error as e:
            raise self.Error(f'Cancel order failed: {e.message}')
        return self._parse_order_result(symbol, r)

    def _parse_order_result(self, symbol: str, result: dict) -> BaseExchange.OrderResult:
        side = result['order']['type'].upper()
        # figure out amounts
        base, quote = symbol.lower().split('_')
        cur = quote if side == 'BUY' else base
        if cur == 'idr':
            cur = 'rp'
        amt_orig = Decimal(result['order'][f'order_{cur}'])
        amt_left = Decimal(result['order'][f'remain_{cur}'])
        price = Decimal(result['order']['price'])
        # convert amounts if it's a BUY
        if side == 'BUY':
            amt_orig /= price
            amt_left /= price
        # figure out order status
        if result['order']['status'] == 'open':
            if amt_left > 0:
                status = 'PARTIALLY_FILLED'
            else:
                status = 'NEW'
        elif result['order']['status'] == 'filled':
            status = 'FILLED'
        elif result['order']['status'] == 'cancelled':
            status = 'CANCELLED'
        else:
            status = 'OTHER'
        return BaseExchange.OrderResult(
            symbol=symbol,
            order_id=result['order']['order_id'],
            side=side,
            price=price,
            amount_original=amt_orig,
            amount_executed=(amt_orig - amt_left),
            status=status
        )

    async def _load_symbols_info(self):
        r = await self._api.tickers()
        symbols = {
            symbol.upper(): Decimal(details['last'])
            for symbol, details in r['tickers'].items()
        }
        for symbol, last_price in symbols.items():
            base, quote = symbol.split('_')
            # minimal order size is equivalent of 50k IDR on all pairs
            min_idr = Decimal('50000')
            amount_step = Decimal('0.00000001')
            try:
                if quote == 'IDR':
                    min_total = min_idr
                else:
                    quote_symbol = f'{quote}_IDR'
                    last_price_quote = symbols[quote_symbol]
                    min_total = (min_idr / last_price_quote).quantize(amount_step, rounding=ROUND_UP)
                min_amount = (min_total / last_price).quantize(amount_step, rounding=ROUND_UP)
            except IndodaxOrderbook.Error as e:
                # if orderbook is empty, we just skip it
                logger.info(f'Failed to load {symbol} info: {e.message}')
                continue
            max_amount = Decimal('Inf')
            self._symbols_info[symbol.upper()] = {
                'base_asset': base.upper(),
                'quote_asset': quote.upper(),
                'min_amount': min_amount,
                'max_amount': max_amount,
                'amount_step': amount_step,
                'min_total': min_total
            }
