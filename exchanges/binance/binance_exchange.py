from decimal import Decimal
from typing import Dict, Tuple
from exchanges.base_exchange import BaseExchange
from exchanges.base_orderbook import BaseOrderbook
from .binance_api import BinanceAPI
from .binance_websocket import BinanceWebsocket
from .binance_orderbook import BinanceOrderbook


class BinanceExchange(BaseExchange):
    def __init__(self, api: BinanceAPI):
        super().__init__()
        self._api = api
        self._symbols_info = {}
        self._websockets = []

    @classmethod
    async def create(cls, api_key: str, api_secret: str):
        api = await BinanceAPI.create(api_key, api_secret)
        self = cls(api)
        await self._load_symbols_info()
        return self

    async def load_balances(self) -> Dict[str, Decimal]:
        balances = {}
        try:
            info = await self._api.account()
        except BinanceAPI.Error as e:
            raise self.Error(e.message)
        try:
            for each in info['balances']:
                asset = each['asset']
                balances[asset] = Decimal(each['free'])
        except KeyError:
            self.Error(f'Bad data format! Data: {info}')
        except (ValueError, TypeError):
            self.Error(f'Could not parse balance for asset: {asset}')
        return balances

    def get_symbols_info(self):
        return self._symbols_info

    def make_symbol(self, base: str, quote: str) -> str:
        return base + quote

    def run_orderbooks(self, symbols: Dict[str, dict]) -> Dict[str, BaseOrderbook]:
        orderbooks = {}
        i = 999
        ws = None
        for symbol, details in symbols.items():
            # starting a websocket per every 50 symbols
            if i >= 50:
                ws = BinanceWebsocket()
                self._websockets.append(ws)
                i = 0
            # starting an orderbook watcher for every symbol
            ob = BinanceOrderbook(symbol=symbol, websocket=ws)
            orderbooks[symbol] = ob
            i += 1
        for ws in self._websockets:
            ws.start()
        return orderbooks

    async def create_order(self, symbol: str, side: str, order_type: str, amount: Decimal,
                           price: Decimal or None = None) -> BaseExchange.OrderResult:
        if side not in ['BUY', 'SELL']:
            raise self.Error('Bad side')
        if order_type not in ['LIMIT', 'MARKET']:
            raise self.Error('Bad order type')
        try:
            r = await self._api.create_order(
                symbol=symbol,
                side=side,
                order_type=order_type,
                quantity=f'{amount:f}',
                price=None if price is None else f'{price:f}',
            )
            return self._parse_order_result(r)
        except BinanceAPI.Error as e:
            raise self.Error(e.message)

    async def get_order_result(self, symbol: str, order_id: str) -> BaseExchange.OrderResult:
        try:
            r = await self._api.order_info(symbol, order_id)
            return self._parse_order_result(r)
        except BinanceAPI.Error as e:
            raise self.Error(e.message)

    async def cancel_order(self, symbol: str, order_id: str) -> BaseExchange.OrderResult:
        try:
            r = await self._api.cancel_order(symbol, order_id)
            return self._parse_order_result(r)
        except BinanceAPI.Error as e:
            raise self.Error(e.message)

    async def measure_ping(self) -> Tuple[int, int, int]:
        try:
            return await self._api.measure_ping()
        except BinanceAPI.Error as e:
            raise self.Error(f'Failed to measure ping: {e.message}')

    async def stop(self):
        for ws in self._websockets:
            ws.stop()
        await self._api.stop()

    def _parse_order_result(self, result: dict) -> BaseExchange.OrderResult:
        status = result['status']
        if status == 'CANCELED':
            status = 'CANCELLED'
        if status not in ['NEW', 'PARTIALLY_FILLED', 'FILLED', 'CANCELLED']:
            status = 'OTHER'
        amt_quote = Decimal(result['cummulativeQuoteQty'])
        if amt_quote < 0:
            amt_quote = None
        return BaseExchange.OrderResult(
            symbol=result['symbol'],
            order_id=result['orderId'],
            side=result['side'],
            price=Decimal(result['price']),
            amount_original=Decimal(result['origQty']),
            amount_executed=Decimal(result['executedQty']),
            amount_quote=amt_quote,
            status=status
        )

    async def _load_symbols_info(self):
        symbols = await self._api.get_symbols_info()
        for symbol in symbols:
            symbol_name = symbol['symbol']
            info = {
                'symbol': symbol_name,
                'status': symbol['status'],
                'base_asset': symbol['baseAsset'],
                'quote_asset': symbol['quoteAsset'],
                'min_price': None,
                'max_price': None,
                'price_step': None,
                'min_amount': None,
                'max_amount': None,
                'amount_step': None,
                'min_total': None,
            }
            for f in symbol['filters']:
                if f['filterType'] == 'PRICE_FILTER':
                    info['min_price'] = Decimal(f['minPrice']).normalize()
                    info['max_price'] = Decimal(f['maxPrice']).normalize()
                    info['price_step'] = Decimal(f['tickSize']).normalize()
                elif f['filterType'] == 'LOT_SIZE':
                    info['min_amount'] = Decimal(f['minQty']).normalize()
                    info['max_amount'] = Decimal(f['maxQty']).normalize()
                    info['amount_step'] = Decimal(f['stepSize']).normalize()
                elif f['filterType'] == 'MIN_NOTIONAL':
                    info['min_total'] = Decimal(f['minNotional']).normalize()
            self._symbols_info[symbol_name] = info
