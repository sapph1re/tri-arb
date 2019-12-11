from decimal import Decimal, DivisionByZero, ROUND_UP
from typing import Dict, Tuple
from helpers import LRUDict
from exchanges.base_exchange import BaseExchange
from exchanges.base_orderbook import BaseOrderbook
from exchanges.base_api import BaseAPI
from .poloniex_api import PoloniexAPI
from .poloniex_orderbook import PoloniexOrderbook
from .poloniex_websocket import PoloniexWebsocket
from logger import get_logger
logger = get_logger(__name__)


class PoloniexExchange(BaseExchange):
    def __init__(self, api: PoloniexAPI):
        super().__init__()
        self._api = api
        self._symbols_info = {}
        self._websockets = []
        # we store orders info {order_id: {side, price, amount}} because Poloniex API is limited on this
        # max size of this storage is limited to keep memory usage reasonable
        # at its max size, least-recently-used entry is removed when a new one is added
        self._orders = LRUDict(maxsize=128)

    @classmethod
    async def create(cls, api_key: str, api_secret: str):
        api = PoloniexAPI(api_key, api_secret)
        self = cls(api)
        await self._load_symbols_info()
        return self

    async def load_balances(self) -> Dict[str, Decimal]:
        try:
            r = await self._api.balances()
        except PoloniexAPI.Error as e:
            raise PoloniexExchange.Error(e.message)
        balances = {asset: Decimal(balance) for asset, balance in r.items()}
        # update symbols info as well, as rates change and thus min amounts change as well
        await self._load_symbols_info()
        return balances

    def get_symbols_info(self):
        return self._symbols_info

    def make_symbol(self, base: str, quote: str) -> str:
        return f'{quote}_{base}'

    def run_orderbooks(self, symbols: Dict[str, dict]) -> Dict[str, BaseOrderbook]:
        if not self._orderbooks:
            i = 999
            ws = None
            for symbol, details in symbols.items():
                # starting a websocket per every 20 symbols
                if i >= 20:
                    ws = PoloniexWebsocket()
                    self._websockets.append(ws)
                    i = 0
                # starting an orderbook watcher for every symbol
                ob = PoloniexOrderbook(symbol=symbol, websocket=ws)
                self._orderbooks[symbol] = ob
                i += 1
            for ws in self._websockets:
                ws.start()
        return self._orderbooks

    async def create_order(self, symbol: str, side: str, order_type: str, amount: Decimal,
                           price: Decimal or None = None) -> BaseExchange.OrderResult:
        if side not in ['BUY', 'SELL']:
            raise PoloniexExchange.Error('Bad side')
        if order_type not in ['LIMIT', 'MARKET']:
            raise PoloniexExchange.Error('Bad order type')
        try:
            # prepare
            if order_type == 'MARKET':
                if side == 'BUY':
                    price = self._orderbooks[symbol].get_best_ask() * 2
                else:
                    price = self._orderbooks[symbol].get_best_bid() / 2
            # place the order
            if side == 'BUY':
                r = await self._api.buy(symbol, str(price), str(amount), urgency=1)
            else:
                r = await self._api.sell(symbol, str(price), str(amount), urgency=1)
        except (PoloniexAPI.Error, PoloniexOrderbook.Error) as e:
            raise PoloniexExchange.Error(f'Create order failed: {e.message}')
        else:
            # read the result
            try:
                order_id = r['orderNumber']
                self._orders[order_id] = {'side': side, 'price': price, 'amount': amount}
                amount_executed = Decimal(0)
                amount_quote = Decimal(0)
                for trade in r['resultingTrades']:
                    amount_executed += Decimal(trade['amount'])
                    amount_quote += Decimal(trade['total'])
                if amount_executed == 0:
                    status = 'NEW'
                elif amount_executed == amount:
                    status = 'FILLED'
                else:
                    status = 'PARTIALLY_FILLED'
                return BaseExchange.OrderResult(
                    symbol=symbol,
                    order_id=order_id,
                    side=side,
                    price=price,
                    amount_original=amount,
                    amount_executed=amount_executed,
                    amount_quote=amount_quote,
                    status=status
                )
            except (KeyError, TypeError):
                raise PoloniexExchange.Error(f'Bad create order response: {r}')

    async def get_order_result(self, symbol: str, order_id: str) -> BaseExchange.OrderResult:
        try:
            r = await self._api.order_status(order_id, urgency=1)
        except BaseAPI.OrderNotFound:
            # order is filled or cancelled
            try:
                r = await self._api.order_trades(order_id, urgency=1)
            except BaseAPI.OrderNotFound:
                # order cancelled unfilled
                try:
                    return BaseExchange.OrderResult(
                        symbol=symbol,
                        order_id=order_id,
                        side=self._orders[order_id]['side'],
                        price=self._orders[order_id]['price'],
                        amount_original=self._orders[order_id]['amount'],
                        amount_executed=Decimal(0),
                        amount_quote=Decimal(0),
                        status='CANCELLED'
                    )
                except KeyError:
                    raise PoloniexExchange.Error(f'Failed to get cancelled order details, '
                                                 f'not found in cache: {order_id}')
            except PoloniexAPI.Error as e:
                raise PoloniexExchange.Error(f'Failed to get order trades: {e.message}')
            else:
                # order is filled or was partially filled and then cancelled
                try:
                    amount_original = self._orders[order_id]['amount']
                    amount_executed = Decimal(0)
                    amount_quote = Decimal(0)
                    for trade in r:
                        amount_executed += Decimal(trade['amount'])
                        amount_quote += Decimal(trade['total'])
                    status = 'CANCELLED' if amount_executed < amount_original else 'FILLED'
                    return BaseExchange.OrderResult(
                        symbol=symbol,
                        order_id=order_id,
                        side=self._orders[order_id]['side'],
                        price=self._orders[order_id]['price'],
                        amount_original=amount_original,
                        amount_executed=amount_executed,
                        amount_quote=amount_quote,
                        status=status
                    )
                except (KeyError, TypeError):
                    raise PoloniexExchange.Error(f'Failed to get order trades, bad response: {r}')
        except PoloniexAPI.Error as e:
            raise PoloniexExchange.Error(f'Failed to get order status: {e.message}')
        else:
            # order is new or partially filled
            try:
                result = r[next(iter(r))]   # gets the value of the first entry in the dict
                side = result['type'].upper()
                if side not in ['BUY', 'SELL']:
                    raise PoloniexExchange.Error(f'Failed to get order status, bad side: {side}')
                amount_original = Decimal(result['startingAmount'])
                amount_left = Decimal(result['amount'])
                amount_executed = amount_original - amount_left
                amount_quote = Decimal(0)
                if amount_executed > 0:
                    status = 'PARTIALLY_FILLED'
                    # check trades
                    try:
                        rt = await self._api.order_trades(order_id, urgency=1)
                    except PoloniexAPI.Error as e:
                        raise PoloniexExchange.Error(f'Failed to get partially filled order trades: {e.message}')
                    else:
                        try:
                            for trade in rt:
                                amount_quote += Decimal(trade['total'])
                        except (KeyError, TypeError):
                            raise PoloniexExchange.Error(f'Failed to get order trades, bad response: {rt}')
                else:
                    status = 'NEW'
                return BaseExchange.OrderResult(
                    symbol=result['currencyPair'],
                    order_id=order_id,
                    side=side,
                    price=Decimal(result['rate']),
                    amount_original=amount_original,
                    amount_executed=amount_executed,
                    amount_quote=amount_quote,
                    status=status
                )
            except (KeyError, TypeError, StopIteration):
                raise PoloniexExchange.Error(f'Failed to get order status, bad response: {r}')

    async def cancel_order(self, symbol: str, order_id: str) -> BaseExchange.OrderResult:
        try:
            r = await self._api.cancel_order(order_id, urgency=1)
        except PoloniexAPI.OrderNotFound:
            raise PoloniexExchange.OrderNotFound
        except PoloniexAPI.Error as e:
            raise PoloniexExchange.Error(f'Failed to cancel order: {e.message}')
        else:
            return await self.get_order_result(symbol, order_id)

    async def measure_ping(self) -> Tuple[int, int, int]:
        try:
            return await self._api.measure_ping()
        except PoloniexAPI.Error as e:
            raise self.Error(f'Failed to measure ping: {e.message}')

    async def stop(self):
        for ws in self._websockets:
            ws.stop()
        await self._api.stop()

    async def _load_symbols_info(self):
        try:
            tickers = await self._api.all_tickers()
        except PoloniexAPI.Error as e:
            logger.warning(f'Failed to load symbols info: {e.message}')
            return
        last_prices = {
            symbol: Decimal(details['last'])
            for symbol, details in tickers.items()
        }
        for symbol, last_price in last_prices.items():
            quote, base = symbol.split('_')  # note: they're backwards on Poloniex
            if quote == 'BTC':
                min_total = Decimal('0.0001')
            elif quote == 'USDC':
                min_total = Decimal('1.0')
            elif quote == 'USDT':
                min_total = Decimal('1.0')
            elif quote == 'ETH':
                min_total = Decimal('0.0001')
            else:
                logger.error(f'Unsupported quote asset: {quote}')
                return
            amount_step = Decimal('0.00000001')
            try:
                min_amount = (min_total / last_price).quantize(amount_step, rounding=ROUND_UP)
            except DivisionByZero:
                # if last price is empty, skip it
                # logger.info(f'Failed to load {symbol} info, last price is empty')
                continue
            max_amount = Decimal('Inf')
            self._symbols_info[symbol] = {
                'base_asset': base,
                'quote_asset': quote,
                'min_amount': min_amount,
                'max_amount': max_amount,
                'amount_step': amount_step,
                'min_total': min_total
            }
