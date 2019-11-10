import asyncio
import aiohttp
import time
import hashlib
import hmac
import urllib.parse
from typing import List, Tuple
from aiohttp.client_exceptions import ClientError
from exchanges.base_api import BaseAPI
from logger import get_logger
logger = get_logger(__name__)


class PoloniexAPI(BaseAPI):
    def __init__(self, api_key: str, api_secret: str):
        super().__init__()
        self._base_url = 'https://poloniex.com/'
        self._request_interval = 0.17  # rate limit is 6 requests/sec
        self._api_key = api_key
        self._api_secret = api_secret
        loop = asyncio.get_event_loop()
        self._session = aiohttp.ClientSession(loop=loop, headers={})

    async def all_tickers(self, urgency: int = 0):
        return await self._safe_call(urgency, self._request_public, 'returnTicker')

    async def balances(self, urgency: int = 0):
        return await self._safe_call(urgency, self._request_private, 'returnBalances')

    async def buy(self, symbol: str, price: str, amount: str, urgency: int = 0):
        return await self._safe_call(urgency, self._request_private, 'buy', {
            'currencyPair': symbol,
            'rate': price,
            'amount': amount
        })

    async def sell(self, symbol: str, price: str, amount: str, urgency: int = 0):
        return await self._safe_call(urgency, self._request_private, 'sell', {
            'currencyPair': symbol,
            'rate': price,
            'amount': amount
        })

    async def order_status(self, order_id: str, urgency: int = 0):
        return await self._safe_call(urgency, self._request_private, 'returnOrderStatus', {'orderNumber': order_id})

    async def order_trades(self, order_id: str, urgency: int = 0):
        return await self._safe_call(urgency, self._request_private, 'returnOrderTrades', {'orderNumber': order_id})

    async def cancel_order(self, order_id: str, urgency: int = 0):
        return await self._safe_call(urgency, self._request_private, 'cancelOrder', {'orderNumber': order_id})

    async def measure_ping(self) -> Tuple[int, int, int]:
        pings = [
            await self._throttle(self._measure_ping_once)
            for i in range(10)
        ]
        avg = int(sum(pings) / len(pings))
        return min(pings), max(pings), avg

    async def stop(self):
        await super().stop()
        await self._session.close()

    async def _measure_ping_once(self) -> int:
        t = time.time()
        await self._request_private('returnBalances')
        return int((time.time() - t) * 1000)

    async def _request_public(self, command: str):
        data = {'command': command}
        url = f'public?{urllib.parse.urlencode(data)}'
        return await self._request('get', url)

    async def _request_private(self, command: str, data: dict = None) -> dict:
        if data is None:
            data = {}
        # preparing data
        data = {
            'command': command,
            'nonce': int(time.time() * 1000),
            **data
        }
        query = urllib.parse.urlencode(data)
        sig = hmac.new(self._api_secret.encode('utf8'), query.encode('utf8'), hashlib.sha512).hexdigest()
        kwargs = {
            'headers': {
                'Key': self._api_key,
                'Sign': sig
            },
            'data': data,
        }
        return await self._request('post', 'tradingApi', **kwargs)

    async def _request(self, verb: str, endpoint: str, **kwargs) -> dict:
        url = self._base_url + endpoint
        try:
            async with getattr(self._session, verb)(url, **kwargs) as response:
                result = await self._handle_response(response)
        except ClientError as e:
            raise PoloniexAPI.Error(str(e))
        return result

    async def _handle_response(self, response: aiohttp.ClientResponse) -> dict:
        if not str(response.status).startswith('2'):
            raise PoloniexAPI.Error(await response.text())
        try:
            r = await response.json()
        except ValueError:
            raise PoloniexAPI.Error(f'Invalid Response: {await response.text()}')
        # check if poloniex returned an error
        try:
            if 'result' in r:
                if 'error' in r['result']:
                    raise PoloniexAPI.Error(r['result']['error'])
                else:
                    return r['result']
            else:
                if 'error' in r:
                    raise PoloniexAPI.Error(r['error'])
                else:
                    return r
        except PoloniexAPI.Error as e:
            if 'Order not found' in e.message:
                raise PoloniexAPI.OrderNotFound
            if 'Not enough' in e.message:
                raise PoloniexAPI.ErrorNoRetry(e.message)
            if 'Total must be at least' in e.message:
                raise PoloniexAPI.ErrorNoRetry(e.message)
            raise


async def main():
    from config import config

    api = PoloniexAPI(
        config.get('Exchange', 'APIKey'),
        config.get('Exchange', 'APISecret')
    )
    logger.info(f'Tickers: {await api.all_tickers()}')
    logger.info(f'Balances: {await api.balances()}')
    # logger.info(f'Placing order: {await api.buy("BTC_ETH", "0.01000000", "0.1")}')
    # logger.info(f'Cancelling order: {await api.cancel_order("661906638940")}')
    # logger.info(f'Nonexistent order status: {await api.order_status("123456")}')
    # logger.info(f'Nonexistent order trades: {await api.order_trades("123456")}')
    await api.stop()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
