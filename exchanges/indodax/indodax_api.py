import asyncio
import aiohttp
import time
import hashlib
import hmac
import urllib.parse
from typing import Tuple
from aiohttp.client_exceptions import ClientError
from exchanges.base_api import BaseAPI
from logger import get_logger
logger = get_logger(__name__)


class IndodaxAPI(BaseAPI):

    def __init__(self, api_key: str, api_secret: str):
        super().__init__()
        # self._base_url = 'https://indodax.com/'   # regular API url
        self._base_url = 'https://btcapi.net/'  # no-rate-limit API url
        # self._request_interval = 0.333  # rate limit is 180 requests/min
        self._request_interval = 0.01  # they said no rate limit for me, but let's stay at 100 requests/sec
        self._api_key = api_key
        self._api_secret = api_secret
        loop = asyncio.get_event_loop()
        self._session = aiohttp.ClientSession(loop=loop, headers={})

    async def depth(self, symbol: str, urgency: int = 0) -> dict:
        return await self._safe_call(urgency, self._request_public, 'get', f'api/{symbol.lower()}/depth')

    async def tickers(self, urgency: int = 0) -> dict:
        return await self._safe_call(urgency, self._request_public, 'get', 'api/tickers')

    async def account_info(self, urgency: int = 0) -> dict:
        return await self._safe_call(urgency, self._request_private, 'getInfo')

    async def create_order(self, symbol: str, side: str, price: str, amount: str, urgency: int = 0) -> dict:
        """amount is the amount to spend, i.e. when buying the amount is in quote currency"""
        base, quote = symbol.lower().split('_')
        spendable = quote if side == 'buy' else base
        params = {
            'pair': symbol,
            'type': side,
            'price': price,
            spendable: amount
        }
        return await self._safe_call(urgency, self._request_private, 'trade', params)

    async def order_info(self, symbol: str, order_id: int, urgency: int = 0) -> dict:
        params = {
            'pair': symbol.lower(),
            'order_id': order_id
        }
        return await self._safe_call(urgency, self._request_private, 'getOrder', params)

    async def cancel_order(self, symbol: str, order_id: int, side: str, urgency: int = 0) -> dict:
        params = {
            'pair': symbol.lower(),
            'order_id': order_id,
            'type': side
        }
        return await self._safe_call(urgency, self._request_private, 'cancelOrder', params)

    async def open_orders(self, symbol: str = None, urgency: int = 0) -> dict:
        params = {'pair': symbol.lower()} if symbol is not None else {}
        return await self._safe_call(urgency, self._request_private, 'openOrders', params)

    async def order_history(self, symbol: str, count: int = None, _from: int = None, urgency: int = 0) -> dict:
        params = {'pair': symbol.lower()}
        if count is not None:
            params['count'] = count
        if _from is not None:
            params['from'] = _from
        return await self._safe_call(urgency, self._request_private, 'orderHistory', params)

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
        await self._request_private('getInfo')
        return int((time.time() - t)*1000)

    async def _request_public(self, verb: str, endpoint: str):
        return await self._request(verb, endpoint)

    async def _request_private(self, method: str, data: dict = None) -> dict:
        if data is None:
            data = {}
        # preparing data
        data = {
            'method': method,
            'timestamp': int(time.time() * 1000),
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
        r = await self._request('post', 'tapi', **kwargs)
        self._last_request_ts = time.time()
        try:
            if r['success']:
                return r['return']
            else:
                raise self.Error(r['error'])
        except (KeyError, TypeError):
            raise self.Error(f'Bad response: {r}')

    async def _request(self, verb, endpoint, **kwargs) -> dict:
        url = self._base_url + endpoint
        # logger.debug(f'Requesting {verb.upper()} {endpoint} {kwargs}')
        try:
            async with getattr(self._session, verb)(url, **kwargs) as response:
                result = await self._handle_response(response)
        except ClientError as e:
            raise self.Error(str(e))
        return result

    async def _handle_response(self, response: aiohttp.ClientResponse) -> dict:
        if not str(response.status).startswith('2'):
            raise self.Error(await response.text())
        try:
            return await response.json()
        except ValueError:
            text = await response.text()
            raise self.Error(f'Invalid Response: {text}')


async def main():
    from config import config

    api = IndodaxAPI(
        config.get('Exchange', 'APIKey'),
        config.get('Exchange', 'APISecret')
    )
    print(await api.depth('btc_idr'))
    print(await api.account_info())

    r = await api.create_order('btc_idr', 'sell', '2000000000', '0.00045')
    print(r)
    oid = r['order_id']
    print(await api.order_info('btc_idr', oid))
    print(await api.cancel_order('btc_idr', oid, 'sell'))

    r = await api.create_order('btc_idr', 'buy', '5000000', '50000')
    print(r)
    oid = r['order_id']
    print(await api.order_info('btc_idr', oid))
    print(await api.cancel_order('btc_idr', oid, 'buy'))

    await api.stop()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())

