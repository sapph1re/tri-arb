import asyncio
from decimal import Decimal
from binance_api import BinanceApi
from helpers import run_async_repeatedly
from logger import get_logger

logger = get_logger(__name__)


class BinanceTradeFeeException(AttributeError):
    pass


class BinanceAccountInfo:
    def __init__(self, api: BinanceApi, auto_update_interval: int = 60):
        self._api = api
        self._can_trade = False
        self._trade_fee = Decimal('0.001')
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

    def can_trade(self) -> bool:
        return self._can_trade

    def get_trade_fee(self):
        return self._trade_fee

    def get_all_balances(self) -> dict:
        return self._balances

    def get_balance(self, asset: str) -> Decimal:
        try:
            return self._balances[asset]
        except KeyError:
            return Decimal('0')

    async def update_info(self):
        try:
            info = await self._api.account()
        except BinanceApi.Error as e:
            logger.error(f'Failed to load account info: {e.message}')
            return
        self._process_info(info)

    def _process_info(self, info):
        try:
            self._can_trade = info['canTrade']
            maker_commission = info['makerCommission']
            taker_commission = info['takerCommission']
            if maker_commission == taker_commission:
                self._trade_fee = Decimal(maker_commission) / 100 / 100
            else:
                raise BinanceTradeFeeException('Maker and Taker commissions are different! '
                                               'It can cause wrong calculations and profit loss!')
            self._balances.clear()
            for each in info['balances']:
                asset = each['asset']
                balance = Decimal(each['free'])
                self._balances[asset] = balance
            # logger.info(f'BAI > Update OK: {info}')
        except KeyError:
            logger.error(f'BAI > process_info() KeyError: Wrong data format! Data: {info}')
        except (ValueError, TypeError):
            logger.error(f'BAI > Could not parse balance for asset: {asset}')
        except BinanceTradeFeeException as e:
            raise e
        except BaseException as e:
            logger.exception(f'BAI > process_info(): Unknown EXCEPTION: {e}')

    def stop(self):
        self._update_stop.set()


async def main():
    from config import API_KEY, API_SECRET

    api = await BinanceApi.create(API_KEY, API_SECRET)
    acc = await BinanceAccountInfo.create(api, auto_update_interval=5)

    print(f'Trade fee: {acc.get_trade_fee()}')

    acc.stop()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
