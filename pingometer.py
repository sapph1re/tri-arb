import asyncio
from config import config, get_exchange_class
from logger import get_logger
logger = get_logger(__name__)


async def main():
    exchange_class = get_exchange_class()
    exchange = await exchange_class.create(
        config.get('Exchange', 'APIKey'),
        config.get('Exchange', 'APISecret')
    )
    pmin, pmax, pavg = await exchange.measure_ping()
    print(f'Ping: min {pmin} ms, max {pmax} ms, avg {pavg} ms')


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
