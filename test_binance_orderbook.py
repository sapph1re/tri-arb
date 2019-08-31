import asyncio
import random
import json
from decimal import Decimal
from pydispatch import dispatcher
from binance_api import BinanceApi
from binance_websocket import BinanceWebsocket
from binance_orderbook import BinanceOrderbook
from logger import get_logger
logger = get_logger(__name__)


# {symbol: {
#       update_id: {
#           'bids': ...,
#           'asks': ...
#       },
#       ...
# },
# ... }
ob_snapshots = {}


def test_on_orderbook_changed(sender: BinanceOrderbook, symbol: str):
    orderbook = sender
    update_id = orderbook.get_update_id()
    logger.info(f'Orderbook changed: {symbol}, last update ID: {update_id}')
    if symbol not in ob_snapshots:
        ob_snapshots[symbol] = {}
    ob_snapshots[symbol][update_id] = {
        'bids': orderbook.get_bids(),
        'asks': orderbook.get_asks()
    }


def find_books_mismatch(a: list, b: list) -> str or None:
    if len(a) != len(b):
        return 'length mismatch'
    for i in range(len(a)):
        if a[i][0] != b[i][0]:
            return f'price mismatch on level {i}'
        if a[i][1] != b[i][1]:
            return f'amount mismatch on level {i}'
    return None


async def main():
    from config import API_KEY, API_SECRET

    api = await BinanceApi.create(API_KEY, API_SECRET)
    symbols_dict = await api.get_symbols_info()
    symbols_list = [(v.get_base_asset(), v.get_quote_asset()) for k, v in symbols_dict.items()]

    websockets = []
    orderbooks = []
    i = 999
    ws = None
    # pick a few random symbols
    random.shuffle(symbols_list)
    symbols_list = symbols_list[:5]
    for base, quote in symbols_list:
        if i >= 2:
            ws = BinanceWebsocket()
            websockets.append(ws)
            i = 0
        ob = BinanceOrderbook(api, base, quote, ws)
        orderbooks.append(ob)
        i += 1

    dispatcher.connect(test_on_orderbook_changed, signal='orderbook_changed', sender=dispatcher.Any)

    for ws in websockets:
        ws.start()

    await asyncio.sleep(15)

    for base, quote in symbols_list:
        symbol = (base + quote).lower()
        if symbol not in ob_snapshots:
            logger.info(f'No snapshots for {symbol}, skipping it')
            continue
        while True:
            depth = await api.depth(symbol=symbol, limit=20)
            update_id = depth['lastUpdateId']
            if update_id in ob_snapshots[symbol]:
                logger.info(f'Comparing snapshot of {symbol} at {update_id} with actual depth data...')
                bids = [(Decimal(price), Decimal(amount)) for price, amount in depth['bids']]
                asks = [(Decimal(price), Decimal(amount)) for price, amount in depth['asks']]
                bids_snapshot = ob_snapshots[symbol][update_id]['bids']
                asks_snapshot = ob_snapshots[symbol][update_id]['asks']
                bids_mismatch = find_books_mismatch(bids, bids_snapshot)
                asks_mismatch = find_books_mismatch(asks, asks_snapshot)
                if bids_mismatch:
                    logger.error(f'{symbol} at {update_id} bids: {bids_mismatch}')
                    logger.info(f'Bids snapshot: {bids_snapshot}')
                    logger.info(f'Bids actual: {bids}')
                if asks_mismatch:
                    logger.error(f'{symbol} at {update_id} asks: {asks_mismatch}')
                    logger.info(f'Asks snapshot: {asks_snapshot}')
                    logger.info(f'Asks actual: {asks}')
                if not bids_mismatch and not asks_mismatch:
                    logger.info(f'{symbol} at {update_id}: FULL MATCH')
                break
            else:
                logger.info(f'{symbol}: {update_id} not found in snapshots, retrying...')

    for ws in websockets:
        ws.stop()


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
