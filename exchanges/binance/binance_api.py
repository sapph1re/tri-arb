import time
import asyncio
from typing import List, Tuple
from .binance.client import AsyncClient
from .binance.exceptions import BinanceAPIException
from logger import get_logger
logger = get_logger(__name__)


class BinanceAPI:

    class Error(BaseException):
        def __init__(self, message):
            self.message = message

    def __init__(self, client: AsyncClient):
        self._client = client

    @classmethod
    async def create(cls, api_key: str, api_secret: str):
        client = await AsyncClient.create(api_key, api_secret)
        return cls(client)

    async def safe_call(self, func, *args, **kwargs):
        tries = 10
        try:
            while 1:
                try:
                    return await func(*args, **kwargs)
                except BaseException as e:
                    logger.warning(f'API call failed: {args} {kwargs}. Reason: {e}')
                    tries -= 1
                    if tries > 0:
                        continue
                    else:
                        raise
        except (asyncio.TimeoutError, BinanceAPIException):
            raise self.Error('Failed 10 times')

    async def time(self) -> dict:
        """
        Получение времени биржи - /api/v1/time

        Вес - 1

        :return: словарь с текущим временем.
        """
        return await self.safe_call(
            self._client.get_server_time
        )

    async def exchange_info(self) -> dict:
        """
        Настройки и лимиты биржи - /api/v1/exchangeInfo

        Вес - 1

        :return: структура данных в словаре
        """
        return await self.safe_call(
            self._client.get_exchange_info
        )

    async def depth(self, symbol: str, limit: int = 100) -> dict:
        """
        Открытые ордера на бирже - /api/v1/depth
        Метод позволяет получить книгу ордеров.

        Вес зависит от параметра limit. При лимите от 5 до 100 вес будет равен 1. Для параметра 500 вес составит 5.
        Для параметра 1000 вес будет 10.

        Параметры:
            Обязательные:
                :param symbol: пара
            Необязательные:
                :param limit: кол-во возвращаемых записей от 5 до 1000 (по умолчанию 100).
                    Допустимые значения: 5, 10, 20, 50, 100, 500, 1000.
                    Еще можно указать 0, но он может вернуть большое кол-во данных.

        :return: значения в словаре
        """
        return await self.safe_call(
            self._client.get_order_book,
            symbol=symbol.upper(),
            limit=limit
        )

    async def create_order(self, symbol: str, side: str, order_type: str, quantity,
                           time_in_force: str = None, price=None, new_client_order_id: str = None,
                           stop_price=None, iceberg_qty=None, recv_window: int = None,
                           new_order_resp_type: str = None) -> dict:
        """
        Создание ордера - /api/v3/order

        Вес - 1

        Параметры:
            Обязательные:
                :param symbol: пара
                :param side: тип ордера (BUY либо SELL)
                :param order_type: тип ордера (LIMIT, MARKET, STOP_LOSS, STOP_LOSS_LIMIT,
                    TAKE_PROFIT, TAKE_PROFIT_LIMIT, LIMIT_MAKER)
                :param quantity: количество к покупке
                timestamp: текущее время в миллисекундах (в коде, выложенном здесь, проставляется автоматически,
                    указывать не надо.
            Необязательные:
                :param time_in_force: (GTC, IOC, FOK). По умолчанию GTC. Расшифрую.
                    GTC (Good Till Cancelled) – ордер будет висеть до тех пор, пока его не отменят.
                    IOC (Immediate Or Cancel) – Будет куплено то количество, которое можно купить немедленно.
                        Все, что не удалось купить, будет отменено.
                    FOK (Fill-Or-Kill) – Либо будет куплено все указанное количество немедленно,
                        либо не будет куплено вообще ничего, ордер отменится.
                :param price: цена
                :param new_client_order_id: Идентификатор ордера, который вы сами придумаете (строка).
                    Если не указан, генерится автоматически.
                :param stop_price: стоп-цена, можно указывать если тип ордера STOP_LOSS, STOP_LOSS_LIMIT, TAKE_PROFIT
                    или TAKE_PROFIT_LIMIT.
                :param iceberg_qty: кол-во для ордера-айсберга, можно указывать, если тип ордера LIMIT, STOP_LOSS_LIMIT
                    and TAKE_PROFIT_LIMIT
                :param recv_window: кол-во миллисекунд, которое прибавляется к timestamp и
                    формирует окно действия запроса (см. выше). По умолчанию 5000.
                :param new_order_resp_type: какую информацию возвращать, если удалось создать ордер.
                    Допустимые значения ACK, RESULT, или FULL, по умолчанию RESULT.

        В зависимости от типа ордера, некоторые поля становятся обязательными:

            Тип ордера                    Обязательные поля
            LIMIT                         timeInForce, quantity, price
            MARKET                        quantity
            STOP_LOSS                     quantity, stopPrice
            STOP_LOSS_LIMIT               timeInForce, quantity, price, stopPrice
            TAKE_PROFIT                   quantity, stopPrice
            TAKE_PROFIT_LIMIT             timeInForce, quantity, price, stopPrice
            LIMIT_MAKER                   quantity, price

        Ордера типа LIMIT_MAKER – это ордера типа обычного LIMIT, но они отклонятся,
            если ордер при выставлении может выполниться по рынку. Другими словами, вы никогда не будете тейкером,
            ордер либо выставится выше/ниже рынка, либо не выставится вовсе.
        Ордера типа STOP_LOSS и TAKE_PROFIT исполнятся по рынку (ордер типа MARKET),
            как только будет достигнута цена stopPrice.
        Любые ордера LIMIT или LIMIT_MAKER могут формировать ордер-айсберг, установив параметр icebergQty.
        Если установлен параметр icebergQty, то параметр timeInForce ОБЯЗАТЕЛЬНО должен иметь значение GTC.

        Для того, что бы выставлять цены, противоположные текущим для ордеров типов MARKET и LIMIT:
        Цена выше рыночной: STOP_LOSS BUY, TAKE_PROFIT SELL
        Цена ниже рыночной: STOP_LOSS SELL, TAKE_PROFIT BUY

        :return: при создании ордера вернется словарь, содержимое которого зависит от newOrderRespType
        """
        kwargs = {
            'symbol': symbol.upper(),
            'side': side.upper(),
            'type': order_type.upper(),
            'quantity': quantity
        }
        if order_type.upper() != 'MARKET':
            kwargs['timeInForce'] = time_in_force if time_in_force is not None else 'GTC'
        if price:
            kwargs['price'] = price
        if new_client_order_id:
            kwargs['newClientOrderId'] = new_client_order_id
        if stop_price:
            kwargs['stopPrice'] = stop_price
        if iceberg_qty:
            kwargs['icebergQty'] = iceberg_qty
        if recv_window:
            kwargs['recvWindow'] = recv_window
        if new_order_resp_type:
            kwargs['newOrderRespType'] = new_order_resp_type
        return await self.safe_call(
            self._client.create_order,
            **kwargs
        )

    async def test_order(self, symbol: str, side: str, order_type: str, quantity,
                         time_in_force: str = None, price=None, new_client_order_id: str = None,
                         stop_price=None, iceberg_qty=None, recv_window: int = None,
                         new_order_resp_type: str = None) -> dict:
        """
        Тестирование создания ордера: /api/v3/order/test
        Метод позволяет протестировать создание ордера – например, проверить, правильно ли настроены временные рамки. По факту такой ордер никогда не будет исполнен, и средства на его создание затрачены не будут.

        Вес: 1

        Параметры:
            Обязательные:
                :param symbol: пара
                :param side: тип ордера (BUY либо SELL)
                :param order_type: тип ордера (LIMIT, MARKET, STOP_LOSS, STOP_LOSS_LIMIT,
                    TAKE_PROFIT, TAKE_PROFIT_LIMIT, LIMIT_MAKER)
                :param quantity: количество к покупке
                timestamp: текущее время в миллисекундах (в коде, выложенном здесь, проставляется автоматически,
                    указывать не надо.
            Необязательные:
                :param time_in_force: (GTC, IOC, FOK). По умолчанию GTC. Расшифрую.
                    GTC (Good Till Cancelled) – ордер будет висеть до тех пор, пока его не отменят.
                    IOC (Immediate Or Cancel) – Будет куплено то количество, которое можно купить немедленно.
                        Все, что не удалось купить, будет отменено.
                    FOK (Fill-Or-Kill) – Либо будет куплено все указанное количество немедленно,
                        либо не будет куплено вообще ничего, ордер отменится.
                :param price: цена
                :param new_client_order_id: Идентификатор ордера, который вы сами придумаете (строка).
                    Если не указан, генерится автоматически.
                :param stop_price: стоп-цена, можно указывать если тип ордера STOP_LOSS, STOP_LOSS_LIMIT, TAKE_PROFIT
                    или TAKE_PROFIT_LIMIT.
                :param iceberg_qty: кол-во для ордера-айсберга, можно указывать, если тип ордера LIMIT, STOP_LOSS_LIMIT
                    and TAKE_PROFIT_LIMIT
                :param recv_window: кол-во миллисекунд, которое прибавляется к timestamp и
                    формирует окно действия запроса (см. выше). По умолчанию 5000.
                :param new_order_resp_type: какую информацию возвращать, если удалось создать ордер.
                    Допустимые значения ACK, RESULT, или FULL, по умолчанию RESULT.

        :return: пустой словарь в случае успеха.
        """
        kwargs = {
            'symbol': symbol.upper(),
            'side': side.upper(),
            'type': order_type.upper(),
            'quantity': quantity
        }
        if time_in_force:
            kwargs['timeInForce'] = time_in_force
        if price:
            kwargs['price'] = price
        if new_client_order_id:
            kwargs['newClientOrderId'] = new_client_order_id
        if stop_price:
            kwargs['stopPrice'] = stop_price
        if iceberg_qty:
            kwargs['icebergQty'] = iceberg_qty
        if recv_window:
            kwargs['recvWindow'] = recv_window
        if new_order_resp_type:
            kwargs['newOrderRespType'] = new_order_resp_type
        return await self.safe_call(
            self._client.create_test_order,
            **kwargs
        )

    async def order_info(self, symbol: str, order_id: int = None, orig_client_order_id: str = None,
                         recv_window: int = None) -> dict:
        """
        Получить информацию по созданному ордеру.

        Вес – 1
        Метод – GET

        Параметры:
            Обязательные:
                :param symbol: пара
                :param order_id: ID ордера, назначенный биржей
                :param orig_client_order_id: ID ордера, назначенный пользователем или сгенерированный (см. создание ордера)
                Либо order_id либо orig_client_order_id необходимо предоставить.
                timestamp: текущее время (в представленном коде проставляется автоматически, указывать не надо)
            Необязательные:
                :param recv_window: окно валидности запроса.

        :return: словарь.
        """
        kwargs = {'symbol': symbol.upper()}
        if order_id:
            kwargs['orderId'] = order_id
        else:
            kwargs['origClientOrderId'] = orig_client_order_id
        if recv_window:
            kwargs['recvWindow'] = recv_window
        return await self.safe_call(
            self._client.get_order,
            **kwargs
        )

    async def cancel_order(self, symbol: str, order_id: int = None,
                           orig_client_order_id: str = None, new_client_order_id: str = None,
                           recv_window: int = None) -> dict:
        """
        Отмена ордера - /api/v3/order

        Вес – 1
        Метод – DELETE

        Параметры:
            Обязательные:
                :param symbol: пара
                :param order_id: ID ордера, назначенный биржей
                :param orig_client_order_id: ID ордера, назначенный пользователем или сгенерированный (см. создание ордера)
                Либо orderId либо origClientOrderId необходимо предоставить.
                timestamp: текущее время (в представленном коде проставляется автоматически, указывать не надо)
            Необязательные:
                :param new_client_order_id: позволяет однозначно определить отмену, если не указано, генерируется автоматически
                :param recv_window: окно валидности запроса.

        :return: словарь.
        """
        kwargs = {'symbol': symbol.upper()}
        if order_id:
            kwargs['orderId'] = order_id
        else:
            kwargs['origClientOrderId'] = orig_client_order_id
        if recv_window:
            kwargs['recvWindow'] = recv_window
        if new_client_order_id:
            kwargs['newClientOrderId'] = new_client_order_id
        return await self.safe_call(
            self._client.cancel_order,
            **kwargs
        )

    async def account(self, recv_window: int = None) -> dict:
        """
        Информация по аккаунту - /api/v3/account

        Вес – 5
        Метод – GET

        Параметры:
            Обязательные:
                timestamp: текущее время (в представленном коде проставляется автоматически, указывать не надо)
            Не обязательные:
                :param recv_window: окно валидности запроса.

        :return: словарь.
        """
        kwargs = {}
        if recv_window:
            kwargs['recvWindow'] = recv_window
        return await self.safe_call(
            self._client.get_account,
            **kwargs
        )

    async def get_symbols_info(self) -> List[dict]:
        """
        :return: list of dictionaries with symbols info:
        {
            symbol: "ETHBTC",
            status: "TRADING",      #  Возможные статусы: PRE_TRADING, TRADING, POST_TRADING, END_OF_DAY,
                                    #                    HALT, AUCTION_MATCH, BREAK
            baseAsset: "ETH",
            baseAssetPrecision: 8,  #  Как мы заметили, он одинаковый (т.е. равен 8) для всех символов
            quoteAsset: "BTC",
            quotePrecision: 8,      #  Как мы заметили, он одинаковый (т.е. равен 8) для всех символов
            orderTypes: [
                "LIMIT",
                "LIMIT_MAKER",
                "MARKET",
                "STOP_LOSS_LIMIT",
                "TAKE_PROFIT_LIMIT"
            ],
            icebergAllowed: false,  #
            filters: [
                {
                    filterType: "PRICE_FILTER",     #  Ограничение цены создаваемого ордера.
                    minPrice: "0.00000100",         #  Цена ордера должна быть в диапазоне min_price и max_price,
                    maxPrice: "100000.00000000",    #   и шаг торговли должен быть кратен tickSize.
                    tickSize: "0.00000100"          #  Да да, тут нельзя ставить ордера с произвольной ценой.
                },
                {
                    filterType: "LOT_SIZE",         #  ограничение объема создаваемого ордера.
                    minQty: "0.00100000",           #  Объем должен быть в диапазоне minQty и maxQty,
                    maxQty: "100000.00000000",      #   и быть кратен stepSize.
                    stepSize: "0.00100000"
                },
                {
                    filterType: "MIN_NOTIONAL",     # Итоговая сумма ордера (объем*цена) должна быть выше minNotional.
                    minNotional: "0.00100000"
                }
            ]
        }
        """
        response_json = await self.safe_call(
            self.exchange_info
        )
        try:
            symbols_info = response_json['symbols']
        except LookupError:
            raise self.Error('Failed to load symbols')
        return symbols_info

    async def measure_ping(self) -> Tuple[int, int, int]:
        pings = [
            await self._measure_ping_once()
            for i in range(10)
        ]
        avg = int(sum(pings) / len(pings))
        return min(pings), max(pings), avg

    async def stop(self):
        await self._client.session.close()

    async def _measure_ping_once(self) -> int:
        t = time.time()
        await self._client.get_account()
        return int((time.time() - t)*1000)


async def main():
    from config import config

    api = await BinanceAPI.create(
        config.get('Exchange', 'APIKey'),
        config.get('Exchange', 'APISecret')
    )
    funcs = [
        api.time(),
        api.exchange_info(),
        api.depth(symbol='ethbtc', limit=5),
        api.test_order(symbol='ethbtc', side='BUY', order_type='MARKET', quantity=0.5),
        api.order_info(symbol='ethbtc', order_id=56577459),
        api.cancel_order(symbol='ethbtc', order_id=56577459),
        api.account()
    ]
    for func in funcs:
        try:
            result = await func
        except BinanceAPI.Error as e:
            result = e.message
        print(f'{func.__name__}():\t{result}')


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
