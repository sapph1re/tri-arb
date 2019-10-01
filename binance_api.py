import asyncio
from typing import List, Dict
from binance import AsyncClient
from binance.exceptions import BinanceAPIException
from logger import get_logger


logger = get_logger(__name__)


class BinanceSymbolInfo:

    def __init__(self, json: dict):
        """
        :return: list of dictionaries with symbols info:
        {
            baseAssetPrecision: 8,  #  Как мы заметили, он одинаковый (т.е. равен 8) для всех символов
            quotePrecision: 8,      #  Как мы заметили, он одинаковый (т.е. равен 8) для всех символов
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
        self.__symbol = json['symbol']
        self.__status = json[
            'status']  # Возможные статусы: PRE_TRADING, TRADING, POST_TRADING, END_OF_DAY, HALT, AUCTION_MATCH, BREAK
        self.__base_asset = json['baseAsset']
        self.__quote_asset = json['quoteAsset']
        self.__order_types = json[
            'orderTypes']  # "LIMIT", "LIMIT_MAKER", "MARKET", "STOP_LOSS_LIMIT", "TAKE_PROFIT_LIMIT"
        self.__iceberg_allowed = json['icebergAllowed']

        self.__min_price_filter = None
        self.__max_price_filter = None
        self.__price_step_size = None
        self.__min_qty_filter = None
        self.__max_qty_filter = None
        self.__qty_step_size = None
        self.__min_notional = None

        filters = json['filters']
        for each in filters:
            if each['filterType'] == 'PRICE_FILTER':
                self.__min_price_filter = each['minPrice']
                self.__max_price_filter = each['maxPrice']
                self.__price_step_size = each['tickSize']
            elif each['filterType'] == 'LOT_SIZE':
                self.__min_qty_filter = each['minQty']
                self.__max_qty_filter = each['maxQty']
                self.__qty_step_size = each['stepSize']
            elif each['filterType'] == 'MIN_NOTIONAL':
                self.__min_notional = each['minNotional']

    def get_symbol(self):
        """
        :return:  symbol (for example 'ETHBTC')
        """
        return self.__symbol

    def get_status(self):
        """
        Possible statuses: PRE_TRADING, TRADING, POST_TRADING, END_OF_DAY, HALT, AUCTION_MATCH, BREAK
        :return: One of possible statuses.
        """
        return self.__status

    def get_base_asset(self):
        """
        :return: base asset (from symbol 'ETHBTC' it returns 'ETH')
        """
        return self.__base_asset

    def get_quote_asset(self):
        """
        :return: quote asset (from symbol 'ETHBTC' it returns 'BTC')
        """
        return self.__quote_asset

    def get_order_types(self):
        """
        Possible types: "LIMIT", "LIMIT_MAKER", "MARKET", "STOP_LOSS_LIMIT", "TAKE_PROFIT_LIMIT"
        :return: list of available order types for current symbol
        """
        return self.__order_types

    def is_iceberg_allowed(self):
        return self.__iceberg_allowed

    def get_price_filter(self):
        """
        Price limit filter:
        1. minPrice <= yourPrice <= maxPrice
        2. yourPrice % priceStepSize = 0
        :return: tuple of 3 elements: minPrice, maxPrice and priceStepSize
        """
        return self.__min_price_filter, self.__max_price_filter, self.__price_step_size

    def get_qty_filter(self):
        """
        Qty limit filter:
        1. minQty <= yourQty <= maxQty
        2. yourQty % qtyStepSize = 0
        :return: tuple of 3 elements: minQty, maxQty and qtyStepSize
        """
        return self.__min_qty_filter, self.__max_qty_filter, self.__qty_step_size

    def get_min_notional(self):
        """
        Notional filter:
        yourPrice * yourQty >= minNotional
        :return: minNotional
        """
        return self.__min_notional


class BinanceApi:
    def __init__(self, client: AsyncClient):
        self.client = client

    @classmethod
    async def create(cls, api_key, api_secret):
        client = await AsyncClient.create(api_key, api_secret)
        return cls(client)

    async def time(self) -> dict:
        """
        Получение времени биржи - /api/v1/time

        Вес - 1

        :return: словарь с текущим временем.
        """
        return await self.client.get_server_time()

    async def exchange_info(self) -> dict:
        """
        Настройки и лимиты биржи - /api/v1/exchangeInfo

        Вес - 1

        :return: структура данных в словаре
        """
        return await self.client.get_exchange_info()

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
        return await self.client.get_order_book(symbol=symbol.upper(), limit=limit)

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
        return await self.client.create_order(**kwargs)

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
        return await self.client.create_test_order(**kwargs)

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
        return await self.client.get_order(**kwargs)

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
        return await self.client.cancel_order(**kwargs)

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
        return await self.client.get_account(**kwargs)

    async def get_symbols_info_json(self) -> List[dict]:
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
        response_json = await self.exchange_info()
        try:
            symbols_info = response_json['symbols']
        except LookupError:
            return []
        return symbols_info

    async def get_symbols_info(self) -> Dict[str, BinanceSymbolInfo]:
        return {
            each['symbol']: BinanceSymbolInfo(each)
            for each in await self.get_symbols_info_json()
        }


async def main():
    from config import API_KEY, API_SECRET

    api = await BinanceApi.create(API_KEY, API_SECRET)
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
        except BinanceAPIException as e:
            result = str(e)
        print(f'{func.__name__}():\t{result}')


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main())
