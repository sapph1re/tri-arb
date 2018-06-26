import sys
import time
import hmac
import hashlib
import urllib.parse
import json
from typing import List, Dict, Callable
from PyQt5.QtCore import (QObject, QByteArray, QUrl, QEventLoop, pyqtSignal)
from PyQt5.QtNetwork import (QNetworkAccessManager, QNetworkRequest, QNetworkReply)
from custom_logging import get_logger


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
        # TODO: make precision values
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


class BinanceApi(QObject):
    """
    Более подробная информация: https://bablofil.ru/binance-api/

    Практически во всех подписанных запросах необходимо указывать параметр timestamp - это текущее unix-время в милиосекундах.
    Но, так как некоторые сети бывают перегружены, то ваш запрос может заблудиться и придти позже.
    Поэтому биржа предоставляет вам временное окно (по умолчанию 5000 милисекунд).
    Если у вас запросы не успевают придти в это окно, вы можете его расширить с помощью параметра recvWindow.
    """

    methods = {
        # public methods
        'ping': {'url': 'api/v1/ping', 'method': 'GET', 'private': False},
        'time': {'url': 'api/v1/time', 'method': 'GET', 'private': False},
        'exchangeInfo': {'url': 'api/v1/exchangeInfo', 'method': 'GET', 'private': False},
        'depth': {'url': 'api/v1/depth', 'method': 'GET', 'private': False},
        'trades': {'url': 'api/v1/trades', 'method': 'GET', 'private': False},
        'historicalTrades': {'url': 'api/v1/historicalTrades', 'method': 'GET', 'private': False},
        'aggTrades': {'url': 'api/v1/aggTrades', 'method': 'GET', 'private': False},
        'klines': {'url': 'api/v1/klines', 'method': 'GET', 'private': False},
        'ticker24hr': {'url': 'api/v1/ticker/24hr', 'method': 'GET', 'private': False},
        'tickerPrice': {'url': 'api/v3/ticker/price', 'method': 'GET', 'private': False},
        'tickerBookTicker': {'url': 'api/v3/ticker/bookTicker', 'method': 'GET', 'private': False},
        # private methods
        'createOrder': {'url': 'api/v3/order', 'method': 'POST', 'private': True},
        'testOrder': {'url': 'api/v3/order/test', 'method': 'POST', 'private': True},
        'orderInfo': {'url': 'api/v3/order', 'method': 'GET', 'private': True},
        'cancelOrder': {'url': 'api/v3/order', 'method': 'DELETE', 'private': True},
        'openOrders': {'url': 'api/v3/openOrders', 'method': 'GET', 'private': True},
        'allOrders': {'url': 'api/v3/allOrders', 'method': 'GET', 'private': True},
        'account': {'url': 'api/v3/account', 'method': 'GET', 'private': True},
        'myTrades': {'url': 'api/v3/myTrades', 'method': 'GET', 'private': True},
    }

    start_call_api_async = pyqtSignal(str, 'PyQt_PyObject', 'QNetworkRequest', 'QByteArray')

    def __init__(self, api_key, api_secret, parent=None):
        super(BinanceApi, self).__init__(parent)

        self.start_call_api_async.connect(self.__call_api_async)

        self.api_key = api_key
        self.api_secret = bytearray(api_secret, encoding='utf-8')
        self.__q_nam = QNetworkAccessManager()

        self.__time_delta = 0
        time_response = self.time()
        try:
            server_time = time_response['serverTime']
            self.__time_delta = int(server_time - time.time() * 1000)
        except LookupError as e:
            logger.info('BAPI > Time synchronization got BAD response: {}'.format(str(e)))

    def ping(self, slot: Callable[[], None] or None = None) -> QNetworkReply or dict:
        """
        Проверка связи - /api/v1/ping
        Метод для проверки работы API.

        Вес - 1

        :return: пустой словарь в случае успеха.
        {}
        """
        return self.__call_api(slot=slot, command='ping')

    def time(self, slot: Callable[[], None] or None = None) -> QNetworkReply or dict:
        """
        Получение времени биржи - /api/v1/time

        Вес - 1

        :return: словарь с текущим временем.
        """
        return self.__call_api(slot=slot, command='time')

    def exchangeInfo(self, slot: Callable[[], None] or None = None) -> QNetworkReply or dict:
        """
        Настройки и лимиты биржи - /api/v1/exchangeInfo

        Вес - 1

        :return: структура данных в словаре
        """
        return self.__call_api(slot=slot, command='exchangeInfo')

    def depth(self, symbol: str, limit: int = 100, slot: Callable[[], None] or None = None) -> QNetworkReply or dict:
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
        return self.__call_api(slot=slot, command='depth', symbol=symbol.upper(), limit=limit)

    def trades(self, symbol: str, limit: int = 500, slot: Callable[[], None] or None = None) -> QNetworkReply or dict:
        """
        Последние (чужие) сделки - /api/v1/trades

        Вес - 1

        Параметры:
            Обязательные:
                :param symbol: пара
            Необязательные:
                :param limit: кол-во возвращаемых записей (максимум 500, по умолчанию 500).

        :return: список словарей.
        """
        return self.__call_api(slot=slot, command='trades', symbol=symbol.upper(), limit=limit)

    def aggTrades(self, symbol: str, fromID: int = None, limit: int = 500,
                  startTime: int = None, endTime: int = None,
                  slot: Callable[[], None] or None = None) -> QNetworkReply or dict:
        """
        Сжатая история сделок - /api/v1/aggTrades

        Метод позволяет получить суммарную историю сделок. Сделки, выполненные в одно время по одному ордеру
        и по одной цене будут представлены одной строкой с объединенным количеством.

        Вес - 1

        Параметры:
            Обязательные:
                :param symbol: пара
            Необязательные:
                :param fromID: показывать начиная со сделки № (включительно)
                :param startTime: начиная с какого времени (включительно)
                :param endTime: заканчивая каким временем (включительно)
                :param limit: Кол-во записей (максимум 500, по умолчанию 500)

        :return: список словарей
        """
        kwargs = {'command': 'aggTrades',
                  'slot': slot,
                  'symbol': symbol.upper(),
                  'limit': limit}
        if fromID:
            kwargs['fromID'] = fromID
        if startTime:
            kwargs['startTime'] = startTime
        if endTime:
            kwargs['endTime'] = endTime
        return self.__call_api(**kwargs)

    def klines(self, symbol: str, interval: str = '15m', limit: int = 500,
               startTime: int = None, endTime: int = None,
               slot: Callable[[], None] or None = None) -> QNetworkReply or dict:
        """
        Данные по свечам – /api/v1/klines

        Вес – 1

        Параметры:
            Обязательные:
                :param symbol: пара
                :param interval: период свечи
                    Допустимые интервалы:
                    •    1m     // 1 минута
                    •    3m     // 3 минуты
                    •    5m    // 5 минут
                    •    15m  // 15 минут
                    •    30m    // 30 минут
                    •    1h    // 1 час
                    •    2h    // 2 часа
                    •    4h    // 4 часа
                    •    6h    // 6 часов
                    •    8h    // 8 часов
                    •    12h    // 12 часов
                    •    1d    // 1 день
                    •    3d    // 3 дня
                    •    1w    // 1 неделя
                    •    1M    // 1 месяц
            Необязательные:
                :param limit: кол-во свечей (максимум 500, по умолчанию 500)
                :param startTime: время начала построения
                :param endTime: окончание периода
                Если не указаны параметры startTime и endTime, то возвращаются самые последние свечи.

        :return: список списков.
        """
        kwargs = {'command': 'klines',
                  'slot': slot,
                  'symbol': symbol.upper(),
                  'interval': interval,
                  'limit': limit}
        if startTime:
            kwargs['startTime'] = startTime
        if endTime:
            kwargs['endTime'] = endTime
        return self.__call_api(**kwargs)

    def ticker24hr(self, symbol: str = None, slot: Callable[[], None] or None = None) -> QNetworkReply or dict:
        """
        Статистика за 24 часа - /api/v1/ticker/24hr

        Вес – 1, если указана пара, иначе вес равен (количеству всех торгуемых пар)/2.

        Параметры:
            Необязательные:
                :param symbol: пара
                Если symbol не указан, возвращаются данные по всем парам.
                В этом случае, считается, что вы сделали столько запросов к бирже, сколько вернулось пар.

        :return: словарь, если указана пара, и список словарей, если пара не указана.
        """
        kwargs = {'command': 'ticker24hr',
                  'slot': slot}
        if symbol:
            kwargs['symbol'] = symbol.upper()
        return self.__call_api(**kwargs)

    def tickerPrice(self, symbol: str = None, slot: Callable[[], None] or None = None) -> QNetworkReply or dict:
        """
        Последняя цена по паре (или парам) - /api/v3/ticker/price

        Вес - 1

        Параметры:
            Необязательные:
                :param symbol: пара
                Если параметр symbol не указан, то возвращаются цены по всем парам.

        :return: словарь, если указана пара, и список словарей, если пара не указана.
        """
        kwargs = {'command': 'tickerPrice',
                  'slot': slot}
        if symbol:
            kwargs['symbol'] = symbol.upper()
        return self.__call_api(**kwargs)

    def tickerBookTicker(self, symbol: str = None, slot: Callable[[], None] or None = None) -> QNetworkReply or dict:
        """
        Лучшие цены покупки/продажи - /api/v3/ticker/bookTicker

        Вес 1

        Параметры:
            Необязательные:
                :param symbol: пара
                Если параметр symbol не указан, возвращаются данные по всем парам.

        :return: словарь, если указана пара, и список словарей, если пара не указана.
        """
        kwargs = {'command': 'tickerBookTicker',
                  'slot': slot}
        if symbol:
            kwargs['symbol'] = symbol.upper()
        return self.__call_api(**kwargs)

    def createOrder(self, symbol: str, side: str, type: str, quantity,
                    timeInForce: str = None, price=None, newClientOrderId: str = None,
                    stopPrice=None, icebergQty=None, recvWindow: int = None,
                    newOrderRespType: str = None, slot: Callable[[], None] or None = None) -> QNetworkReply or dict:
        """
        Создание ордера - /api/v3/order

        Вес - 1

        Параметры:
            Обязательные:
                :param symbol: пара
                :param side: тип ордера (BUY либо SELL)
                :param type: тип ордера (LIMIT, MARKET, STOP_LOSS, STOP_LOSS_LIMIT,
                    TAKE_PROFIT, TAKE_PROFIT_LIMIT, LIMIT_MAKER)
                :param quantity: количество к покупке
                timestamp: текущее время в миллисекундах (в коде, выложенном здесь, проставляется автоматически,
                    указывать не надо.
            Необязательные:
                :param timeInForce: (GTC, IOC, FOK). По умолчанию GTC. Расшифрую.
                    GTC (Good Till Cancelled) – ордер будет висеть до тех пор, пока его не отменят.
                    IOC (Immediate Or Cancel) – Будет куплено то количество, которое можно купить немедленно.
                        Все, что не удалось купить, будет отменено.
                    FOK (Fill-Or-Kill) – Либо будет куплено все указанное количество немедленно,
                        либо не будет куплено вообще ничего, ордер отменится.
                :param price: цена
                :param newClientOrderId: Идентификатор ордера, который вы сами придумаете (строка).
                    Если не указан, генерится автоматически.
                :param stopPrice: стоп-цена, можно указывать если тип ордера STOP_LOSS, STOP_LOSS_LIMIT, TAKE_PROFIT
                    или TAKE_PROFIT_LIMIT.
                :param icebergQty: кол-во для ордера-айсберга, можно указывать, если тип ордера LIMIT, STOP_LOSS_LIMIT
                    and TAKE_PROFIT_LIMIT
                :param recvWindow: кол-во миллисекунд, которое прибавляется к timestamp и
                    формирует окно действия запроса (см. выше). По умолчанию 5000.
                :param newOrderRespType: какую информацию возвращать, если удалось создать ордер.
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
        kwargs = {'command': 'createOrder',
                  'slot': slot,
                  'symbol': symbol.upper(),
                  'side': side.upper(),
                  'type': type.upper(),
                  'quantity': quantity}
        if timeInForce:
            kwargs['timeInForce'] = timeInForce
        if price:
            kwargs['price'] = price
        if newClientOrderId:
            kwargs['newClientOrderId'] = newClientOrderId
        if stopPrice:
            kwargs['stopPrice'] = stopPrice
        if icebergQty:
            kwargs['icebergQty'] = icebergQty
        if recvWindow:
            kwargs['recvWindow'] = recvWindow
        if newOrderRespType:
            kwargs['newOrderRespType'] = newOrderRespType
        return self.__call_api(**kwargs)

    def testOrder(self, symbol: str, side: str, type: str, quantity,
                  timeInForce: str = None, price=None, newClientOrderId: str = None,
                  stopPrice=None, icebergQty=None, recvWindow: int = None,
                  newOrderRespType: str = None, slot: Callable[[], None] or None = None) -> QNetworkReply or dict:
        """
        Тестирование создания ордера: /api/v3/order/test
        Метод позволяет протестировать создание ордера – например, проверить, правильно ли настроены временные рамки. По факту такой ордер никогда не будет исполнен, и средства на его создание затрачены не будут.

        Вес: 1

        Параметры:
            Обязательные:
                :param symbol: пара
                :param side: тип ордера (BUY либо SELL)
                :param type: тип ордера (LIMIT, MARKET, STOP_LOSS, STOP_LOSS_LIMIT,
                    TAKE_PROFIT, TAKE_PROFIT_LIMIT, LIMIT_MAKER)
                :param quantity: количество к покупке
                timestamp: текущее время в миллисекундах (в коде, выложенном здесь, проставляется автоматически,
                    указывать не надо.
            Необязательные:
                :param timeInForce: (GTC, IOC, FOK). По умолчанию GTC. Расшифрую.
                    GTC (Good Till Cancelled) – ордер будет висеть до тех пор, пока его не отменят.
                    IOC (Immediate Or Cancel) – Будет куплено то количество, которое можно купить немедленно.
                        Все, что не удалось купить, будет отменено.
                    FOK (Fill-Or-Kill) – Либо будет куплено все указанное количество немедленно,
                        либо не будет куплено вообще ничего, ордер отменится.
                :param price: цена
                :param newClientOrderId: Идентификатор ордера, который вы сами придумаете (строка).
                    Если не указан, генерится автоматически.
                :param stopPrice: стоп-цена, можно указывать если тип ордера STOP_LOSS, STOP_LOSS_LIMIT, TAKE_PROFIT
                    или TAKE_PROFIT_LIMIT.
                :param icebergQty: кол-во для ордера-айсберга, можно указывать, если тип ордера LIMIT, STOP_LOSS_LIMIT
                    and TAKE_PROFIT_LIMIT
                :param recvWindow: кол-во миллисекунд, которое прибавляется к timestamp и
                    формирует окно действия запроса (см. выше). По умолчанию 5000.
                :param newOrderRespType: какую информацию возвращать, если удалось создать ордер.
                    Допустимые значения ACK, RESULT, или FULL, по умолчанию RESULT.

        :return: пустой словарь в случае успеха.
        """
        kwargs = {'command': 'testOrder',
                  'slot': slot,
                  'symbol': symbol.upper(),
                  'side': side.upper(),
                  'type': type.upper(),
                  'quantity': quantity}
        if timeInForce:
            kwargs['timeInForce'] = timeInForce
        if price:
            kwargs['price'] = price
        if newClientOrderId:
            kwargs['newClientOrderId'] = newClientOrderId
        if stopPrice:
            kwargs['stopPrice'] = stopPrice
        if icebergQty:
            kwargs['icebergQty'] = icebergQty
        if recvWindow:
            kwargs['recvWindow'] = recvWindow
        if newOrderRespType:
            kwargs['newOrderRespType'] = newOrderRespType
        return self.__call_api(**kwargs)

    def orderInfo(self, symbol: str, orderId: int = None,
                  origClientOrderId: str = None, recvWindow: int = None,
                  slot: Callable[[], None] or None = None) -> QNetworkReply or dict:
        """
        Получить информацию по созданному ордеру.

        Вес – 1
        Метод – GET

        Параметры:
            Обязательные:
                :param symbol: пара
                :param orderId: ID ордера, назначенный биржей
                :param origClientOrderId: ID ордера, назначенный пользователем или сгенерированный (см. создание ордера)
                Либо orderId либо origClientOrderId необходимо предоставить.
                timestamp: текущее время (в представленном коде проставляется автоматически, указывать не надо)
            Необязательные:
                :param recvWindow: окно валидности запроса.

        :return: словарь.
        """
        kwargs = {'command': 'orderInfo',
                  'slot': slot,
                  'symbol': symbol.upper()}
        if orderId:
            kwargs['orderId'] = orderId
        else:
            kwargs['origClientOrderId'] = origClientOrderId
        if recvWindow:
            kwargs['recvWindow'] = recvWindow
        return self.__call_api(**kwargs)

    def cancelOrder(self, symbol: str, orderId: int = None,
                    origClientOrderId: str = None, newClientOrderId: str = None,
                    recvWindow: int = None, slot: Callable[[], None] or None = None) -> QNetworkReply or dict:
        """
        Отмена ордера - /api/v3/order

        Вес – 1
        Метод – DELETE

        Параметры:
            Обязательные:
                :param symbol: пара
                :param orderId: ID ордера, назначенный биржей
                :param origClientOrderId: ID ордера, назначенный пользователем или сгенерированный (см. создание ордера)
                Либо orderId либо origClientOrderId необходимо предоставить.
                timestamp: текущее время (в представленном коде проставляется автоматически, указывать не надо)
            Необязательные:
                :param newClientOrderId: позволяет однозначно определить отмену, если не указано, генерируется автоматически
                :param recvWindow: окно валидности запроса.

        :return: словарь.
        """
        kwargs = {'command': 'cancelOrder',
                  'slot': slot,
                  'symbol': symbol.upper()}
        if orderId:
            kwargs['orderId'] = orderId
        else:
            kwargs['origClientOrderId'] = origClientOrderId
        if recvWindow:
            kwargs['recvWindow'] = recvWindow
        if newClientOrderId:
            kwargs['newClientOrderId'] = newClientOrderId
        return self.__call_api(**kwargs)

    def openOrders(self, symbol: str = None, recvWindow: int = None,
                   slot: Callable[[], None] or None = None) -> QNetworkReply or dict:
        """
        Текущие открытые пользователем ордера - /api/v3/openOrders

        Вес – 1 если указана пара, либо (количество всех открытых для торгов пар) / 2.
        Метод – GET

        Параметры:
            Обязательные:
                timestamp: текущее время (в представленном коде проставляется автоматически, указывать не надо)
            Необязательные:
                :param symbol: пара
                :param recvWindow: окно валидности запроса.

        :return: список словарей.
        """
        kwargs = {'command': 'openOrders',
                  'slot': slot}
        if symbol:
            kwargs['symbol'] = symbol.upper()
        if recvWindow:
            kwargs['recvWindow'] = recvWindow
        return self.__call_api(**kwargs)

    def allOrders(self, symbol: str, orderId: int = None,
                  limit: int = None, recvWindow: int = None,
                  slot: Callable[[], None] or None = None) -> QNetworkReply or dict:
        """
        Все ордера пользователя вообще - /api/v3/allOrders
        Метод позволяет получить вообще все ордера пользователя – открытые, исполненные или отмененные.

        Вес – 5
        Метод – GET

        Параметры:
            Обязательные:
                :param symbol: пара
                timestamp: текущее время (в представленном коде проставляется автоматически, указывать не надо)
            Не обязательные:
                :param orderId: Если указан, то вернутся все ордера, которые >= указанному. Если не указан, вернутся самые последние.
                :param limit: кол-во возвращаемых ордеров (максимум 500, по умолчанию 500)
                :param recvWindow: окно валидности запроса.

        :return: список словарей.
        """
        kwargs = {'command': 'allOrders',
                  'slot': slot,
                  'symbol': symbol.upper()}
        if orderId:
            kwargs['orderId'] = orderId
        if limit:
            kwargs['limit'] = limit
        if recvWindow:
            kwargs['recvWindow'] = recvWindow
        return self.__call_api(**kwargs)

    def account(self, recvWindow: int = None, slot: Callable[[], None] or None = None) -> QNetworkReply or dict:
        """
        Информация по аккаунту - /api/v3/account

        Вес – 5
        Метод – GET

        Параметры:
            Обязательные:
                timestamp: текущее время (в представленном коде проставляется автоматически, указывать не надо)
            Не обязательные:
                :param recvWindow: окно валидности запроса.

        :return: словарь.
        """
        kwargs = {'command': 'account',
                  'slot': slot}
        if recvWindow:
            kwargs['recvWindow'] = recvWindow
        return self.__call_api(**kwargs)

    def myTrades(self, symbol: str, limit: int = None,
                 fromId: int = None, recvWindow: int = None,
                 slot: Callable[[], None] or None = None) -> QNetworkReply or dict:
        """
        Список сделок пользователя - /api/v3/myTrades
        Метод позволяет получить историю торгов авторизованного пользователя по указанной паре.

        Вес – 5.

        Параметры:
            Обязательные:
                :param symbol: пара
                timestamp: текущее время (в представленном коде проставляется автоматически, указывать не надо)
            Не обязательные:
                :param limit: кол-во возвращаемых сделок (максимум 500, по умолчанию 500)
                :param fromId: с какой сделки начинать вывод. По умолчанию выводятся самые последние.
                :param recvWindow: окно валидности запроса.

        :return: список словарей.
        """
        kwargs = {'command': 'myTrades',
                  'slot': slot,
                  'symbol': symbol.upper()}
        if limit:
            kwargs['limit'] = limit
        if fromId:
            kwargs['fromId'] = fromId
        if recvWindow:
            kwargs['recvWindow'] = recvWindow
        return self.__call_api(**kwargs)

    def __call_api(self, **kwargs) -> dict or None:
        command = kwargs.pop('command')
        slot = kwargs.pop('slot')
        api_url = 'https://api.binance.com/' + self.methods[command]['url']

        payload = kwargs
        headers = {}

        method = self.methods[command]['method']
        private = self.methods[command]['private']

        if private:
            payload.update({'timestamp': int(time.time() * 1000) + self.__time_delta})

            sign = hmac.new(
                key=self.api_secret,
                msg=urllib.parse.urlencode(payload).encode('utf-8'),
                digestmod=hashlib.sha256
            ).hexdigest()

            payload.update({'signature': sign})
            headers = dict()
            headers['X-MBX-APIKEY'] = self.api_key
            headers['Content-Type'] = 'application/x-www-form-urlencoded'

        if method == 'GET':
            api_url += '?' + urllib.parse.urlencode(payload)

        q_url = QUrl(api_url)
        q_data = QByteArray()
        key_list = list(payload.keys())
        for key in key_list:
            param = str(key) + '=' + str(payload[key])
            if key is not key_list[-1]:
                param += '&'
            q_data.append(param)

        q_request = QNetworkRequest()
        q_request.setUrl(q_url)
        for k, v in headers.items():
            header = QByteArray().append(k)
            value = QByteArray().append(v)
            q_request.setRawHeader(header, value)

        if slot:
            self.start_call_api_async.emit(method, slot, q_request, q_data)
            return None
        else:
            return self.__call_api_sync(method, q_request, q_data)

    def __call_api_sync(self, method: str, q_request: QNetworkRequest, q_data: QByteArray) -> dict:
        reply = None
        if method == 'POST':
            reply = self.__q_nam.post(q_request, q_data)
        elif method == 'DELETE':
            reply = self.__q_nam.deleteResource(q_request)
        elif method == 'GET':
            reply = self.__q_nam.get(q_request)
        else:
            logger.error('BAPI > Request: No such method!')

        if reply:
            # logger.debug('BAPI> Request without slot defined can slow down the application!')
            loop = QEventLoop()
            reply.finished.connect(loop.quit)
            loop.exec()
            response = bytes(reply.readAll()).decode("utf-8")
            response_json = {}
            if response:
                try:
                    response_json = json.loads(response)
                except json.JSONDecodeError:
                    logger.error('BAPI> Request FAILED, server response is not JSON: {}', response)
                    return {'error': 'Bad response'}
            return response_json
        else:
            logger.error('BAPI> Request FAILED: No Reply')
            return {'error': 'No Reply'}

    def __call_api_async(self, method, slot, q_request, q_data):
        reply = None
        if method == 'POST':
            reply = self.__q_nam.post(q_request, q_data)
        elif method == 'DELETE':
            reply = self.__q_nam.deleteResource(q_request)
        elif method == 'GET':
            reply = self.__q_nam.get(q_request)
        else:
            logger.error('BAPI > Request: No such method!')

        if reply:
            reply.finished.connect(slot)
        else:
            logger.error('BAPI> Request FAILED: No Reply')

    def get_symbols_info_json(self) -> List[dict]:
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
        response_json = self.exchangeInfo()
        try:
            symbols_info = response_json['symbols']
        except LookupError:
            return []
        return symbols_info

    def get_symbols_info(self) -> Dict[str, BinanceSymbolInfo]:
        return {each['symbol']: BinanceSymbolInfo(each) for each in self.get_symbols_info_json()}


class _SelfTestReceiver(QObject):

    def __init__(self):
        super(_SelfTestReceiver, self).__init__()
        self.__counter = 0

    def receive_slot(self):
        self.__counter += 1
        reply = self.sender()
        request = reply.request()
        request_url = str(request.url().path()).ljust(25)
        response = bytes(reply.readAll()).decode("utf-8")
        try:
            response_json = json.loads(response)
        except json.JSONDecodeError:
            print('Bad response from server')
            return
        print('{}: from {} : {} ### {}'.format(str(self.__counter).zfill(2), reply.operation(),
                                               request_url, response_json))

    def print(self, method, message):
        self.__counter += 1
        method = method.ljust(20)
        print('{}: from {} ### {}'.format(str(self.__counter).zfill(2), method, message))

    def reset_counter(self):
        self.__counter = 0


def _main():
    from PyQt5.QtCore import QCoreApplication, QTimer
    from config import API_KEY, API_SECRET

    app = QCoreApplication(sys.argv)

    bapi = BinanceApi(API_KEY, API_SECRET)
    tr = _SelfTestReceiver()
    slot = tr.receive_slot
    # for i in range(10):
    #     QTimer.singleShot(0, lambda: bapi.time(slot))

    sync_func_list = [('ping', lambda: bapi.ping()),
                      ('time', lambda: bapi.time()),
                      ('exchangeInfo', lambda: bapi.exchangeInfo()),
                      ('depth', lambda: bapi.depth('ethbtc', limit=5)),
                      ('trades', lambda: bapi.trades('ethbtc', limit=5)),
                      ('aggTrades', lambda: bapi.aggTrades('ethbtc', limit=5)),
                      ('klines', lambda: bapi.klines('ethbtc', limit=5)),
                      ('ticker24hr', lambda: bapi.ticker24hr('ethbtc')),
                      ('tickerPrice', lambda: bapi.tickerPrice()),
                      ('tickerBookTicker', lambda: bapi.tickerBookTicker(symbol='ethbtc')),
                      ('testOrder', lambda: bapi.testOrder('ethbtc', 'BUY', 'MARKET', 0.5)),
                      ('orderInfo', lambda: bapi.orderInfo('ethbtc', 56577459)),
                      ('cancelOrder', lambda: bapi.cancelOrder('ethbtc', 56577459)),
                      ('openOrders', lambda: bapi.openOrders()),
                      ('allOrders', lambda: bapi.allOrders('ethbtc', limit=5)),
                      ('account', lambda: bapi.account()),
                      ('myTrades', lambda: bapi.myTrades('ethbtc', limit=5))]

    async_func_list = [lambda: bapi.ping(slot=slot),
                       lambda: bapi.time(slot=slot),
                       lambda: bapi.exchangeInfo(slot=slot),
                       lambda: bapi.depth('ethbtc', limit=5, slot=slot),
                       lambda: bapi.trades('ethbtc', limit=5, slot=slot),
                       lambda: bapi.aggTrades('ethbtc', limit=5, slot=slot),
                       lambda: bapi.klines('ethbtc', limit=5, slot=slot),
                       lambda: bapi.ticker24hr('ethbtc', slot=slot),
                       lambda: bapi.tickerPrice(slot=slot),
                       lambda: bapi.tickerBookTicker(symbol='ethbtc', slot=slot),
                       lambda: bapi.testOrder('ethbtc', 'BUY', 'MARKET', 0.5, slot=slot),
                       lambda: bapi.orderInfo('ethbtc', 56577459, slot=slot),
                       lambda: bapi.cancelOrder('ethbtc', 56577459, slot=slot),
                       lambda: bapi.openOrders(slot=slot),
                       lambda: bapi.allOrders('ethbtc', limit=5, slot=slot),
                       lambda: bapi.account(slot=slot),
                       lambda: bapi.myTrades('ethbtc', limit=5, slot=slot)]

    print('<> Summary {} functions!'.format(len(sync_func_list)))
    print('> Synchronous calls:')
    tr.reset_counter()
    for method, func in sync_func_list:
        tr.print(method, func())
    print()
    print('<> Summary {} functions!'.format(len(async_func_list)))
    print('> Asynchronous calls:')
    tr.reset_counter()
    for func in async_func_list:
        QTimer.singleShot(0, func)

    sys.exit(app.exec_())


if __name__ == '__main__':
    _main()
