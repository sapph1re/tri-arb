import time
import urllib
import hmac
import hashlib
import requests

from urllib.parse import urlparse

class BinanceApi:

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

    def __init__(self, api_key, api_secret):
        self.api_key = api_key
        self.api_secret = bytearray(api_secret, encoding='utf-8')

    # def __getattr__(self, name):
    #     def wrapper(*args, **kwargs):
    #         kwargs.update(command=name)
    #         return self.call_api(**kwargs)
    #
    #     return wrapper

    def ping(self):
        """
        Проверка связи - /api/v1/ping
        Метод для проверки работы API.

        Вес - 1

        :return: пустой словарь в случае успеха.
        {}
        """
        return self.__call_api(command='ping')

    def time(self):
        """
        Получение времени биржи - /api/v1/time

        Вес - 1

        :return: словарь с текущим временем.
        """
        return self.__call_api(command='time')

    def exchangeInfo(self):
        """
        Настройки и лимиты биржи - /api/v1/exchangeInfo

        Вес - 1

        :return: структура данных в словаре
        """
        return self.__call_api(command='exchangeInfo')

    def depth(self, symbol: str, limit: int=100):
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
        return self.__call_api(command='depth', symbol=symbol.upper(), limit=limit)

    def trades(self, symbol: str, limit: int=500):
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
        return self.__call_api(command='trades', symbol=symbol.upper(), limit=limit)

    def aggTrades(self, symbol: str, fromID: int=None, limit: int=500,
                  startTime: int=None, endTime: int=None):
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
                  'symbol': symbol.upper(),
                  'limit': limit}
        if fromID:
            kwargs['fromID'] = fromID
        if startTime:
            kwargs['startTime'] = startTime
        if endTime:
            kwargs['endTime'] = endTime
        return self.__call_api(**kwargs)

    def klines(self, symbol: str, interval: str = '15m', limit: int=500,
               startTime: int=None, endTime: int=None):
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
                  'symbol': symbol.upper(),
                  'interval': interval,
                  'limit': limit}
        if startTime:
            kwargs['startTime'] = startTime
        if endTime:
            kwargs['endTime'] = endTime
        return self.__call_api(**kwargs)

    def ticker24hr(self, symbol: str=None):
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
        kwargs = {'command': 'ticker24hr'}
        if symbol:
            kwargs['symbol'] = symbol.upper()
        return self.__call_api(**kwargs)

    def tickerPrice(self, symbol: str=None):
        """
        Последняя цена по паре (или парам) - /api/v3/ticker/price

        Вес - 1

        Параметры:
            Необязательные:
                :param symbol: пара
                Если параметр symbol не указан, то возвращаются цены по всем парам.

        :return: словарь, если указана пара, и список словарей, если пара не указана.
        """
        kwargs = {'command': 'tickerPrice'}
        if symbol:
            kwargs['symbol'] = symbol.upper()
        return self.__call_api(**kwargs)

    def tickerBookTicker(self, symbol: str=None):
        """
        Лучшие цены покупки/продажи - /api/v3/ticker/bookTicker

        Вес 1

        Параметры:
            Необязательные:
                :param symbol: пара
                Если параметр symbol не указан, возвращаются данные по всем парам.

        :return: словарь, если указана пара, и список словарей, если пара не указана.
        """
        kwargs = {'command': 'tickerBookTicker'}
        if symbol:
            kwargs['symbol'] = symbol.upper()
        return self.__call_api(**kwargs)

    def createOrder(self, symbol: str, side: str, type: str, quantity,
                    timeInForce: str=None, price=None, newClientOrderId: str=None,
                    stopPrice=None, icebergQty=None, recvWindow: int=None,
                    newOrderRespType: str=None):
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
                    timeInForce: str=None, price=None, newClientOrderId: str=None,
                    stopPrice=None, icebergQty=None, recvWindow: int=None,
                    newOrderRespType: str=None):
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
                  origClientOrderId: str = None, recvWindow: int = None):
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
                    recvWindow: int = None):
        """
        Отмена ордера - /api/v3/order

        Вес – 1
        Метод – DELETE

        Параметры:
            Обязательные:
                :param symbol: пара
                :param orderId: ID ордера, назначенный биржей
                :param rigClientOrderId: ID ордера, назначенный пользователем или сгенерированный (см. создание ордера)
                Либо orderId либо origClientOrderId необходимо предоставить.
                timestamp: текущее время (в представленном коде проставляется автоматически, указывать не надо)
            Необязательные:
                :param newClientOrderId: позволяет однозначно определить отмену, если не указано, генерируется автоматически
                :param recvWindow: окно валидности запроса.

        :return: словарь.
        """
        kwargs = {'command': 'cancelOrder',
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

    def openOrders(self, symbol: str = None, recvWindow: int = None):
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
        kwargs = {'command': 'openOrders'}
        if symbol:
            kwargs['symbol'] = symbol.upper()
        if recvWindow:
            kwargs['recvWindow'] = recvWindow
        return self.__call_api(**kwargs)

    def allOrders(self, symbol: str, orderId: int = None,
                  limit: int = None, recvWindow: int = None):
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
                  'symbol': symbol.upper()}
        if orderId:
            kwargs['orderId'] = orderId
        if limit:
            kwargs['limit'] = limit
        if recvWindow:
            kwargs['recvWindow'] = recvWindow
        return self.__call_api(**kwargs)

    def account(self, recvWindow: int = None):
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
        kwargs = {'command': 'account'}
        if recvWindow:
            kwargs['recvWindow'] = recvWindow
        return self.__call_api(**kwargs)

    def myTrades(self, symbol: str, limit: int = None,
                 fromId: int = None, recvWindow: int = None):
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
                  'symbol': symbol.upper()}
        if limit:
            kwargs['limit'] = limit
        if fromId:
            kwargs['fromId'] = fromId
        if recvWindow:
            kwargs['recvWindow'] = recvWindow
        return self.__call_api(**kwargs)

    def __call_api(self, **kwargs):
        command = kwargs.pop('command')
        api_url = 'https://api.binance.com/' + self.methods[command]['url']

        payload = kwargs
        headers = {}

        if self.methods[command]['private']:
            payload.update({'timestamp': int(time.time() * 1000)})

            sign = hmac.new(
                key=self.api_secret,
                msg=urllib.parse.urlencode(payload).encode('utf-8'),
                digestmod=hashlib.sha256
            ).hexdigest()

            payload.update({'signature': sign})
            headers = {"X-MBX-APIKEY": self.api_key}

        if self.methods[command]['method'] == 'GET':
            api_url += '?' + urllib.parse.urlencode(payload)

        response = requests.request(method=self.methods[command]['method'], url=api_url,
                                    data="" if self.methods[command]['method'] == 'GET' else payload, headers=headers)
        return response.json()


if __name__ == '__main__':

    def print_list(command_name, lst):
        print('<><><>' + command_name + '<><><>')
        for each in lst:
            print('>>>')
            if isinstance(each, dict):
                for key, value in each.items():
                    print('{}:\t{}'.format(key, value))
            else:
                for value in each:
                    print(value)
        print()


    def print_dict(command_name, dct):
        print('<><><>' + command_name + '<><><>')
        for key, value in dct.items():
            print('{}:\t{}'.format(key, value))
        print()

    from config import API_KEY, API_SECRET

    bot = BinanceApi(API_KEY, API_SECRET)
    print_dict('ping', bot.ping())
    print_dict('time', bot.time())
    print_dict('exchangeInfo', bot.exchangeInfo())
    print_dict('depth', bot.depth('ethbtc', limit=5))
    print_list('trades', bot.trades('ethbtc', limit=5))
    print_list('aggTrades', bot.aggTrades('ethbtc', limit=5))
    print_list('klines', bot.klines('ethbtc', limit=5))
    print_dict('ticker24hr', bot.ticker24hr('ethbtc'))
    print_list('tickerPrice', bot.tickerPrice())
    print_dict('tickerBookTicker', bot.tickerBookTicker(symbol='ethbtc'))
    print_dict('testOrder', bot.testOrder('ethbtc', 'BUY', 'MARKET', 0.5))
    print_dict('orderInfo', bot.orderInfo('ethbtc', 56577459))
    print_dict('cancelOrder', bot.cancelOrder('ethbtc', 56577459))
    print_list('openOrders', bot.openOrders())
    print_list('allOrders', bot.allOrders('ethbtc', limit=5))
    print_dict('account', bot.account())
    print_list('myTrades', bot.myTrades('ethbtc', limit=5))

