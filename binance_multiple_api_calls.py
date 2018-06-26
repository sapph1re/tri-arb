import sys
import json
import uuid
from typing import List, Dict, Callable

from PyQt5.QtCore import (QObject, pyqtSignal)
from PyQt5.QtNetwork import (QNetworkReply)

from binance_api import BinanceApi
from custom_logging import get_logger

logger = get_logger(__name__)


class BinanceApiCall(QObject):

    __static_id = 0
    update_received = pyqtSignal(int)

    def __init__(self, method: Callable, kwargs: dict = None, parent=None):
        super(BinanceApiCall, self).__init__(parent=parent)

        if not kwargs:
            kwargs = {}

        self.__id = BinanceApiCall.__static_id
        BinanceApiCall.__static_id += 1

        self.__method = method
        self.__kwargs = kwargs

        self.__result = None

    def get_id(self) -> uuid:
        return self.__id

    def get_method(self) -> Callable:
        return self.__method

    def set_method(self, method: Callable):
        self.__method = method

    def get_kwargs(self) -> dict:
        return self.__kwargs

    def set_kwargs(self, kwargs: dict):
        self.__kwargs = kwargs

    def update_result_slot(self):
        reply = self.sender()

        if isinstance(reply, QNetworkReply):
            response = bytes(reply.readAll()).decode("utf-8")
            if response:
                self.__result = json.loads(response)
        else:
            logger.debug('BAC > update_result_slot(): Sender is not QNetworkReply object!'.format(self.__symbol))

        self.update_received.emit(self.__id)

    def get_result(self):
        return self.__result


class BinanceMultipleApiCalls(QObject):

    finished = pyqtSignal(dict)

    def __init__(self, api: BinanceApi, calls_list: List[BinanceApiCall], parent=None):
        super(BinanceMultipleApiCalls, self).__init__(parent=parent)

        self.__api = api
        self.__calls_dict = {}
        self.__calls_flag = {}
        self.__calls_result = {}
        for call in calls_list:
            call_id = call.get_id()
            self.__calls_dict[call_id] = call
            self.__calls_flag[call_id] = False
            self.__calls_result[call_id] = None

        self.__running = False

    def get_api(self) -> BinanceApi:
        return self.__api

    def set_api(self, api: BinanceApi) -> bool:
        if self.__running:
            return False

        self.__api = api
        return True

    def get_calls(self) -> Dict[int, BinanceApiCall]:
        return self.__calls_dict

    def set_calls(self, calls_list: List[BinanceApiCall]) -> bool:
        if self.__running:
            return False

        self.__calls_dict = {}
        self.__calls_flag = {}
        for call in calls_list:
            call_id = call.get_id()
            self.__calls_dict[call_id] = call
            self.__calls_flag[call_id] = False
            self.__calls_result[call_id] = None
        return True

    def append_calls(self, calls_list: List[BinanceApiCall]) -> bool:
        if self.__running:
            return False

        for call in calls_list:
            call_id = call.get_id()
            self.__calls_dict[call_id] = call
            self.__calls_flag[call_id] = False
            self.__calls_result[call_id] = None
        return True

    def remove_calls(self, calls_list: List[BinanceApiCall]) -> bool:
        if self.__running:
            return False

        for call in calls_list:
            call_id = call.get_id()
            self.__calls_dict.pop(call_id)
            self.__calls_flag.pop(call_id)
            self.__calls_result.pop(call_id)
        return True

    def start_calls(self) -> bool:
        if self.__running:
            return False

        for call_id, call in self.__calls_dict.items():
            method = call.get_method()
            kwargs = call.get_kwargs()
            slot = call.update_result_slot
            call.update_received.connect(self.__update_call_slot)
            method(slot=slot, **kwargs)
        return True

    def __update_call_slot(self, call_id: int):
        self.__calls_flag[call_id] = True
        self.__calls_result[call_id] = self.__calls_dict[call_id].get_result()

        flags = [v for _, v in self.__calls_flag.items()]
        if all(flags):
            self.__running = False
            self.finished.emit(self.__calls_result)

    def get_results(self):
        if self.__running:
            return None

        return self.__calls_result


class _SelfTestReceiver:

    @staticmethod
    def update_slot(results: dict):
        for k, v in results.items():
            print('{}\t: {}'.format(k, v))


def _main():
    from PyQt5.QtCore import QCoreApplication, QTimer
    from config import API_KEY, API_SECRET

    app = QCoreApplication(sys.argv)
    bapi = BinanceApi(API_KEY, API_SECRET)
    tr = _SelfTestReceiver()

    bapi_calls_1ist01 = []
    bapi_calls_params01 = [(bapi.time, {}),
                           (bapi.time, {}),
                           (bapi.time, {})]
    for method, kwargs in bapi_calls_params01:
        bapi_call = BinanceApiCall(method, kwargs)
        bapi_calls_1ist01.append(bapi_call)

    bapi_calls_1ist02 = []
    bapi_calls_params02 = [(bapi.depth, {'symbol': 'ethbtc',
                                         'limit': 5}),
                           (bapi.depth, {'symbol': 'xrpbtc',
                                         'limit': 5}),
                           (bapi.depth, {'symbol': 'btcusdt',
                                         'limit': 5})]
    for method, kwargs in bapi_calls_params02:
        bapi_call = BinanceApiCall(method, kwargs)
        bapi_calls_1ist02.append(bapi_call)

    multi_call01 = BinanceMultipleApiCalls(bapi, bapi_calls_1ist01)
    multi_call02 = BinanceMultipleApiCalls(bapi, bapi_calls_1ist02)

    multi_call01.finished.connect(tr.update_slot)
    multi_call02.finished.connect(tr.update_slot)

    QTimer.singleShot(0, multi_call01.start_calls)
    QTimer.singleShot(0, multi_call02.start_calls)
    QTimer.singleShot(5000, app.exit)

    sys.exit(app.exec_())


if __name__ == '__main__':
    _main()
