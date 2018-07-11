import os
import errno
import json
import csv
from datetime import datetime
from typing import List
from PyQt5.QtCore import QThread, Qt, pyqtSignal
from binance_api import BinanceApi
from binance_orderbook import BinanceOrderBook
from custom_logging import get_logger

logger = get_logger(__name__)


class BinanceOBUpdateInterval:

    def __init__(self, symbol: str, start_id: int, end_id: int, json_dict):
        self.symbol = symbol
        self.start_id = start_id
        self.end_id = end_id
        self.json_dict = json_dict


class BinanceOBDebugger(QThread):

    symbol_update = pyqtSignal(dict)
    debug_finished = pyqtSignal()

    def __init__(self, api: BinanceApi, base: str, quote: str,
                 start_id: int, end_id: int, symbol_dir: str = 'ob_scrapper/',
                 parent=None):
        super(BinanceOBDebugger, self).__init__(parent=parent)

        start_ob = BinanceOrderBook(api, base, quote)
        self.__start_ob = self.load_order_book(start_ob, start_id, symbol_dir)
        self.symbol_update.connect(self.__start_ob.update_orderbook, Qt.DirectConnection)

        end_ob = BinanceOrderBook(api, base, quote)
        self.__end_ob = self.load_order_book(end_ob, end_id, symbol_dir)

        symbol = end_ob.get_symbol()
        updates_intervals = self.load_updates(symbol_dir, symbol)
        self.__updates_interval = self.find_needed_interval(updates_intervals, start_id, end_id)

    def run(self):
        updates_dict = self.__updates_interval.json_dict
        sorted_updates = [updates_dict[key] for key in sorted(updates_dict.keys())]
        for update in sorted_updates:
            self.symbol_update.emit(update)
            start_ob_id = self.__start_ob.get_update_id()
            end_ob_id = self.__end_ob.get_update_id()
            if start_ob_id == end_ob_id:
                break
        self.__save_order_books()
        self.debug_finished.emit()

    def __save_order_books(self):
        update_id = self.__end_ob.get_update_id()
        symbol = self.__end_ob.get_symbol()
        cur_bids = self.__start_ob.get_bids()
        cur_asks = self.__start_ob.get_asks()
        new_bids = self.__end_ob.get_bids()
        new_asks = self.__end_ob.get_asks()

        save_dir = './ob_debugger/'
        try:
            os.makedirs(save_dir)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

        for lst, is_bids, is_local in [(cur_bids, True, True),
                                       (new_bids, True, False),
                                       (cur_asks, False, True),
                                       (new_asks, False, False)]:
            self.__write_list_in_file(update_id, symbol, lst,
                                      is_bids, is_local, save_dir)

    @staticmethod
    def __write_list_in_file(update_id: int, symbol: str, lst: list, is_bids: bool, is_local: bool, save_dir: str):
        datetime_str = datetime.utcnow().strftime("%Y%m%d-%H%M%S-%f")
        side_of_list = 'BIDS' if is_bids else 'ASKS'
        type_of_list = 'LOCAL' if is_local else 'NEW'
        filename = save_dir + '_'.join([datetime_str,
                                        symbol,
                                        str(update_id),
                                        side_of_list,
                                        type_of_list])
        filename += '.csv'
        sep = '|'
        with open(filename, "w", encoding='utf-8') as fp:
            csv_writer = csv.writer(fp, delimiter=sep, lineterminator='\n')
            csv_writer.writerow(['sep={}'.format(sep)])
            for row in lst:
                csv_writer.writerow(('="{}"'.format(each) for each in row))


    @staticmethod
    def load_order_book(ob: BinanceOrderBook, update_id: int, symbol_dir: str) -> BinanceOrderBook:
        symbol = ob.get_symbol()
        path = '{}{}/snapshots/'.format(symbol_dir, symbol)
        files_list = os.listdir(path)
        files_list = [path + f for f in files_list if os.path.isfile(path + f)]
        for file in files_list:
            with open(file) as f:
                data = json.load(f)
                if data['lastUpdateId'] == update_id:
                    ob.test_load_snapshot(file)
                    return ob
        raise FileExistsError('No snapshot was found for id: {}', update_id)

    @staticmethod
    def load_updates(symbol_dir: str, symbol: str) -> list:
        path = '{}{}/updates/'.format(symbol_dir, symbol)
        files_list = os.listdir(path)
        files_list = [path + f for f in files_list if os.path.isfile(path + f)]

        updates_intervals = []

        start_id = -1
        end_id = -1
        json_dict = {}
        for file in files_list:
            if not json_dict:
                with open(file) as f:
                    data = json.load(f)
                start_id = data['U']
                end_id = data['u']
                json_dict[end_id] = data
            else:
                try:
                    with open(file) as f:
                        data = json.load(f)
                except json.JSONDecodeError as e:
                    logger.exception("BOBD {} > Couldn't decode file: {}", symbol, file)
                    raise e
                if (end_id + 1) == data['U']:
                    end_id = data['u']
                    json_dict[end_id] = data
                else:
                    interval = BinanceOBUpdateInterval(symbol, start_id,
                                                       end_id, json_dict.copy())
                    updates_intervals.append(interval)
                    json_dict.clear()
                    start_id = data['U']
                    end_id = data['u']
                    json_dict[end_id] = data
        else:
            interval = BinanceOBUpdateInterval(symbol, start_id, end_id,
                                               json_dict.copy())
            updates_intervals.append(interval)
        return updates_intervals

    @staticmethod
    def find_needed_interval(intervals_list: List[BinanceOBUpdateInterval], start_id: int,
                             end_id: int) -> BinanceOBUpdateInterval:
        for interval in intervals_list:
            if ((interval.start_id <= start_id <= interval.end_id)
                    and (interval.start_id <= end_id <= interval.end_id)
                    and (end_id in interval.json_dict)):
                return interval
        raise IndexError('No matching interval found for ids: {} -> {}', start_id, end_id)


def main():
    import sys
    from PyQt5.QtCore import QCoreApplication, QTimer
    from binance_api import BinanceApi
    from config import API_KEY, API_SECRET

    app = QCoreApplication(sys.argv)
    api = BinanceApi(API_KEY, API_SECRET)

    # Check that you have both snapshots for given IDs and appropriate update interval
    # Especially check that you have update that ends with same ID as "end_id" snapshot (not between)
    # BinanceOBDebugger (BOBD) will check it but if you have a good updates interval
    #  and BOBD says that interval is bad...
    # That means there is no update which "to_id" matches "end_id" of snapshot.
    ob_debugger = BinanceOBDebugger(api, 'ETH', 'BTC', 266198434, 266200409)
    ob_debugger.debug_finished.connect(app.quit)
    QTimer.singleShot(0, ob_debugger.start)

    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
