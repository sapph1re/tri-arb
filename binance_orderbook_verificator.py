import os
import errno
import csv
from datetime import datetime
from typing import List
from PyQt5.QtCore import QTimer, QObject, Qt, pyqtSignal, pyqtSlot
from binance_api import BinanceApi
from binance_depth_websocket import BinanceDepthWebsocket
from binance_orderbook import BinanceOrderBook
from custom_logging import get_logger

logger = get_logger(__name__)


class BinanceOBSnapshot:
    def __init__(self, update_id, symbol, bids, asks):
        self.id = update_id
        self.symbol = symbol
        self.bids = bids
        self.asks = asks


class BinanceOBVerificator(QObject):
    verification_finished = pyqtSignal(str)

    def __init__(self, api: BinanceApi, order_book: BinanceOrderBook, parent=None):
        super(BinanceOBVerificator, self).__init__(parent=parent)
        self.__api = api
        self.__cur_ob = order_book
        self.__symbol = self.__cur_ob.get_symbol()
        self.__snapshots = {}
        self.__new_ob = order_book
        self.__new_ob_too_new_flag = False
        self.__dump_threshold = 75

        self.__directory = './ob_verification/'
        try:
            os.makedirs(self.__directory)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

    def start_verify_ob(self):
        logger.debug('BOBV {} > Verification STARTED!', self.__symbol)
        self.__start_verify_ob()

    def __start_verify_ob(self):
        self.__new_ob_too_new_flag = False
        self.__snapshots = {}

        cur_ob_id = self.__cur_ob.get_update_id()
        cur_ob_bids = self.__cur_ob.get_bids()
        cur_ob_asks = self.__cur_ob.get_asks()
        cur_ob_snapshot = BinanceOBSnapshot(cur_ob_id, self.__symbol, cur_ob_bids, cur_ob_asks)
        self.__snapshots[cur_ob_id] = cur_ob_snapshot

        self.__new_ob = BinanceOrderBook(self.__api, self.__cur_ob.get_base(), self.__cur_ob.get_quote())
        self.__cur_ob.ob_updated.connect(self.update_snapshots_slot)
        self.__new_ob.ob_updated.connect(self.new_ob_inited)
        self.__new_ob.init_order_book()

    @pyqtSlot()
    def update_snapshots_slot(self):
        cur_ob_id = self.__cur_ob.get_update_id()
        cur_ob_bids = self.__cur_ob.get_bids()
        cur_ob_asks = self.__cur_ob.get_asks()
        cur_ob_snapshot = BinanceOBSnapshot(cur_ob_id, self.__symbol, cur_ob_bids, cur_ob_asks)
        self.__snapshots[cur_ob_id] = cur_ob_snapshot
        if self.__new_ob_too_new_flag:
            self.check_new_ob()

    @pyqtSlot()
    def new_ob_inited(self):
        self.__new_ob.ob_updated.disconnect(self.new_ob_inited)
        self.check_new_ob()

    def check_new_ob(self):
        self.__new_ob_too_new_flag = False

        new_ob_id = self.__new_ob.get_update_id()
        cur_ob_id = self.__cur_ob.get_update_id()
        if new_ob_id in self.__snapshots:
            logger.debug('BOBV {} > new_ob_id {} in snapshots!', self.__symbol, new_ob_id)
            self.finish_verification()
        elif new_ob_id > cur_ob_id:
            self.__new_ob_too_new_flag = True
            logger.debug('BOBV {} > new_ob_id {} > {} cur_ob_id!', self.__symbol, new_ob_id, cur_ob_id)
        else:
            # case where new_ob_id < cur_ob_id and not in snapshots
            # we need to reset snapshots and reinit new_ob
            logger.debug('BOBV {} > new_ob_id {} < {} cur_ob_id and not in snapshots! REINIT!', self.__symbol, new_ob_id, cur_ob_id)
            self.__cur_ob.ob_updated.disconnect(self.update_snapshots_slot)
            self.__start_verify_ob()

    def finish_verification(self):
        self.__cur_ob.ob_updated.disconnect(self.update_snapshots_slot)
        logger.debug('BOBV {} > Verification FINISHED!', self.__symbol)

        update_id = self.__new_ob.get_update_id()
        new_bids = self.__new_ob.get_bids()
        new_asks = self.__new_ob.get_asks()
        cur_bids = self.__snapshots[update_id].bids
        cur_asks = self.__snapshots[update_id].asks

        new_bids_len = len(new_bids)
        new_asks_len = len(new_asks)
        cur_bids_len = len(cur_bids)
        cur_asks_len = len(cur_asks)

        min_bids_len = new_bids_len if new_bids_len < cur_bids_len else cur_bids_len
        min_asks_len = new_asks_len if new_asks_len < cur_asks_len else cur_asks_len

        message = '{} id:{} Order Book verification:\n'.format(self.__symbol, update_id)
        message += 'Local bids length > {} <> {} < New bids length\n'.format(cur_bids_len, new_bids_len)
        message += 'Local asks length > {} <> {} < New asks length\n'.format(cur_asks_len, new_asks_len)

        bids_ind = 0
        bids_diff = False
        for bids_ind in range(min_bids_len):
            if new_bids[bids_ind] == cur_bids[bids_ind]:
                continue
            else:
                bids_diff = True
                break
        if bids_diff:
            message += 'Differences in bids start from index = {}'.format(bids_ind)
            if bids_ind <= self.__dump_threshold:
                message += '\tDUMPED'
            message += '\n'
            # new_bids_set = set(new_bids)
            # cur_bids_set = set(cur_bids)
            # diff_cur = cur_bids_set - new_bids_set
            # diff_new = new_bids_set - cur_bids_set
            # diff_cur = ['(' + str(price) + ' , ' + str(quantity) + ')' for price, quantity in diff_cur]
            # diff_new = ['(' + str(price) + ' , ' + str(quantity) + ')' for price, quantity in diff_new]
            # diff_cur_str = ' , '.join(diff_cur)
            # diff_new_str = ' , '.join(diff_new)
            # message += 'Different elements in Local bids but not in New bids: {}\n'.format(diff_cur_str)
            # message += 'Different elements in New bids but not in Local bids: {}\n'.format(diff_new_str)
        else:
            message += 'Bids lists are IDENTICAL\n'

        asks_ind = 0
        asks_diff = False
        for asks_ind in range(min_asks_len):
            if new_asks[asks_ind] == cur_asks[asks_ind]:
                continue
            else:
                asks_diff = True
                break
        if asks_diff:
            message += 'Differences in asks start from index = {}'.format(asks_ind)
            if asks_ind <= self.__dump_threshold:
                message += '\tDUMPED'
            message += '\n'
            # new_asks_set = set(new_asks)
            # cur_asks_set = set(cur_asks)
            # diff_cur = cur_asks_set - new_asks_set
            # diff_new = new_asks_set - cur_asks_set
            # diff_cur = ['(' + str(price) + ' , ' + str(quantity) + ')' for price, quantity in diff_cur]
            # diff_new = ['(' + str(price) + ' , ' + str(quantity) + ')' for price, quantity in diff_new]
            # diff_cur_str = ' , '.join(diff_cur)
            # diff_new_str = ' , '.join(diff_new)
            # message += 'Different elements in Local asks but not in New asks: {}\n'.format(diff_cur_str)
            # message += 'Different elements in New asks but not in Local asks: {}\n'.format(diff_new_str)
        else:
            message += 'Asks lists are IDENTICAL\n'
        # logger.info(message)
        self.__write_stats_log(message)

        if bids_diff and (bids_ind <= self.__dump_threshold):
            for lst, is_bids, is_local in [(cur_bids, True, True),
                                           (new_bids, True, False)]:
                self.__write_list_in_file(update_id, lst, is_bids, is_local)

        if asks_diff and (asks_ind <= self.__dump_threshold):
            for lst, is_bids, is_local in [(cur_asks, False, True),
                                           (new_asks, False, False)]:
                self.__write_list_in_file(update_id, lst, is_bids, is_local)

        self.verification_finished.emit(self.__symbol)

    def __write_stats_log(self, message: str):
        datetime_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S,%f")
        message = datetime_str + ' ### ' + message + '\n'
        filename = self.__directory + 'verification_statistic.txt'
        try:
            with open(filename, mode='a+', encoding='utf-8') as fp:
                fp.write(message)
        except Exception as e:
            logger.exception('BOBV {} > Save stats to TXT failed: {}', self.__symbol, str(e))

    def __write_list_in_file(self, update_id: int, lst: list, is_bids: bool, is_local: bool):
        datetime_str = datetime.utcnow().strftime("%Y%m%d-%H%M%S-%f")
        side_of_list = 'BIDS' if is_bids else 'ASKS'
        type_of_list = 'LOCAL' if is_local else 'NEW'
        filename = self.__directory + '_'.join([datetime_str,
                                                self.__symbol,
                                                str(update_id),
                                                side_of_list,
                                                type_of_list])
        filename += '.csv'
        sep = '|'
        try:
            with open(filename, "w", encoding='utf-8') as fp:
                csv_writer = csv.writer(fp, delimiter=sep, lineterminator='\n')
                csv_writer.writerow(['sep={}'.format(sep)])
                for row in lst:
                    csv_writer.writerow(('="{}"'.format(each) for each in row))
        except Exception as e:
            logger.exception('BOBV {} > Save to CSV failed: {}', self.__symbol, str(e))


class OBVerificationController(QObject):
    verifications_finished = pyqtSignal()

    def __init__(self, api: BinanceApi, ob_list: List[BinanceOrderBook], reverification_time: int = 600, parent=None):
        super(OBVerificationController, self).__init__(parent)

        self.__api = api
        self.__timeout = reverification_time  # in seconds
        self.__symbols = set()
        self.__ver_dict = {}
        for ob in ob_list:
            symbol = ob.get_symbol()
            ver = BinanceOBVerificator(api, ob)
            self.__ver_dict[symbol] = ver

    def start_verifications(self):
        start_time = self.__timeout * 1000
        QTimer.singleShot(start_time, self.start_verifications)

        for symbol, verificator in self.__ver_dict.items():
            self.__symbols.add(symbol)
            verificator.verification_finished.connect(self.update_verificators_state, Qt.QueuedConnection)
            verificator.start_verify_ob()

    @pyqtSlot(str)
    def update_verificators_state(self, symbol: str):
        logger.debug('OBVC > Verificator {} updated his state!', symbol)
        self.__ver_dict[symbol].verification_finished.disconnect(self.update_verificators_state)
        self.__symbols.remove(symbol)
        if not self.__symbols:
            logger.debug('OBVC > All verifications are finished!')
            self.verifications_finished.emit()


def main():
    import sys
    import logging
    from PyQt5.QtCore import QCoreApplication
    from binance_orderbook_scrapper import BinanceOBScrapper
    from config import API_KEY, API_SECRET

    # ob_logger = logging.getLogger('binance_orderbook')
    # ob_logger.setLevel(logging.INFO)

    app = QCoreApplication(sys.argv)

    api = BinanceApi(API_KEY, API_SECRET)

    symbols_set = {('ETH', 'BTC'),
                   ('ETC', 'BTC'),
                   ('EOS', 'BTC'),
                   ('BCD', 'BTC'),
                   ('BNB', 'BTC'),
                   ('BTC', 'USDT'),
                   ('ETH', 'USDT'),
                   ('ETC', 'USDT'),
                   ('EOS', 'USDT'),
                   ('BNB', 'USDT')}

    top_coins = {'BTC', 'ETH', 'XRP', 'ETC', 'EOS'}  # , 'LTC', 'XLM', 'ADA', 'TRX', 'ICX'}
    bot_coins = {'ICN', 'SNGLS', 'OAX', 'MTH', 'STORJ'}  # , 'MOD', 'GRS', 'TNT', 'VIBE', 'RDN'}
    coins_list = top_coins.union(bot_coins)
    quotes_list = {'BTC', 'ETH', 'BNB', 'USDT'}
    all_symbols_dict = api.get_symbols_info()

    symbols_set = set()
    for quote in quotes_list:
        for coin in coins_list:
            if (coin + quote) in all_symbols_dict:
                symbols_set.add((coin, quote))

    minutes = 3  # in minutes
    reverification_time = minutes * 60  # in seconds

    ob_list = []
    sc_list = []
    ws = BinanceDepthWebsocket()
    for base, quote in symbols_set:
        ob = BinanceOrderBook(api, base, quote, ws, reinit_timeout=reverification_time)
        ob_list.append(ob)
        sc = BinanceOBScrapper(api, base, quote, ws, reinit_timeout=5)
        sc_list.append(sc)
        QTimer.singleShot(2000, sc.init_ob)
    vc = OBVerificationController(api, ob_list, reverification_time=reverification_time)
    # vc.verifications_finished.connect(app.quit)

    ws.connect()

    start_time = (reverification_time - 15) * 1000
    QTimer.singleShot(start_time, vc.start_verifications)

    sys.exit(app.exec_())

if __name__ == '__main__':
    main()

    # set1 = {(501, 0.4), (502, 1.2), (503, 0.8), (504, 2.1), (505, 4.3), (506, 5)}
    # set2 = {(501, 0.4), (502, 1.2), (503, 0.8), (504, 2.2), (505, 4.3), (507, 5)}
    # print(set1 - set2)
    # print(set2 - set1)
    # print(set1 ^ set2)

    # tup1 = (501, 0.4)
    # tup2 = (501, 0.4)
    # tup3 = (502, 0.4)
    # tup4 = (501, 0.6)
    # print(tup1 == tup2)
    # print(tup1 == tup3)
    # print(tup1 == tup4)

    # from decimal import Decimal
    # tup = {(Decimal(501), Decimal(0.4)), (Decimal(502), Decimal(0.5))}
    # tup_str = str(tup)
    # print(tup_str)
