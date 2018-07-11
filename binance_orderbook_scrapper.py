import os
import errno
import json
from datetime import datetime
from PyQt5.QtCore import QTimer, QObject, pyqtSlot
from binance_api import BinanceApi
from binance_depth_websocket import BinanceDepthWebsocket
from binance_orderbook import BinanceOrderBook
from custom_logging import get_logger

logger = get_logger(__name__)


class BinanceOBScrapper(QObject):
    def __init__(self, api: BinanceApi, base: str, quote: str,
                 ws: BinanceDepthWebsocket = None,
                 reinit_timeout: int = 10, parent=None):
        super(BinanceOBScrapper, self).__init__(parent=parent)

        self.__reinit_timeout = reinit_timeout

        self.__ob = BinanceOrderBook(api, base, quote)
        self.__ob.ob_updated.connect(self.__dump_ob_snapshot)

        self.__symbol = self.__ob.get_symbol()

        if ws is None:
            self.__ws = BinanceDepthWebsocket()
            self.__ws.add_symbol(self.__symbol)
            self.__ws.symbol_updated.connect(self.__dump_ws_update)
            self.__ws.connect()
        else:
            self.__ws = ws
            self.__ws.add_symbol(self.__symbol)
            self.__ws.symbol_updated.connect(self.__dump_ws_update)

        self.__base_directory = './ob_scrapper/{}/'.format(self.__symbol)
        self.__updates_directory = self.__base_directory + 'updates/'
        self.__snapshots_directory = self.__base_directory + 'snapshots/'
        try:
            os.makedirs(self.__updates_directory)
            os.makedirs(self.__snapshots_directory)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

    @pyqtSlot()
    def init_ob(self):
        reinit_time = self.__reinit_timeout * 1000
        QTimer.singleShot(reinit_time, self.init_ob)
        self.__ob.init_order_book()

    @pyqtSlot()
    def __dump_ob_snapshot(self):
        timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S-%f")
        update_id = str(self.__ob.get_update_id())

        filename = '_'.join([timestamp,
                             self.__symbol,
                             update_id])
        filename = self.__snapshots_directory + filename + '.json'
        self.__ob.save_to(filename)

    @pyqtSlot(dict)
    def __dump_ws_update(self, update):
        if (not update) or (update['s'] != self.__symbol):
            return

        timestamp = datetime.utcnow().strftime("%Y%m%d-%H%M%S-%f")
        from_id = str(update['U'])
        to_id = str(update['u'])

        filename = '_'.join([timestamp,
                             self.__symbol,
                             from_id,
                             to_id])
        filename = self.__updates_directory + filename + '.json'

        with open(filename, 'w') as fp:
            json.dump(update, fp, indent=2, ensure_ascii=False)


def main():
    import sys
    from PyQt5.QtCore import QCoreApplication
    from config import API_KEY, API_SECRET

    app = QCoreApplication(sys.argv)
    api = BinanceApi(API_KEY, API_SECRET)
    scrapper = BinanceOBScrapper(api, 'EOS', 'USDT')
    QTimer.singleShot(3000, scrapper.init_ob)

    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
