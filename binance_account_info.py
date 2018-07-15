import json
from decimal import Decimal
from PyQt5.QtCore import QTimer, QObject, pyqtSlot
from binance_api import BinanceApi
from custom_logging import get_logger

logger = get_logger(__name__)


class BinanceTradeFeeException(AttributeError):
    pass


class BinanceAccountInfo(QObject):
    def __init__(self, api: BinanceApi, auto_update_interval: int = 60, parent=None):
        super(BinanceAccountInfo, self).__init__(parent=parent)

        self.__api = api
        self.__can_trade = False
        self.__trade_fee = Decimal('0.001')
        self.__balances = {}

        self.__timer = QTimer()
        self.__timer.setInterval(auto_update_interval * 1000)
        self.__timer.timeout.connect(self.update_info_async)

        self.update_info()

    def can_trade(self) -> bool:
        return self.__can_trade

    def set_auto_update_interval(self, auto_update_interval: int):
        self.__timer.setInterval(auto_update_interval * 1000)
        self.__timer.start()

    def get_trade_fee(self):
        return self.__trade_fee

    def get_all_balances(self) -> dict:
        return self.__balances

    def get_balance(self, asset: str) -> Decimal:
        try:
            return self.__balances[asset]
        except KeyError:
            return Decimal('0')

    def update_info(self):
        self.__timer.start()
        json_data = self.__api.account()
        self.__parse_info_json(json_data)

    @pyqtSlot()
    def update_info_async(self):
        self.__timer.start()
        self.__api.account(slot=self.parse_info)

    @pyqtSlot()
    def parse_info(self):
        try:
            reply = self.sender()
            response = bytes(reply.readAll()).decode("utf-8")
            json_data = json.loads(response)
            self.__parse_info_json(json_data)
        except json.JSONDecodeError:
            logger.error('BAI > JSON Decode FAILED: {}', str(response))
        except BaseException as e:
            logger.exception('BAI > parse_info(): Unknown EXCEPTION: {}', str(e))

    def __parse_info_json(self, json_data):
        try:
            self.__can_trade = json_data['canTrade']

            maker_commission = json_data['makerCommission']
            taker_commission = json_data['takerCommission']
            if maker_commission == taker_commission:
                self.__trade_fee = Decimal(maker_commission) / 100 / 100
            else:
                raise BinanceTradeFeeException('Maker and Taker commissions are different! '
                                               'It can cause wrong calculations and profit loss!')

            self.__balances.clear()
            for each in json_data['balances']:
                asset = each['asset']
                balance = Decimal(each['free'])
                self.__balances[asset] = balance
            logger.debug('BAI > Update OK: {}', str(json_data))
        except KeyError:
            logger.error('BAI > __parse_info_json() KeyError: Wrong data format!')
        except (ValueError, TypeError):
            logger.error('BAI > Could not parse balance for asset: {}', str(asset))
        except BinanceTradeFeeException as e:
            raise e
        except BaseException as e:
            logger.exception('BAI > __parse_info_json(): Unknown EXCEPTION: {}', str(e))


def main():
    import sys
    from PyQt5.QtCore import QCoreApplication
    from config import API_KEY, API_SECRET

    app = QCoreApplication(sys.argv)
    api = BinanceApi(API_KEY, API_SECRET)
    bui = BinanceAccountInfo(api, auto_update_interval=5)
    QTimer.singleShot(0, lambda: print('Trade fee = {}'.format(bui.get_trade_fee())))
    QTimer.singleShot(10 * 1000, app.quit)
    sys.exit(app.exec_())


if __name__ == '__main__':
    main()
