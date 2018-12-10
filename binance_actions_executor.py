from typing import List, Tuple
from collections import deque
from PyQt5.QtCore import QThread, pyqtSignal
from binance_api import BinanceApi
from binance_account_info import BinanceAccountInfo
from custom_logging import get_logger


logger = get_logger(__name__)


class BinanceActionException(Exception):
    pass


class BinanceSingleAction:

    def __init__(self, pair: Tuple[str, str], side: str, quantity, price,
                 order_type='LIMIT', timeInForce: str = 'FOK', newClientOrderId: str = None):
        """
        :param base: EOS/USDT -> BTC
        :param quote: EOS/USDT -> USDT
        :param symbol: пара (т.е. 'BTCUSDT', 'ETHBTC', 'EOSETH')
        :param side: тип ордера (BUY либо SELL)
        :param quantity: количество к покупке
        :param price: цена
        :param order_type: тип ордера (LIMIT, MARKET, STOP_LOSS, STOP_LOSS_LIMIT,
                    TAKE_PROFIT, TAKE_PROFIT_LIMIT, LIMIT_MAKER)
        :param timeInForce: (GTC, IOC, FOK). По умолчанию GTC. Расшифрую.
                    GTC (Good Till Cancelled) – ордер будет висеть до тех пор, пока его не отменят.
                    IOC (Immediate Or Cancel) – Будет куплено то количество, которое можно купить немедленно.
                        Все, что не удалось купить, будет отменено.
                    FOK (Fill-Or-Kill) – Либо будет куплено все указанное количество немедленно,
                        либо не будет куплено вообще ничего, ордер отменится.
        :param newClientOrderId: Идентификатор ордера, который вы сами придумаете (строка).
                    Если не указан, генерится автоматически.
        """
        self.base = pair[0]
        self.quote = pair[1]
        self.symbol = (pair[0]+pair[1]).upper()
        self.side = side.upper()
        self.quantity = quantity
        self.price = price
        self.type = order_type.upper()
        self.timeInForce = timeInForce.upper()
        self.newClientOrderId = newClientOrderId


class BinanceActionsExecutor(QThread):

    action_executed = pyqtSignal()

    def __init__(self, api: BinanceApi, actions_list: List[BinanceSingleAction],
                 account_info: BinanceAccountInfo = None, parent=None):
        super(BinanceActionsExecutor, self).__init__(parent=parent)

        self.__api = api
        self.__actions_list = actions_list

        self.__account_info = account_info if account_info is not None else BinanceAccountInfo(self.__api)
        self.action_executed.connect(self.__account_info.update_info_async)

        self.__pretty_str = ''
        self.__set_pretty_str(actions_list)

    def __str__(self):
        return self.__pretty_str

    def __set_pretty_str(self, actions_list: List[BinanceSingleAction]):
        self.__pretty_str = ' -> '.join([action.side + ': ' + action.symbol for action in actions_list])

    def get_actions_list(self) -> List[BinanceSingleAction]:
        return self.__actions_list

    def set_actions_list(self, actions_list: List[BinanceSingleAction]):
        self.__actions_list = actions_list
        self.__set_pretty_str(actions_list)

    def run(self):
        logger.info('Executor starting...')
        revert_flag = False
        actions_list = self.__get_executable_actions_list()
        logger.info('Executable actions list: {}', actions_list)

        actions_length = len(actions_list)
        for i in range(actions_length):
            action = actions_list[i]
            logger.info('Executing action: {}...', action)
            reply_json = self.__try_create_order_three_times(action)

            if self.__is_order_filled(reply_json):
                logger.info('Action completed')
                continue

            logger.info('Order is not filled')
            if i <= (actions_length // 2):
                logger.info('Reverting...')
                revert_flag = True
            break
        else:
            return

        start_index = i
        end_index = actions_length
        step = 1
        revert_side = False
        if revert_flag:  # if not revert it continue to execute triangle as market orders
            end_index = -1
            step = -1
            revert_side = True

        for i in range(start_index, end_index, step):
            action = actions_list[i]
            action.type = 'MARKET'
            if revert_side:
                action.side = 'BUY' if action.side == 'SELL' else 'SELL'
            logger.info('Executing emergency action: {}...', action)
            reply_json = self.__try_create_order_three_times(action)
            if self.__is_order_filled(reply_json):
                logger.info('Action completed')
                continue
            else:
                # TODO: Подумать: а какие варианты можно ещё придумать, если маркет ордер фейлится...
                logger.error('BAE {} > Continue arbitrage as market orders FAILED: {}', str(self), str(reply_json))
                break

        logger.info('Executor finished')

    def __get_executable_actions_list(self) -> List[BinanceSingleAction]:
        shift = 0
        actions_list = self.__actions_list
        for action in actions_list:
            side = action.side
            base = action.base
            quote = action.quote
            quantity = action.quantity
            price = action.price

            asset = quote if side == 'BUY' else base
            amount = quantity * price
            balance = self.__account_info.get_balance(asset)
            if balance < amount:
                shift -= 1
            else:
                dq = deque(actions_list)
                return dq.rotate(shift)
        return []

    def __try_create_order_three_times(self, action: BinanceSingleAction):
        reply_json = None
        repeat_counter = 0
        while ((not reply_json) or ('status' not in reply_json)) and (repeat_counter < 3):
            reply_json = self.__api.createOrder(action.symbol, action.side, action.type, action.quantity,
                                                action.timeInForce, action.price, action.newClientOrderId)
            repeat_counter += 1
        if reply_json:
            return reply_json
        else:
            return None

    def __is_order_filled(self, reply_json) -> bool:
        if reply_json and ('status' in reply_json):
            status = reply_json['status']
            if status == 'NEW':
                cur_symbol = reply_json['symbol']
                cur_order_id = reply_json['orderId']
                status = self.__check_new_order_status(cur_symbol, cur_order_id)
            if status == 'FILLED':
                self.action_executed.emit()
                return True
        return False

    def __check_new_order_status(self, symbol: str, order_id) -> str or None:
        reply_json = self.__api.orderInfo(symbol, order_id)
        if reply_json and ('status' in reply_json):
                status = reply_json['status']
                if status != 'NEW':
                    return status
                else:
                    self.msleep(100)
                    self.__check_new_order_status(symbol, order_id)
        else:
            logger.error('BAE {} > Check order status FAILED: {}', str(self), str(reply_json))
            return None


def main():
    pass


if __name__ == '__main__':
    main()
