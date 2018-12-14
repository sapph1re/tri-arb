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
        self.pair = pair
        self.base = pair[0]
        self.quote = pair[1]
        self.symbol = (pair[0]+pair[1]).upper()
        self.side = side.upper()
        self.quantity = quantity
        self.price = price
        self.type = order_type.upper()
        self.timeInForce = timeInForce.upper()
        self.newClientOrderId = newClientOrderId

    def __str__(self):
        return '{} {} {} {}/{} @ {} ({})'.format(
            self.type, self.side, self.quantity,
            self.base, self.quote, self.price, self.timeInForce
        )

    def __repr__(self):
        return self.__str__()


class BinanceActionsExecutor(QThread):

    action_executed = pyqtSignal()
    execution_finished = pyqtSignal()

    def __init__(self, api_key: str, api_secret: str, actions_list: List[BinanceSingleAction],
                 account_info: BinanceAccountInfo = None, parent=None):
        super(BinanceActionsExecutor, self).__init__(parent=parent)

        self.__api = BinanceApi(api_key, api_secret)
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
        actions_list = self.__get_executable_actions_list()
        logger.info('Executable actions list: {}', actions_list)

        # actions list is required to be exactly 3 actions for now
        actions_length = len(actions_list)
        if actions_length == 0:
            logger.info('Cannot execute those actions')
            self.execution_finished.emit()
            return
        if actions_length != 3:
            logger.error('Bad actions list: {}', actions_list)
            self.execution_finished.emit()
            return
        for i in range(actions_length):
            action = actions_list[i]
            logger.info('Executing action: {}...', action)
            reply_json = self.__try_create_order_three_times(action)

            if self.__is_order_filled(reply_json):
                logger.info('Action completed')
                continue

            # if order is not filled, execute emergency actions
            logger.info('Order is not filled')
            break
        else:
            self.execution_finished.emit()
            return

        # emergency actions in case of any failures
        emergency_actions = []
        if i == 0:
            # first action failed: nothing to revert
            logger.info('Failed to execute the actions list')
        elif i == 1:
            # second action failed: revert first action
            action = actions_list[i]
            logger.info('Reverting first action: {}', action)
            emergency_actions.append(
                BinanceSingleAction(
                    pair=action.pair,
                    side='BUY' if action.side == 'SELL' else 'SELL',
                    quantity=action.quantity,
                    order_type='MARKET'
                )
            )
        elif i == 2:
            # third action failed: complete third action as a market order
            action = actions_list[i]
            logger.info('Finalizing last action: {}, as a market order', action)
            emergency_actions.append(
                BinanceSingleAction(
                    pair=action.pair,
                    side=action.side,
                    quantity=action.quantity,
                    order_type='MARKET'
                )
            )

        for action in emergency_actions:
            logger.info('Executing emergency action: {}...', action)
            reply_json = self.__try_create_order_three_times(action)
            if self.__is_order_filled(reply_json):
                logger.info('Action completed')
                continue
            else:
                # TODO: Подумать: а какие варианты можно ещё придумать, если маркет ордер фейлится...
                logger.error('BAE {} > Continue arbitrage as market orders FAILED: {}', str(self), str(reply_json))
                break

        self.execution_finished.emit()
        logger.info('Executor finished')

    def __get_executable_actions_list(self) -> List[BinanceSingleAction]:
        actions = self.__actions_list
        logger.debug('Initial actions list: {}', actions)
        # actions list is expected to be exactly three items long, in a triangle
        if len(actions) != 3:
            logger.error('Number of actions is not 3: {}', actions)
            return []
        # first rearrange actions in a sequence to pass funds along the sequence
        gain = []
        spend = []
        for action in actions:
            if action.side == 'BUY':
                gain.append(action.base)
                spend.append(action.quote)
            else:
                gain.append(action.quote)
                spend.append(action.base)
        if gain[0] == spend[1] and gain[1] == spend[2] and gain[2] == spend[0]:
            # sequence is already fine
            pass
        elif gain[0] == spend[2] and gain[2] == spend[1] and gain[1] == spend[0]:
            # sequence needs to be rearranged
            actions = [actions[0], actions[2], actions[1]]
        else:
            logger.error('Bad actions list: not a valid triangle! Actions: {}', actions)
            return []
        logger.debug('Sequenced actions list: {}', actions)
        # then figure out which action to start with and rotate the sequence
        shift = 0
        for action in actions:
            side = action.side
            base = action.base
            quote = action.quote
            quantity = action.quantity
            price = action.price

            if side == 'BUY':
                asset = quote
                amount = quantity * price
            else:
                asset = base
                amount = quantity
            balance = self.__account_info.get_balance(asset)
            logger.debug('{} balance: {}', asset, balance)
            if balance < amount:
                shift -= 1
            else:
                dq = deque(actions)
                logger.debug('Rotating actions list by: {}', shift)
                dq.rotate(shift)
                return list(dq)
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
