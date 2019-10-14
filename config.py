import configparser
import importlib
from decimal import Decimal
from typing import Type
from exchanges.base_exchange import BaseExchange

config = configparser.ConfigParser(
    inline_comment_prefixes = ('#', ';'),
    converters = {'decimal': Decimal}
)
config.read('config.ini')

def get_exchange_class() -> Type[BaseExchange]:
    exchange_name = config.get('Exchange', 'Exchange').lower()
    module = importlib.import_module(f'exchanges.{exchange_name}.{exchange_name}_exchange')
    exchange_class = getattr(module, f'{exchange_name.capitalize()}Exchange')
    return exchange_class
