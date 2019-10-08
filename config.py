from decimal import Decimal
import configparser

config = configparser.ConfigParser(
    inline_comment_prefixes = ('#', ';'),
    converters = {'decimal': Decimal}
)
config.read('config.ini')
