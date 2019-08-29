from typing import Dict
from itertools import combinations, product

from binance_api import BinanceApi, BinanceSymbolInfo
from config import API_KEY, API_SECRET
from logger import get_logger

logger = get_logger(__name__)


class TrianglesFinder:

    def __init__(self):
        pass

    @staticmethod
    def __find_first_duplicate(lst: list):
        set_ = set()
        for item in lst:
            if item in set_:
                return item
            set_.add(item)
        return None

    def __check_triangle(self, xy, xz, yz) -> bool:
        base_list = [xy[1], xz[1], yz[1]]
        quote_list = [xy[2], xz[2], yz[2]]
        base_duplicate = self.__find_first_duplicate(base_list)
        if not base_duplicate:
            return False
        quote_duplicate = self.__find_first_duplicate(quote_list)
        if not quote_duplicate:
            return False
        if base_duplicate == quote_duplicate:
            return False
        base_last = [item for item in base_list if item != base_duplicate]
        quote_last = [item for item in quote_list if item != quote_duplicate]
        if base_last == quote_last:
            return True
        else:
            return False

    def make_triangles_var3_slow(self, symbols_info: Dict[str, BinanceSymbolInfo]):
        """
        Low performance triangles finder through finding all possibles triplets.
        :param symbols_info: Dict[symbol string, BinanceSymbolInfo class]
        :return: set of tuples of 3 tuples of base and quote assets
                Example: set{
                                ((ETH, BTC), (EOS, BTC), (EOS, ETH)),
                                ((ETH, BTC), (BNB, BTC), (BNB, ETH))
                            }
        """
        symbols = [(k, v.get_base_asset(), v.get_quote_asset()) for k, v in symbols_info.items()]
        triangles = set()
        for triplet in combinations(symbols, 3):
            if self.__check_triangle(triplet[0], triplet[1], triplet[2]):
                # print('{}\t{}\t{}'.format(each[0][0], each[1][0], each[2][0]))
                triangles.add(
                    ((triplet[0][1], triplet[0][2]),
                     (triplet[1][1], triplet[1][2]),
                     (triplet[2][1], triplet[2][2]))
                )
        return triangles

    @staticmethod
    def make_asset_dicts(symbols_info: Dict[str, BinanceSymbolInfo]):
        """
        :param symbols_info: Dict[symbol string, BinanceSymbolInfo class]
        :return: (base_dictionary, quote_dictionary)
        """
        base_dict = {}
        quote_dict = {}
        for k, v in symbols_info.items():
            symbol = k
            base_asset = v.get_base_asset()
            quote_asset = v.get_quote_asset()
            if base_asset not in base_dict:
                base_dict[base_asset] = set()
            if quote_asset not in quote_dict:
                quote_dict[quote_asset] = set()
            base_dict[base_asset].add(symbol)
            quote_dict[quote_asset].add(symbol)
        return base_dict, quote_dict

    def make_triangles(self, symbols_info: Dict[str, BinanceSymbolInfo]):
        """
        Main and the fastest make triangles finder function.
        :param symbols_info: Dict[symbol string, BinanceSymbolInfo class]
        :return: set of tuples of 3 tuples of base and quote assets
                Example: set{
                                ((ETH, BTC), (EOS, BTC), (EOS, ETH)),
                                ((ETH, BTC), (BNB, BTC), (BNB, ETH))
                            }
        """
        base_dict, quote_dict = self.make_asset_dicts(symbols_info)
        triangles = set()

        for k, v in quote_dict.items():
            quote = k
            quote_len = len(quote)
            combs = combinations(v, 2)

            for a, b in combs:
                base1 = a[:-quote_len]
                base2 = b[:-quote_len]
                if (base1 not in quote_dict) and (base2 not in quote_dict):
                    continue

                c1 = base2 + base1
                c2 = base1 + base2

                if ((base1 in quote_dict) and (base2 in base_dict) and
                        (c1 in quote_dict[base1]) and (c1 in base_dict[base2])):
                    triangles.add(
                        ((base1, quote),
                         (base2, quote),
                         (base2, base1))
                    )
                elif ((base2 in quote_dict) and (base1 in base_dict) and
                      (c2 in quote_dict[base2]) and (c2 in base_dict[base1])):
                    triangles.add(
                        ((base2, quote),
                         (base1, quote),
                         (base1, base2))
                    )
        return triangles

    def make_triangles_var2(self, symbols_info: Dict[str, BinanceSymbolInfo]):
        """
        Secondary and and fast enough triangles finder function.
        :param symbols_info: Dict[symbol string, BinanceSymbolInfo class]
        :return: set of tuples of 3 tuples of base and quote assets
                Example: set{
                                ((ETH, BTC), (EOS, BTC), (EOS, ETH)),
                                ((ETH, BTC), (BNB, BTC), (BNB, ETH))
                            }
        """
        base_dict, quote_dict = self.make_asset_dicts(symbols_info)
        triangles = set()

        for k, v in quote_dict.items():
            quote = k
            quote_len = len(quote)
            combs = product(v, repeat=2)

            for a, b in combs:
                base1 = a[:-quote_len]
                base2 = b[:-quote_len]
                if (base1 not in quote_dict) and (base2 not in quote_dict):
                    continue

                c = base1 + base2

                if ((base2 in quote_dict) and (base1 in base_dict) and
                        (c in quote_dict[base2]) and (c in base_dict[base1])):
                    triangles.add(
                        ((base2, quote),
                         (base1, quote),
                         (base1, base2))
                    )
        return triangles

    @staticmethod
    def triangles_verification(in_list: list) -> bool:
        """
        Check all triangles sets in list.
        :param in_list: List of sets of tuples of 3 tuples of base and quote assets
                Example:
                [
                    {
                        ((ETH, BTC), (EOS, BTC), (EOS, ETH)),
                        ((ETH, BTC), (BNB, BTC), (BNB, ETH))
                    },
                    {
                        ((ETH, BTC), (EOS, BTC), (EOS, ETH)),
                        ((ETH, BTC), (BNB, BTC), (BNB, ETH))
                    }
                ]
        :return: True if all of them are the same or False if else
        """
        triangles_list = []
        for each in in_list:
            symbols_list = [(item[0][0] + item[0][1],
                             item[1][0] + item[1][1],
                             item[2][0] + item[2][1])
                            for item in each]
            triangles_list.append(symbols_list)
        sorted_triangles_list = []
        for triangles in triangles_list:
            sorted_triangles = set()
            for each in triangles:
                lst = list(each)
                srt = sorted(lst)
                tpl = tuple(srt)
                sorted_triangles.add(tpl)
            sorted_triangles = sorted(list(sorted_triangles))
            sorted_triangles_list.append(sorted_triangles)

        files_list = [open('triangles_verification' + str(i + 1).zfill(2) + '.txt', 'w')
                      for i in range(len(sorted_triangles_list))]

        for file, sorted_triangles in zip(files_list, sorted_triangles_list):
            for triangle in sorted_triangles:
                file.write('\t'.join(triangle) + '\n')

        for file in files_list:
            file.close()

        len_str = ''
        for sorted_triangles in sorted_triangles_list:
            len_str += '{}\t'.format(len(sorted_triangles))

        if all(sorted_triangles_list[0] == each for each in sorted_triangles_list):
            logger.info('TF > SUCCESS: All triangles sets are the same!')
            return True
        else:
            logger.info('TF > FAILED: Triangles sets are different! :(')
            return False

    @staticmethod
    def save_triangles_set_to(filename: str, triangles_set: set):
        content = ''
        for triangle in triangles_set:
            for symbol in triangle:
                content += symbol[0] + symbol[1] + '\t'
            content += '\n'
        with open(filename, 'w') as fs:
            fs.write(content)

    def _order_symbols_in_triangles(self, triangles_set: set):
        """
        Performance test function. Skip it.
        :param triangles_set:
        :return:
        """
        ordered_triangles_set = set()
        for triangle in triangles_set:
            ordered_triangle = self._order_symbols_in_tuple(triangle)
            ordered_triangles_set.add(ordered_triangle)
        return ordered_triangles_set

    @staticmethod
    def _order_symbols_in_tuple(triple: tuple):
        """
        Performance test function. Skip it.
        :param triple:
        :return:
        """
        # yz, xz, xy
        a, b, c = triple

        if a[1] == c[1]:
            b, c = c, b
        elif b[1] == c[1]:
            a, b, c = b, c, a

        if a[0] != c[0]:
            a, b = b, a

        return a, b, c


if __name__ == '__main__':
    api = BinanceApi(API_KEY, API_SECRET)
    symbols_info = api.get_symbols_info()

    print('>> Symbols count = {}'.format(len(symbols_info)))
    print()

    tf = TrianglesFinder()
    base_dict, quote_dict = tf.make_asset_dicts(symbols_info)

    print('>> Size of base_dict = {}'.format(len(base_dict)))
    for k, v in base_dict.items():
        print('{} : {}'.format(k, v))
    print()

    print('>> Size of quote_dict = {}'.format(len(quote_dict)))
    for k, v in quote_dict.items():
        print('{} : {}'.format(k, v))
    print()

    triangles1 = tf.make_triangles(symbols_info)
    triangles2 = tf.make_triangles_var2(symbols_info)
    # triangles3 = tf.make_triangles_var3(symbols_info)

    tf.triangles_verification([triangles1, triangles2])

    ordered_triangles = tf._order_symbols_in_triangles(triangles1)
    tf.save_triangles_set_to('tri_arb_01.txt', ordered_triangles)