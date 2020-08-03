# -*- coding: utf-8 -*-

import os
import time
from operator import itemgetter
from threading import Thread


def time_track(func):
    def surrogate(*args, **kwargs):
        started_at = time.time()
        result = func(*args, **kwargs)
        ended_at = time.time()
        elapsed = round(ended_at - started_at, 4)
        print(f'Функция работала {elapsed} секунд(ы)')
        return result
    return surrogate


class ReadFile(Thread):
    def __init__(self, file, dirpath, trades, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file = file
        self.dirpath = dirpath
        self.secid_dick = trades
        self.price_list = []

    def run(self):
        with open(os.path.join(self.dirpath, self.file), mode='r') as ff:
            for i, line in enumerate(ff):
                if i == 0:
                    pass
                else:
                    secid, tradetime, price, quantity = line.split(',')
                    self.price_list.append(float(price))
        self.secid_dick[secid] = self.calc()

    def calc(self):
        average_price = (max(self.price_list) + min(self.price_list)) / 2
        volatility = round(
            ((max(self.price_list) - min(self.price_list)) / average_price) * 100, 2)
        return volatility


class Trades(Thread):

    def __init__(self, path, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.path = path
        self.threads = []
        self.volatility = {}
        self.zero_volatility = []

    def run(self):
        self.file_list()
        for thread in self.threads:
            thread.start()
        for thread in self.threads:
            thread.join()
        for key in self.volatility.keys():
            if self.volatility[key] == 0:
                self.zero_volatility.append(key)
        for _ in self.zero_volatility:
            del self.volatility[_]
        self.print()

    def file_list(self):
        self.path = os.path.normpath(self.path)
        for dirpath, dirnames, filenames in os.walk(self.path):
            for file in filenames:
                if not file.startswith('.'):
                    thread = ReadFile(file=file, dirpath=dirpath, trades=self.volatility)
                    self.threads.append(thread)

    def print(self):
        print('Максимальная волатильность:')
        count = 0
        for k, i in sorted(self.volatility.items(), key=itemgetter(1), reverse=True):
            print(f'{k} - {i}')
            count += 1
            if count == 3:
                break
        print('Минимальная волатильность:')
        count = 0
        for k, i in sorted(self.volatility.items(), key=itemgetter(1), reverse=False):
            print(f'{k} - {i}')
            count += 1
            if count == 3:
                break
        print('Нулевая волатильность:')
        print(','.join(sorted(self.zero_volatility)))


@time_track
def main():
    trades = Trades(path='trades')
    trades.start()
    trades.join()


if __name__ == '__main__':
    main()
