#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import akshare as ak
from lightweight_charts import Chart


class ShanghaiStockExchange:

    def __init__(self):
        self.sse_stocks = {}

    def run(self, symbols: list):
        self.compute(symbols)
        self.plot()

    def compute(self, symbols: list):
        for symbol in symbols:
            temp_stock = ak.stock_zh_a_hist(
                symbol=symbol,
                start_date='20190101',
                end_date='20240430',
            )
            single_stock = temp_stock[['日期', '开盘', '最高', '最低', '收盘', '成交量']]
            single_stock = single_stock.sort_values(by='日期', ascending=True)
            single_stock.rename(
                columns={
                    '日期': 'date',
                    '开盘': 'open',
                    '最高': 'high',
                    '最低': 'low',
                    '收盘': 'close',
                    '成交量': 'volume'
                },
                inplace=True
            )
            self.sse_stocks |= {symbol: single_stock}

    def plot(self):
        for symbol, data in self.sse_stocks.items():
            chart = Chart()
            chart.set(data)
            chart.show(block=True)


if __name__ == '__main__':
    sse = ShanghaiStockExchange()
    sse.run(
        symbols=['000001', '000002']
    )
