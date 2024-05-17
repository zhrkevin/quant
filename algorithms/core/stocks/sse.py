#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import akshare as ak
import plotly
from plotly.subplots import make_subplots
from plotly import graph_objs as go
from project.configuration import Config


class ShanghaiStockExchange:

    def __init__(self):
        self.sse_stocks = {}

    def run(self, symbols):
        self.compute(symbols)
        self.plot()

    def compute(self, symbols):
        for symbol in symbols:
            single_stock = ak.stock_zh_a_hist(
                symbol=symbol,
                start_date='20240101',
                end_date='20240430',
            )
            self.sse_stocks |= {symbol: single_stock}
            print(single_stock)

    def plot(self):
        rows = len(self.sse_stocks)
        cols = 1
        vertical = 1 / (rows + 1)
        figures = make_subplots(rows=rows, cols=cols, vertical_spacing=vertical)
        i = 1
        for symbol, data in self.sse_stocks.items():
            trace = go.Candlestick(
                x=data['日期'],
                open=data['开盘'],
                high=data['最高'],
                low=data['最低'],
                close=data['收盘'],
            )
            figures.add_trace(trace, row=i, col=1)
            i += 1

        figures.update_layout(height=1280)

        filename = Config['Paths']['DataPath'] / 'output' / 'plotly.html'
        plotly.offline.plot(figures, auto_open=False, filename=str(filename))

        figures.show()


if __name__ == '__main__':
    sse = ShanghaiStockExchange()
    sse.run(symbols=['000001', '000002'])
