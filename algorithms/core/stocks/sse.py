#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------
# Copyright 2023 for Fosun. All Rights Reserved.
# -----------------------------------------------

import plotly.graph_objs as go
import pandas as pd
import akshare as ak

from project.configuration import Config


class SSE:

    def __init__(self):
        self.sse_summary = pd.DataFrame()
        self.sse_single_stock = pd.DataFrame()

    def summary(self):
        self.sse_summary = ak.stock_sse_summary()

    def run(self):
        code = "000001"
        start = '20220101'
        end = '20230901'
        adjust = ''

        self.sse_single_stock = ak.stock_zh_a_hist(
            symbol=code,
            start_date=start,
            end_date=end,
            adjust=adjust
        )

        print(self.sse_single_stock)

    def plot(self):
        trace = go.Candlestick(
            x=self.sse_single_stock.index,
            open=self.sse_single_stock['开盘'],
            high=self.sse_single_stock['最高'],
            low=self.sse_single_stock['最低'],
            close=self.sse_single_stock['收盘'],
        )
        figure = go.Figure(
            data=trace
        )
        figure.show()

    def dump(self):
        filename = Config['Paths']['DataPath'] / 'input' / 'sse_summary.parquet'
        self.sse_summary.to_parquet(filename)


if __name__ == '__main__':
    sse = SSE()
    sse.run()
    sse.plot()
    sse.dump()
