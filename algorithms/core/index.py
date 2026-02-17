#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import pandas as pd
import polars as pl
import akshare as ak
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from project.configuration import Config


pl.Config(tbl_rows=20, tbl_cols=-1)


class ShanghaiStockExchange:

    def __init__(self):
        self.sse_stocks = {}
        self.sse_single_stock = None

    def run(self, symbol, operation):
        if operation == 'read':
            self.read(symbol)
            self.day_mean()
            self.week_mean()
            self.month_mean()
            self.show()
            self.plot()
        elif operation == 'write':
            self.save([symbol])
            self.show()
        else:
            pass

    def download(self, symbol: str='sh600036', start_date: str='20150101', end_date: str='20251231', adjust: str=''):
        self.sse_single_stock = ak.stock_zh_a_daily(
                symbol=symbol,
                start_date=start_date,
                end_date=end_date,
                adjust=adjust,
            )

    def save(self, symbols: list):
        for symbol in symbols:
            self.download(symbol)
            self.sse_single_stock.to_parquet(Config.Paths.DataPath / 'input' / f'{symbol}.parquet')

    def read(self, symbol: str):
        self.sse_single_stock = pd.read_parquet(Config.Paths.DataPath / 'input' / f'{symbol}.parquet', engine='pyarrow')
        self.sse_single_stock['datetime'] = pd.to_datetime(self.sse_single_stock['date'])
        self.sse_single_stock = pl.from_pandas(self.sse_single_stock)

    def day_mean(self):
        self.day_ma = self.sse_single_stock.select(
            [
                pl.col('close').rolling_mean(5).alias('day_ma_5'),
                pl.col('close').rolling_mean(10).alias('day_ma_10'),
                pl.col('close').rolling_mean(20).alias('day_ma_20'),
                pl.col('close').rolling_mean(30).alias('day_ma_30'),
                pl.col('close').rolling_mean(60).alias('day_ma_60'),
            ]
        )

    def week_mean(self):
        # Calculate weekly moving averages using Polars
        self.week_ma = self.sse_single_stock.select(
            [
                pl.col('close').rolling_mean(5).alias('week_ma_1'),  # Approximately 1 week (5 trading days)
                pl.col('close').rolling_mean(10).alias('week_ma_2'),  # Approximately 2 weeks
                pl.col('close').rolling_mean(20).alias('week_ma_4'),  # Approximately 4 weeks
                pl.col('close').rolling_mean(25).alias('week_ma_5'),  # Approximately 5 weeks
                pl.col('close').rolling_mean(50).alias('week_ma_10'),  # Approximately 10 weeks
            ]
        )

    def month_mean(self):
        # Calculate monthly moving averages using Polars
        self.month_ma = self.sse_single_stock.select(
            [
                pl.col('close').rolling_mean(20).alias('month_ma_5'),  # Approximately 1 month (20 trading days)
                pl.col('close').rolling_mean(40).alias('month_ma_10'),  # Approximately 2 months
                pl.col('close').rolling_mean(60).alias('month_ma_15'),  # Approximately 3 months
                pl.col('close').rolling_mean(120).alias('month_ma_30'),  # Approximately 6 months
                pl.col('close').rolling_mean(240).alias('month_ma_60'),  # Approximately 12 months
            ]
        )

    def show(self):
        print(self.sse_single_stock)

    def plot(self):
        stock_data = self.sse_single_stock[:100]
        day_ma_data = self.day_ma[:100]
        week_ma_data = self.week_ma[:100]
        month_ma_data = self.month_ma[:100]

        # 创建索引作为 x 轴，确保连续性
        indices = list(range(len(stock_data)))

        fig = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            subplot_titles=('交易', '成交量'),
            # vertical_spacing=0.01,
            # row_width=[0.2, 0.7]
        )

        fig.add_trace(
            go.Candlestick(x=indices, open=stock_data['open'], high=stock_data['high'], low=stock_data['low'], close=stock_data['close']),
            row=1, col=1
        )
        fig.add_trace(
            go.Bar(x=indices, y=stock_data['volume'], showlegend=False),
            row=2, col=1
        )

        for key, value in {5: 'red', 10: 'blue', 20: 'green', 30: 'orange', 60: 'purple'}.items():        
            fig.add_trace(
                go.Scatter(x=indices, y=day_ma_data[f'day_ma_{key}'], name=f'{key}-Day MA', line={'color': value, 'width': 2}),
                row=1, col=1
            )

        # 只在某些位置显示日期标签，避免过于密集
        tickvals = []
        ticktext = []
        step = max(1, len(stock_data) // 10)  # 最多显示 10 个标签
        for i in range(0, len(stock_data), step):
            tickvals.append(i)
            ticktext.append(stock_data['date'][i])
        
        # 设置 x 轴标签，只显示部分日期
        fig.update_xaxes(
            tickvals=tickvals,
            ticktext=ticktext,
            row=1, col=1
        )
        fig.update_xaxes(
            tickvals=tickvals,
            ticktext=ticktext,
            row=2, col=1
        )

        # Do not show OHLC's rangeslider plot
        fig.update(layout_xaxis_rangeslider_visible=False)

        fig.show()


if __name__ == '__main__':
    # my_chart = Chart(data=[0, 5, 3, 5], series_type='line')
    # as_js_literal = my_chart.to_js_literal()
    # print(as_js_literal)

    sse = ShanghaiStockExchange()
    sse.run(
        'sh600036',
        'read'
    )
