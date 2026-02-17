#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import polars as pl
import akshare as ak
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from project.configuration import Config


pl.Config(tbl_rows=12, tbl_cols=-1)


class WriteStockExchange:

    def __init__(self, symbol: str):
        """初始化 WriteStockExchange 类
        Args:
            symbol: 股票代码
        """
        self.raw_data = None
        self.save([symbol])

    def save(self, symbols: list):
        """保存股票数据到 parquet 文件
        Args:
            symbols: 股票代码列表
        """
        for symbol in symbols:
            self.download(symbol)

    def download(self, symbol: str = 'sh600036', start_date: str = '20150101', end_date: str = '20251231'):
        """从 akshare 下载股票数据并合并
        Args:
            symbol: 股票代码
            start_date: 开始日期
            end_date: 结束日期
        """
        raw_data = pl.from_pandas(
            ak.stock_zh_a_daily(symbol=symbol, start_date=start_date, end_date=end_date, adjust='')
        )
        qfq_data = pl.from_pandas(
            ak.stock_zh_a_daily(symbol=symbol, start_date=start_date, end_date=end_date, adjust='qfq')
        )
        hfq_data = pl.from_pandas(
            ak.stock_zh_a_daily(symbol=symbol, start_date=start_date, end_date=end_date, adjust='hfq')
        )
        
        # 合并三种复权类型的数据
        self.raw_data = pl.DataFrame(
            {
                'date': raw_data['date'],
                'raw_open': raw_data['open'], 'raw_high': raw_data['high'], 'raw_low': raw_data['low'], 'raw_close': raw_data['close'],
                'qfq_open': qfq_data['open'], 'qfq_high': qfq_data['high'], 'qfq_low': qfq_data['low'], 'qfq_close': qfq_data['close'],
                'hfq_open': hfq_data['open'], 'hfq_high': hfq_data['high'], 'hfq_low': hfq_data['low'], 'hfq_close': hfq_data['close'],
                'volume': raw_data['volume'],
                'amount': raw_data['amount'],
                'turnover': raw_data['turnover'],
            }
        )
        self.raw_data.write_parquet(Config.Paths.DataPath / 'input' / symbol / 'raw.parquet')


class ReadStockExchange:
    """上海证券交易所周股票数据管理类"""

    def __init__(self, symbol: str, period: str = 'day'):
        self.daily_data = None
        self.weekly_data = None
        self.monthly_data = None
        
        self.read(symbol)
        self.plot(period)

    def read(self, symbol: str):
        """从 parquet 文件读取合并的股票数据
        Args:
            symbol: 股票代码
        """

        # 按周统计数据
        self.daily_data = pl.read_parquet(Config.Paths.DataPath / 'input' / symbol / 'raw.parquet')
        self.daily_data = self.daily_data.with_columns(
            pl.col('date').dt.year().alias('year'),
            pl.col('date').dt.ordinal_day().alias('day'),
            pl.when(pl.col('raw_close') >= pl.col('raw_open')).then(pl.lit('green')).otherwise(pl.lit('red')).alias('color')
        )
        self.daily_data.write_parquet(Config.Paths.DataPath / 'input' / symbol / 'stock_day.parquet')

        # 按周统计数据
        self.weekly_data = self.daily_data.group_by(
            pl.col('date').dt.year().alias('year'),
            pl.col('date').dt.week().alias('week')
        ).agg(
            [
                pl.col('date').first(),
                pl.col('raw_open').first(), pl.col('raw_high').max(), pl.col('raw_low').min(), pl.col('raw_close').last(),
                pl.col('qfq_open').first(), pl.col('qfq_high').max(), pl.col('qfq_low').min(), pl.col('qfq_close').last(),
                pl.col('hfq_open').first(), pl.col('hfq_high').max(), pl.col('hfq_low').min(), pl.col('hfq_close').last(),
                pl.col('volume').sum().alias('volume'),
                pl.col('amount').sum().alias('amount'),
                pl.col('turnover').sum().alias('turnover'),
                pl.when(pl.col('raw_close').last() >= pl.col('raw_open').first()).then(pl.lit('green')).otherwise(pl.lit('red')).alias('color'),
            ]
        ).sort('date')
        self.weekly_data.write_parquet(Config.Paths.DataPath / 'input' / symbol / 'stock_week.parquet')
        
        # 按月统计数据
        self.monthly_data = self.daily_data.group_by(
            pl.col('date').dt.year().alias('year'),
            pl.col('date').dt.month().alias('month'),
        ).agg(
            [
                pl.col('date').first(),
                pl.col('raw_open').first(), pl.col('raw_high').max(), pl.col('raw_low').min(), pl.col('raw_close').last(),
                pl.col('qfq_open').first(), pl.col('qfq_high').max(), pl.col('qfq_low').min(), pl.col('qfq_close').last(),
                pl.col('hfq_open').first(), pl.col('hfq_high').max(), pl.col('hfq_low').min(), pl.col('hfq_close').last(),
                pl.col('volume').sum().alias('volume'),
                pl.col('amount').sum().alias('amount'),
                pl.col('turnover').sum().alias('turnover'),
                pl.when(pl.col('raw_close').last() >= pl.col('raw_open').first()).then(pl.lit('green')).otherwise(pl.lit('red')).alias('color'),
            ]
        ).sort('date')
        self.monthly_data.write_parquet(Config.Paths.DataPath / 'input' / symbol / 'stock_month.parquet')

        print(self.daily_data)
        print(self.weekly_data)
        print(self.monthly_data)

    def plot(self, period: str = 'day'):
        """绘制股票 K 线图和成交量图"""
        if period == 'day':
            stock_data = self.daily_data[-100:]
            name = '日线'
        elif period == 'week':
            stock_data = self.weekly_data[-100:]
            name = '周线'
        elif period == 'month':
            stock_data = self.monthly_data[-100:]
            name = '月线'
        else:
            raise ValueError(f'Invalid period: {period}')

        fig = make_subplots(
            rows=2,
            cols=1,
            shared_xaxes=True,
            subplot_titles=('交易', '成交量'),
            row_heights=[0.7, 0.3],
            vertical_spacing=0.1,
        )

        # 使用不复权数据绘制 K 线图
        fig.add_trace(
            go.Candlestick(x=stock_data['date'], open=stock_data['raw_open'], high=stock_data['raw_high'], low=stock_data['raw_low'], close=stock_data['raw_close'], name=name),
            row=1, col=1
        )
        fig.add_trace(
            go.Bar(x=stock_data['date'], y=stock_data['volume'], marker_color=stock_data['color'], showlegend=False),
            row=2, col=1
        )

        # 只在某些位置显示日期标签，避免过于密集
        tickvals = []
        ticktext = []
        step = max(1, len(stock_data) // 10)  # 最多显示 10 个标签
        for i in range(0, len(stock_data), step):
            tickvals.append(stock_data['date'][i])
            ticktext.append(stock_data['date'][i].strftime("%Y-%m-%d"))
        
        # 去除休市的日期，保持x轴连续
        dt_all = pl.select(pl.date_range(start=stock_data['date'][0], end=stock_data['date'][-1], interval='1d')).to_series().to_list()
        dt_all = [d.strftime("%Y-%m-%d") for d in dt_all]
        trading_dates = [d.strftime("%Y-%m-%d") for d in stock_data['date'].to_list()]
        dt_breaks = list(set(dt_all) - set(trading_dates))

        # 设置 x 轴标签，只显示部分日期  # Do not show OHLC's rangeslider plot
        fig.update_xaxes(tickvals=tickvals, ticktext=ticktext, row=1, col=1)
        fig.update_xaxes(tickvals=tickvals, ticktext=ticktext, row=2, col=1)
        fig.update_xaxes(rangebreaks=[dict(values=dt_breaks)]) if period in ['day'] else None
        fig.update(layout_xaxis_rangeslider_visible=False)

        fig.show()


if __name__ == '__main__':
    # WriteStockExchange('sh600036')
    ReadStockExchange('sh600036', period='week')
