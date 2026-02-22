#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import polars as pl
import akshare as ak
from algorithms.basic.plot import Plotting

from project.configuration import Config


pl.Config(tbl_rows=12, tbl_cols=-1)


class WriteStocks:

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


class SplitStocks:
    """上海证券交易所周股票数据管理类"""

    def __init__(self, symbol: str):
        self.daily_data = None
        self.weekly_data = None
        self.monthly_data = None
        
        self.read(symbol)

    def read(self, symbol: str):
        """从 parquet 文件读取合并的股票数据
        Args:
            symbol: 股票代码
        """

        # 读入原始数据
        self.raw_data = pl.read_parquet(Config.Paths.DataPath / 'input' / symbol / 'raw.parquet')

        # 按日天统计数据
        self.day_data = self.raw_data.with_columns(
            pl.col('date').dt.year().alias('year'),
            pl.col('date').dt.ordinal_day().alias('day'),
            pl.when(pl.col('raw_close') >= pl.col('raw_open')).then(pl.lit('green')).otherwise(pl.lit('red')).alias('color')
        ).select(
            'year', 'day', pl.exclude('year', 'day')
        )
        self.day_data.write_parquet(Config.Paths.DataPath / 'input' / symbol / 'stock_day.parquet')
        print(self.day_data)

        # 按周度统计数据
        self.week_data = self.raw_data.group_by(
            pl.col('date').dt.year().alias('year'),
            pl.col('date').dt.week().alias('week')
        ).agg(
            [
                pl.col('date').last(),
                pl.col('raw_open').first(), pl.col('raw_high').max(), pl.col('raw_low').min(), pl.col('raw_close').last(),
                pl.col('qfq_open').first(), pl.col('qfq_high').max(), pl.col('qfq_low').min(), pl.col('qfq_close').last(),
                pl.col('hfq_open').first(), pl.col('hfq_high').max(), pl.col('hfq_low').min(), pl.col('hfq_close').last(),
                pl.col('volume').sum().alias('volume'),
                pl.col('amount').sum().alias('amount'),
                pl.col('turnover').sum().alias('turnover'),
                pl.when(pl.col('raw_close').last() >= pl.col('raw_open').first()).then(pl.lit('green')).otherwise(pl.lit('red')).alias('color'),
            ]
        ).sort('date').with_columns(
            pl.col('week').cast(pl.Int16)
        )
        self.week_data.write_parquet(Config.Paths.DataPath / 'input' / symbol / 'stock_week.parquet')
        print(self.week_data)

        # 按月度统计数据
        self.month_data = self.raw_data.group_by(
            pl.col('date').dt.year().alias('year'),
            pl.col('date').dt.month().alias('month'),
        ).agg(
            [
                pl.col('date').last(),
                pl.col('raw_open').first(), pl.col('raw_high').max(), pl.col('raw_low').min(), pl.col('raw_close').last(),
                pl.col('qfq_open').first(), pl.col('qfq_high').max(), pl.col('qfq_low').min(), pl.col('qfq_close').last(),
                pl.col('hfq_open').first(), pl.col('hfq_high').max(), pl.col('hfq_low').min(), pl.col('hfq_close').last(),
                pl.col('volume').sum().alias('volume'),
                pl.col('amount').sum().alias('amount'),
                pl.col('turnover').sum().alias('turnover'),
                pl.when(pl.col('raw_close').last() >= pl.col('raw_open').first()).then(pl.lit('green')).otherwise(pl.lit('red')).alias('color'),
            ]
        ).sort('date').with_columns(
            pl.col('month').cast(pl.Int16)
        )
        self.month_data.write_parquet(Config.Paths.DataPath / 'input' / symbol / 'stock_month.parquet')
        print(self.month_data)
        
        # 按季度统计数据
        self.quarter_data = self.raw_data.group_by(
            pl.col('date').dt.year().alias('year'),
            pl.col('date').dt.quarter().alias('quarter'),
        ).agg(
            [
                pl.col('date').last(),
                pl.col('raw_open').first(), pl.col('raw_high').max(), pl.col('raw_low').min(), pl.col('raw_close').last(),
                pl.col('qfq_open').first(), pl.col('qfq_high').max(), pl.col('qfq_low').min(), pl.col('qfq_close').last(),
                pl.col('hfq_open').first(), pl.col('hfq_high').max(), pl.col('hfq_low').min(), pl.col('hfq_close').last(),
                pl.col('volume').sum().alias('volume'),
                pl.col('amount').sum().alias('amount'),
                pl.col('turnover').sum().alias('turnover'),
                pl.when(pl.col('raw_close').last() >= pl.col('raw_open').first()).then(pl.lit('green')).otherwise(pl.lit('red')).alias('color'),
            ]
        ).sort('date').with_columns(
            pl.col('quarter').cast(pl.Int16)
        )
        self.quarter_data.write_parquet(Config.Paths.DataPath / 'input' / symbol / 'stock_quarter.parquet')
        print(self.quarter_data)


if __name__ == '__main__':
    # WriteStocks('sh600036')
    SplitStocks('sh600036')
    Plotting('sh600036', period='day')
