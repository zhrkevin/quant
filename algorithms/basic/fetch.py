#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import os
import polars as pl
import akshare as ak
from datetime import date

from algorithms.basic.stocks import Stocks
from project.configuration import Config


pl.Config(tbl_rows=12, tbl_cols=-1)


class WriteStocks:

    def __init__(self):
        self.stocks = Stocks
        self.save()
        # self.download_all_stocks_data(symbol='sh600025')
        # self.download_incremental_stocks_data(symbol='sh600025')


    def save(self):
        for symbol in self.stocks:
            os.makedirs(Config.Paths.DataPath / 'input' / symbol) if not os.path.exists(Config.Paths.DataPath / 'input' / symbol) else None
            self.download_all_stocks_data(symbol)
            self.download_incremental_stocks_data(symbol)

    def download_all_stocks_data(self, symbol: str = 'sh600036', start_date: str = '20150101', end_date: str = '20251231'):
        all_raw_data = pl.read_parquet(Config.Paths.DataPath / 'input' / symbol / 'raw.parquet')
        print('读取全量数据')

        if all_raw_data['date'].max() >= date(2025, 12, 31):
            print(f'{symbol} 全量数据已存在')
        else:
            print(f'{symbol} 全量数据开始下载')
            raw_data = pl.from_pandas(ak.stock_zh_a_daily(symbol=symbol, start_date=start_date, end_date=end_date, adjust=''))
            print('全量除权数据下载完成')
            qfq_data = pl.from_pandas(ak.stock_zh_a_daily(symbol=symbol, start_date=start_date, end_date=end_date, adjust='qfq'))
            print('全量前复权数据下载完成')
            hfq_data = pl.from_pandas(ak.stock_zh_a_daily(symbol=symbol, start_date=start_date, end_date=end_date, adjust='hfq'))
            print('全量后复权数据下载完成')

            new_raw_data = pl.DataFrame(
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
            print(f'三种复权类型全量数据合并完成 \n{new_raw_data}')
            new_raw_data.write_parquet(Config.Paths.DataPath / 'input' / symbol / 'raw.parquet')

    def download_incremental_stocks_data(self, symbol: str = 'sh600036'):
        all_raw_data = pl.read_parquet(Config.Paths.DataPath / 'input' / symbol / 'raw.parquet')
        print(f'读取全量数据 \n{all_raw_data}')

        if all_raw_data['date'].max() >= date.today():
            print(f'{symbol} 增量数据已更新')
        else:
            print(f'最新日期： {all_raw_data["date"].max()} 今天日期：{date.today()}')

            print(f'{symbol} 增量数据开始下载')
            incremental_raw_data = pl.from_pandas(ak.stock_zh_a_daily(symbol=symbol, start_date=all_raw_data['date'].max(), end_date=date.today().strftime('%Y%m%d'), adjust=''))
            print('增量除权数据下载完成')
            incremental_qfq_data = pl.from_pandas(ak.stock_zh_a_daily(symbol=symbol, start_date=all_raw_data['date'].max(), end_date=date.today().strftime('%Y%m%d'), adjust='qfq'))
            print('增量前复权数据下载完成')
            incremental_hfq_data = pl.from_pandas(ak.stock_zh_a_daily(symbol=symbol, start_date=all_raw_data['date'].max(), end_date=date.today().strftime('%Y%m%d'), adjust='hfq'))
            print('增量后复权数据下载完成')

            incremental_data = pl.DataFrame({
                'date': incremental_raw_data['date'],
                'raw_open': incremental_raw_data['open'], 'raw_high': incremental_raw_data['high'], 'raw_low': incremental_raw_data['low'], 'raw_close': incremental_raw_data['close'],
                'qfq_open': incremental_qfq_data['open'], 'qfq_high': incremental_qfq_data['high'], 'qfq_low': incremental_qfq_data['low'], 'qfq_close': incremental_qfq_data['close'],
                'hfq_open': incremental_hfq_data['open'], 'hfq_high': incremental_hfq_data['high'], 'hfq_low': incremental_hfq_data['low'], 'hfq_close': incremental_hfq_data['close'],
                'volume': incremental_raw_data['volume'],
                'amount': incremental_raw_data['amount'],
                'turnover': incremental_raw_data['turnover'],
            })
            new_raw_data = pl.concat([all_raw_data, incremental_data]).unique(subset=['date'], keep='last').sort('date')
            print(f'全量增量量数据合并完成 \n{new_raw_data}')
            new_raw_data.write_parquet(Config.Paths.DataPath / 'input' / symbol / 'raw.parquet')


class SplitStocks:
    """上海证券交易所周股票数据管理类"""

    def __init__(self):
        self.stocks = Stocks
        self.daily_data = None
        self.weekly_data = None
        self.monthly_data = None

        self.run()

    def run(self):
        for symbol in self.stocks:
            self.read(symbol)

    def read(self, symbol: str):
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
    # WriteStocks()
    SplitStocks()
    # Plotting('sh600036', period='day')
