#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import os
import polars as pl
import akshare as ak
import akshare_proxy_patch
from datetime import date

from project.configuration import Config
from algorithm.data.product import Stocks, ETFs


akshare_proxy_patch.install_patch("101.201.173.125", "", 30)
pl.Config(tbl_rows=12, tbl_cols=-1)


class WriteData:

    def __init__(self, renew=False):
        self.renew = renew
        self.save()

    def save(self):
        for symbol in Stocks:
            os.makedirs(Config.Paths.DataPath / 'stock' / symbol) if not os.path.exists(Config.Paths.DataPath / 'stock' / symbol) else None
            self.download_all_stocks_data(symbol)
            self.download_updated_stocks_data(symbol)

        for symbol in ETFs:
            os.makedirs(Config.Paths.DataPath / 'etf' / symbol) if not os.path.exists(Config.Paths.DataPath / 'etf' / symbol) else None
            self.download_all_etfs_data(symbol)
            self.download_updated_etfs_data(symbol)

    def download_all_stocks_data(self, symbol='sh600025', start_date='20150101', end_date='20251231'):
        if not self.renew and os.path.exists(Config.Paths.DataPath / 'stock' / symbol / 'raw.parquet'):
            print(f'{symbol} 全量数据已存在')
        else:
            print(f'{symbol} 全量数据重新下载')
            raw_data = pl.from_pandas(
                ak.stock_zh_a_daily(symbol=symbol, start_date=start_date, end_date=end_date, adjust='')
            )
            print('全量除权数据下载完成')
            qfq_data = pl.from_pandas(
                ak.stock_zh_a_daily(symbol=symbol, start_date=start_date, end_date=end_date, adjust='qfq')
            )
            print('全量前复权数据下载完成')
            hfq_data = pl.from_pandas(
                ak.stock_zh_a_daily(symbol=symbol, start_date=start_date, end_date=end_date, adjust='hfq')
            )
            print('全量后复权数据下载完成')

            new_raw_data = pl.DataFrame({
                'date': raw_data['date'],
                'raw_open': raw_data['open'], 'raw_high': raw_data['high'], 'raw_low': raw_data['low'], 'raw_close': raw_data['close'],
                'qfq_open': qfq_data['open'], 'qfq_high': qfq_data['high'], 'qfq_low': qfq_data['low'], 'qfq_close': qfq_data['close'],
                'hfq_open': hfq_data['open'], 'hfq_high': hfq_data['high'], 'hfq_low': hfq_data['low'], 'hfq_close': hfq_data['close'],
                'volume': raw_data['volume'], 'amount': raw_data['amount'], 'turnover': raw_data['turnover'],
            })
            print(f'三种复权类型全量数据合并完成 \n{new_raw_data}')
            new_raw_data.write_parquet(Config.Paths.DataPath / 'stock' / symbol / 'raw.parquet')

    @staticmethod
    def download_updated_stocks_data(symbol='sh600025'):
        all_raw_data = pl.read_parquet(Config.Paths.DataPath / 'stock' / symbol / 'raw.parquet')
        print(f'读取全量数据')

        if all_raw_data['date'].max() >= date.today():
            print(f'{symbol} \n增量数据已更新')
        else:
            print(f'最新日期： {all_raw_data["date"].max()} 今天日期：{date.today()}')

            print(f'{symbol} \n增量数据开始下载')
            updated_raw_data = pl.from_pandas(
                ak.stock_zh_a_daily(
                    symbol=symbol,
                    start_date=all_raw_data['date'].max(),
                    end_date=date.today().strftime('%Y%m%d'),
                    adjust=''
                )
            )
            print('增量除权数据下载完成')
            updated_qfq_data = pl.from_pandas(
                ak.stock_zh_a_daily(
                    symbol=symbol,
                    start_date=all_raw_data['date'].max(),
                    end_date=date.today().strftime('%Y%m%d'),
                    adjust='qfq'
                )
            )
            print('增量前复权数据下载完成')
            updated_hfq_data = pl.from_pandas(
                ak.stock_zh_a_daily(
                    symbol=symbol,
                    start_date=all_raw_data['date'].max(),
                    end_date=date.today().strftime('%Y%m%d'),
                    adjust='hfq'
                )
            )
            print('增量后复权数据下载完成')

            updated_data = pl.DataFrame({
                'date': updated_raw_data['date'],
                'raw_open': updated_raw_data['open'], 'raw_high': updated_raw_data['high'], 'raw_low': updated_raw_data['low'], 'raw_close': updated_raw_data['close'],
                'qfq_open': updated_qfq_data['open'], 'qfq_high': updated_qfq_data['high'], 'qfq_low': updated_qfq_data['low'], 'qfq_close': updated_qfq_data['close'],
                'hfq_open': updated_hfq_data['open'], 'hfq_high': updated_hfq_data['high'], 'hfq_low': updated_hfq_data['low'], 'hfq_close': updated_hfq_data['close'],
                'volume': updated_raw_data['volume'], 'amount': updated_raw_data['amount'], 'turnover': updated_raw_data['turnover'],
            })
            new_raw_data = pl.concat([all_raw_data, updated_data]).unique(subset=['date'], keep='last').sort('date')
            print(f'全量增量量数据合并完成 \n{new_raw_data}')
            new_raw_data.write_parquet(Config.Paths.DataPath / 'stock' / symbol / 'raw.parquet')

    def download_all_etfs_data(self, symbol='sh000016'):
        if not self.renew and os.path.exists(Config.Paths.DataPath / 'etf' / symbol / 'raw.parquet'):
            print(f'{symbol} 全量数据已存在')
        else:
            print(f'{symbol} 全量数据开始下载')
            all_raw_data = pl.from_pandas(
                ak.stock_zh_index_daily_em(symbol=symbol, start_date='20150101', end_date='20251231')
            )
            all_raw_data = all_raw_data.with_columns(
                pl.col('date').str.strptime(pl.Date, '%Y-%m-%d'),
            ).rename({
                'open': 'raw_open',
                'high': 'raw_high',
                'low': 'raw_low',
                'close': 'raw_close',
            })
            print(f'全量数据下载完成 \n{all_raw_data}')
            all_raw_data.write_parquet(Config.Paths.DataPath / 'etf' / symbol / 'raw.parquet')

    @staticmethod
    def download_updated_etfs_data(symbol='sh000016'):
        all_raw_data = pl.read_parquet(Config.Paths.DataPath / 'etf' / symbol / 'raw.parquet')
        update_date = all_raw_data['date'].max()
        print(f'读取全量数据')

        if update_date >= date.today():
            print(f'{symbol} 增量数据已更新')
        else:
            print(f'最新日期：{update_date} 今天日期：{date.today()}')

            print(f'{symbol} 增量数据开始下载')
            updated_raw_data = pl.from_pandas(
                ak.stock_zh_index_daily_em(
                    symbol=symbol, start_date=update_date.strftime('%Y%m%d'),
                    end_date=date.today().strftime('%Y%m%d')
                )
            )
            updated_raw_data = updated_raw_data.with_columns(
                pl.col('date').str.strptime(pl.Date, '%Y-%m-%d'),
            ).rename({
                'open': 'raw_open',
                'high': 'raw_high',
                'low': 'raw_low',
                'close': 'raw_close',
            })
            print(f'增量数据下载完成')

            new_raw_data = pl.concat([all_raw_data, updated_raw_data]).unique(subset=['date'], keep='last').sort(
                'date')
            print(f'全量增量量数据合并完成 \n{new_raw_data}')
            new_raw_data.write_parquet(Config.Paths.DataPath / 'etf' / symbol / 'raw.parquet')


class SplitData:

    def __init__(self):
        for symbol in Stocks:
            self.stocks(symbol)

        for symbol in ETFs:
            self.etfs(symbol)

    @staticmethod
    def stocks(symbol):
        # 读入原始数据
        raw_data = pl.read_parquet(Config.Paths.DataPath / 'stock' / symbol / 'raw.parquet')

        # 按日天统计数据
        day_data = raw_data.with_columns(
            pl.col('date').dt.year().alias('year'),
            pl.col('date').dt.ordinal_day().alias('day'),
            pl.when(pl.col('raw_close') >= pl.col('raw_open')).then(pl.lit('green')).otherwise(pl.lit('red')).alias('color')
        ).select(
            'year', 'day', pl.exclude('year', 'day')
        )
        day_data.write_parquet(Config.Paths.DataPath / 'stock' / symbol / 'day.parquet')
        print(day_data)

        # 按周度统计数据
        week_data = raw_data.group_by(
            pl.col('date').dt.year().alias('year'),
            pl.col('date').dt.week().alias('week')
        ).agg(
            [
                pl.col('date').last(),
                pl.col('raw_open').first(), pl.col('raw_high').max(), pl.col('raw_low').min(), pl.col('raw_close').last(),
                pl.col('qfq_open').first(), pl.col('qfq_high').max(), pl.col('qfq_low').min(), pl.col('qfq_close').last(),
                pl.col('hfq_open').first(), pl.col('hfq_high').max(), pl.col('hfq_low').min(), pl.col('hfq_close').last(),
                pl.col('volume').sum().alias('volume'), pl.col('amount').sum().alias('amount'), pl.col('turnover').sum().alias('turnover'),
                pl.when(pl.col('raw_close').last() >= pl.col('raw_open').first()).then(pl.lit('green')).otherwise(pl.lit('red')).alias('color'),
            ]
        ).sort('date')
        week_data.write_parquet(Config.Paths.DataPath / 'stock' / symbol / 'week.parquet')
        print(week_data)

        # 按月度统计数据
        month_data = raw_data.group_by(
            pl.col('date').dt.year().alias('year'),
            pl.col('date').dt.month().alias('month'),
        ).agg(
            [
                pl.col('date').last(),
                pl.col('raw_open').first(), pl.col('raw_high').max(), pl.col('raw_low').min(), pl.col('raw_close').last(),
                pl.col('qfq_open').first(), pl.col('qfq_high').max(), pl.col('qfq_low').min(), pl.col('qfq_close').last(),
                pl.col('hfq_open').first(), pl.col('hfq_high').max(), pl.col('hfq_low').min(), pl.col('hfq_close').last(),
                pl.col('volume').sum().alias('volume'), pl.col('amount').sum().alias('amount'), pl.col('turnover').sum().alias('turnover'),
                pl.when(pl.col('raw_close').last() >= pl.col('raw_open').first()).then(pl.lit('green')).otherwise(pl.lit('red')).alias('color'),
            ]
        ).sort('date')
        month_data.write_parquet(Config.Paths.DataPath / 'stock' / symbol / 'month.parquet')
        print(month_data)
        
        # 按季度统计数据
        quarter_data = raw_data.group_by(
            pl.col('date').dt.year().alias('year'),
            pl.col('date').dt.quarter().alias('quarter'),
        ).agg(
            [
                pl.col('date').last(),
                pl.col('raw_open').first(), pl.col('raw_high').max(), pl.col('raw_low').min(), pl.col('raw_close').last(),
                pl.col('qfq_open').first(), pl.col('qfq_high').max(), pl.col('qfq_low').min(), pl.col('qfq_close').last(),
                pl.col('hfq_open').first(), pl.col('hfq_high').max(), pl.col('hfq_low').min(), pl.col('hfq_close').last(),
                pl.col('volume').sum().alias('volume'), pl.col('amount').sum().alias('amount'), pl.col('turnover').sum().alias('turnover'),
                pl.when(pl.col('raw_close').last() >= pl.col('raw_open').first()).then(pl.lit('green')).otherwise(pl.lit('red')).alias('color'),
            ]
        ).sort('date')
        quarter_data.write_parquet(Config.Paths.DataPath / 'stock' / symbol / 'quarter.parquet')
        print(quarter_data)

    @staticmethod
    def etfs(symbol):
        # 读入原始数据
        raw_data = pl.read_parquet(Config.Paths.DataPath / 'etf' / symbol / 'raw.parquet')
        print(raw_data)

        # 按日天统计数据
        day_data = raw_data.with_columns(
            pl.col('date').dt.year().alias('year'),
            pl.col('date').dt.ordinal_day().alias('day'),
            pl.when(pl.col('raw_close') >= pl.col('raw_open')).then(pl.lit('green')).otherwise(pl.lit('red')).alias('color')
        ).select(
            'year', 'day', pl.exclude('year', 'day')
        )
        day_data.write_parquet(Config.Paths.DataPath / 'etf' / symbol / 'day.parquet')
        print(day_data)

        # 按周度统计数据
        week_data = raw_data.group_by(
            pl.col('date').dt.year().alias('year'),
            pl.col('date').dt.week().alias('week')
        ).agg(
            [
                pl.col('date').last(),
                pl.col('raw_open').first(), pl.col('raw_high').max(), pl.col('raw_low').min(), pl.col('raw_close').last(),
                pl.col('volume').sum().alias('volume'), pl.col('amount').sum().alias('amount'),
                pl.when(pl.col('raw_close').last() >= pl.col('raw_open').first()).then(pl.lit('green')).otherwise(pl.lit('red')).alias('color'),
            ]
        ).sort('date')
        week_data.write_parquet(Config.Paths.DataPath / 'etf' / symbol / 'week.parquet')
        print(week_data)

        # 按月度统计数据
        month_data = raw_data.group_by(
            pl.col('date').dt.year().alias('year'),
            pl.col('date').dt.month().alias('month'),
        ).agg(
            [
                pl.col('date').last(),
                pl.col('raw_open').first(), pl.col('raw_high').max(), pl.col('raw_low').min(), pl.col('raw_close').last(),
                pl.col('volume').sum().alias('volume'), pl.col('amount').sum().alias('amount'),
                pl.when(pl.col('raw_close').last() >= pl.col('raw_open').first()).then(pl.lit('green')).otherwise(pl.lit('red')).alias('color'),
            ]
        ).sort('date')
        month_data.write_parquet(Config.Paths.DataPath / 'etf' / symbol / 'month.parquet')
        print(month_data)
        
        # 按季度统计数据
        quarter_data = raw_data.group_by(
            pl.col('date').dt.year().alias('year'),
            pl.col('date').dt.quarter().alias('quarter'),
        ).agg(
            [
                pl.col('date').last(),
                pl.col('raw_open').first(), pl.col('raw_high').max(), pl.col('raw_low').min(), pl.col('raw_close').last(),
                pl.col('volume').sum().alias('volume'), pl.col('amount').sum().alias('amount'),
                pl.when(pl.col('raw_close').last() >= pl.col('raw_open').first()).then(pl.lit('green')).otherwise(pl.lit('red')).alias('color'),
            ]
        ).sort('date')
        quarter_data.write_parquet(Config.Paths.DataPath / 'etf' / symbol / 'quarter.parquet')
        print(quarter_data)


if __name__ == '__main__':
    WriteData()
    SplitData()

    # Plotting('sh600036', period='day')
