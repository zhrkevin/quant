#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import os
import polars as pl
import akshare as ak
import akshare_proxy_patch
from datetime import date, timedelta
from functools import reduce

from project import Config


akshare_proxy_patch.install_patch(auth_ip="101.201.173.125", auth_token="20260403HAHSSA7W")
pl.Config(tbl_rows=12, tbl_cols=-1)


class LimitUp:

    @classmethod
    def run(cls, today=date.today()):
        raw_limitup = pl.from_pandas(ak.stock_zt_pool_em(date=today.strftime('%Y%m%d')))
        raw_limitup.write_parquet(Config['Paths']['DataPath'] / 'limitup' / f'today.parquet')
        print(f'{today} 涨停数据下载完成 \n{raw_limitup}')


class WriteData:

    @classmethod
    def run(cls, today=date.today(), windows=365):
        start_date = (today - timedelta(days=windows)).strftime('%Y%m%d')
        end_date = today.strftime('%Y%m%d')
        print(f'数据日期: {start_date} 今天日期: {end_date}')

        raw_limitup = pl.read_parquet(Config['Paths']['DataPath'] / 'limitup' / f'today.parquet')
        for symbol in raw_limitup['代码'].to_list():        
            if not os.path.exists(Config['Paths']['DataPath'] / f'limitup/stock/{symbol}'):
                os.makedirs(Config['Paths']['DataPath'] / f'limitup/stock/{symbol}')
            raw_data = pl.from_pandas(
                ak.stock_zh_a_hist(symbol=symbol, start_date=start_date, end_date=end_date, adjust='qfq')
            ).with_columns([
                pl.when(pl.col('收盘') > pl.col('开盘')).then(pl.lit('红'))
                  .when(pl.col('收盘') < pl.col('开盘')).then(pl.lit('绿'))
                  .otherwise(pl.lit('黑')).alias('颜色'),
            ])
            raw_data.write_parquet(Config['Paths']['DataPath'] / f'limitup/stock/{symbol}' / f'raw.parquet')
            print(f'{symbol} raw 数据下载完成 \n{raw_data}')
    
    @classmethod
    def aggregation(cls, raw_data):
        pass


class SplitData:

    @classmethod
    def run(cls, symbol):
        # 读入原始数据
        raw_data = pl.read_parquet(Config['Paths']['DataPath'] / f'limitup/stock/{symbol}' / f'raw.parquet')

        # 按日天统计数据
        day_data = raw_data.group_by(pl.col('日期').dt.year().alias('年'), pl.col('日期').dt.ordinal_day().alias('日'))
        day_data = cls.aggregation(day_data)
        day_data.write_parquet(Config['Paths']['DataPath'] / f'limitup/stock/{symbol}' / 'data_daily.parquet')
        print(day_data)

        # 按周度统计数据
        week_data = raw_data.group_by(pl.col('日期').dt.year().alias('年'), pl.col('日期').dt.week().alias('周'))
        week_data = cls.aggregation(week_data)
        week_data.write_parquet(Config['Paths']['DataPath'] / f'limitup/stock/{symbol}' / 'data_weekly.parquet')
        print(week_data)

        # 按月度统计数据
        month_data  = raw_data.group_by(pl.col('日期').dt.year().alias('年'), pl.col('日期').dt.month().alias('月'))
        month_data = cls.aggregation(month_data)
        month_data.write_parquet(Config['Paths']['DataPath'] / f'limitup/stock/{symbol}' / 'data_monthly.parquet')
        print(month_data)

    @classmethod
    def aggregation(cls, raw_data):
        raw_data = raw_data.agg([
            pl.col('日期').last().alias('日期'),
            pl.col('开盘').first().alias('开盘'),
            pl.col('最高').max().alias('最高'),
            pl.col('最低').min().alias('最低'),
            pl.col('收盘').last().alias('收盘'),
            pl.col('成交量').sum().alias('成交量'),
            pl.col('成交额').sum().alias('成交额'),
            pl.col('换手率').sum().alias('换手率'), 
        ]).with_columns(
            (pl.col('收盘') - pl.col('开盘')).alias('涨跌额'),
            ((pl.col('最高') - pl.col('最低')) / pl.col('最低') * 100).alias('振幅'), 
            ((pl.col('收盘') - pl.col('开盘')) / pl.col('开盘') * 100).alias('涨跌幅'),
            pl.when(pl.col('收盘') >= pl.col('开盘')).then(pl.lit('红')).otherwise(pl.lit('绿')).alias('颜色'),
        ).sort('日期')
        return raw_data


class Index:

    raw_data, ma_data, macd_data, boll_data = None, None, None, None

    @classmethod
    def run(cls):
        raw_limitup = pl.read_parquet(Config['Paths']['DataPath'] / 'limitup' / f'today-new.parquet')
        for symbol in raw_limitup['代码'].to_list():
                cls.raw_data = pl.read_parquet(Config['Paths']['DataPath'] / f'limitup/stock/{symbol}' / f'data_{period}.parquet')
                cls.moving_average()
                cls.moving_average_convergence_divergence()
                cls.bollinger_bands()
                reduce_data = reduce(lambda x, y: x.join(y, on='日期', how='inner'), [cls.ma_data, cls.macd_data, cls.boll_data])
                reduce_data.write_parquet(Config['Paths']['DataPath'] / f'limitup/stock/{symbol}' / f'index_{period}.parquet')
                print(f'{symbol} {period} 指标计算完成 \n{reduce_data}')


    @classmethod
    def moving_average(cls):
        # 计算移动平均线 MA 5, 10, 20, 30, 60 周期
        cls.ma_data = pl.DataFrame({
            '日期': cls.raw_data['日期'],
            'MA5': cls.raw_data['收盘'].rolling_mean(5),
            'MA10': cls.raw_data['收盘'].rolling_mean(10),
            'MA20': cls.raw_data['收盘'].rolling_mean(20),
            'MA30': cls.raw_data['收盘'].rolling_mean(30),
            'MA60': cls.raw_data['收盘'].rolling_mean(60),
        })

    @classmethod
    def moving_average_convergence_divergence(cls, fast=12, slow=26, span=9):
        # 计算指数移动平均线 EMA、快线 DIF、慢线 DEA、柱状图 MACD 
        basic_ema = pl.DataFrame({
            '日期': cls.raw_data['日期'],
            'EMA快线': cls.raw_data['收盘'].ewm_mean(span=fast, adjust=False),
            'EMA慢线': cls.raw_data['收盘'].ewm_mean(span=slow, adjust=False),
        })
        cls.macd_data = pl.DataFrame({
            '日期': basic_ema['日期'],
            'DIF': (basic_ema['EMA快线']-basic_ema['EMA慢线']),
            'DEA': (basic_ema['EMA快线']-basic_ema['EMA慢线']).ewm_mean(span=span, adjust=False),
            'MACD': (((basic_ema['EMA快线']-basic_ema['EMA慢线'])-(basic_ema['EMA快线']-basic_ema['EMA慢线']).ewm_mean(span=span, adjust=False))*2).alias('MACD'),
        }).with_columns(
            pl.when(pl.col('DIF')-pl.col('DEA') > 0).then(pl.lit('红'))
              .when(pl.col('DIF')-pl.col('DEA') < 0).then(pl.lit('绿'))
              .otherwise(pl.lit('黑')).alias('颜色'),
        )

    @classmethod
    def bollinger_bands(cls, mean=20, std=2):
        # 计算布林线 20 周期 + 2 倍标准差
        cls.boll_data = pl.DataFrame({
            '日期': cls.raw_data['日期'],
            'Boll中轨': cls.raw_data['收盘'].rolling_mean(mean),
            'Boll上轨': (cls.raw_data['收盘'].rolling_mean(mean) + cls.raw_data['收盘'].rolling_std(mean) * std),
            'Boll下轨': (cls.raw_data['收盘'].rolling_mean(mean) - cls.raw_data['收盘'].rolling_std(mean) * std),
        })


if __name__ == '__main__':
    for period in ['daily', 'weekly', 'monthly']:
        WriteData.run(symbol='000001', period=period)
        Index.run(symbol='000001', period=period)
