#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import os
import datetime
import polars as pl
import akshare as ak
import akshare_proxy_patch


from project.configuration import Config
from algorithm.dividend.product import Stocks, ETFs


akshare_proxy_patch.install_patch("101.201.173.125", "", 30)
pl.Config(tbl_rows=12, tbl_cols=-1)


class FetchLimitUp:

    @classmethod
    def run(cls, selected_date=datetime.date.today()):
        today = selected_date.strftime('%Y%m%d')

        raw_limitup_data = pl.from_pandas(ak.stock_zt_pool_em(date=today))
        raw_limitup_data.write_parquet(Config['Paths']['DataPath'] / 'input' / f'today.parquet')
        print(f'{selected_date} 涨停数据下载完成 \n{raw_limitup_data}')


class SplitData:

    @classmethod
    def stocks(cls, symbol):
        # 读入原始数据
        raw_data = pl.read_parquet(Config['Paths']['DataPath'] / 'input' / symbol / 'raw.parquet')

        # 按日天统计数据
        day_data = raw_data.with_columns(
            pl.col('date').dt.year().alias('year'),
            pl.col('date').dt.ordinal_day().alias('day'),
            pl.when(pl.col('raw_close') >= pl.col('raw_open')).then(pl.lit('green')).otherwise(pl.lit('red')).alias('color')
        ).select(
            'year', 'day', pl.exclude('year', 'day')
        )
        day_data.write_parquet(Config['Paths']['DataPath'] / 'stock' / symbol / 'day.parquet')
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
        week_data.write_parquet(Config['Paths']['DataPath'] / 'stock' / symbol / 'week.parquet')
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
        month_data.write_parquet(Config['Paths']['DataPath'] / 'stock' / symbol / 'month.parquet')
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
        quarter_data.write_parquet(Config['Paths']['DataPath'] / 'stock' / symbol / 'quarter.parquet')
        print(quarter_data)


class Index:

    @classmethod
    def run(cls):
        for period in ['day', 'week', 'month', 'quarter']:
            for stock in Stocks:
                cls.moving_average(stock, period, product='stock')
                cls.moving_average_convergence_divergence(stock, period, product='stock')
                cls.bollinger_bands(stock, period, product='stock')
            for etf in ETFs:
                cls.moving_average(etf, period, product='etf')
                cls.moving_average_convergence_divergence(etf, period, product='etf')
                cls.bollinger_bands(etf, period, product='etf')

    @classmethod
    def moving_average(cls, symbol, period, product, adjust='raw'):
        # 计算移动平均线 MA
        raw_data = pl.read_parquet(Config['Paths']['DataPath'] / product / symbol / f'{period}.parquet')
        new_data = raw_data.select([
            pl.col('date'),
            pl.col(f'{adjust}_close').rolling_mean(5).alias('ma_5'),      # 005 trading period
            pl.col(f'{adjust}_close').rolling_mean(10).alias('ma_10'),    # 010 trading period
            pl.col(f'{adjust}_close').rolling_mean(20).alias('ma_20'),    # 020 trading period
            pl.col(f'{adjust}_close').rolling_mean(30).alias('ma_30'),    # 030 trading period
            pl.col(f'{adjust}_close').rolling_mean(60).alias('ma_60'),    # 060 trading period
            pl.col(f'{adjust}_close').rolling_mean(250).alias('ma_250'),  # 250 trading period
        ])
        new_data.write_parquet(Config['Paths']['DataPath'] / product / symbol / f'ma_{period}.parquet')
        print(new_data)

    @classmethod
    def moving_average_convergence_divergence(cls, symbol, period, product, adjust='raw'):
        # 计算指数移动平均线 EMA、快线 DIF、慢线 DEA、柱状图 MACD 
        fast, slow, span = 12, 26, 9
        raw_data = pl.read_parquet(Config['Paths']['DataPath'] / product / symbol / f'{period}.parquet')
        ema = pl.DataFrame({
            'date': raw_data['date'],
            'ema_fast': raw_data.select([
                pl.col(f'{adjust}_close').ewm_mean(span=fast, adjust=False)
            ]),
            'ema_slow': raw_data.select([
                pl.col(f'{adjust}_close').ewm_mean(span=slow, adjust=False)
            ]),
        })
        new_data = pl.DataFrame({
            'date': ema['date'],
            'dif': ema['ema_fast'] - ema['ema_slow'],
            'dea': (ema['ema_fast'] - ema['ema_slow']).ewm_mean(span=span, adjust=False),
        })
        new_data = new_data.with_columns(
            pl.col('date'),
            (pl.col('dif') - pl.col('dea')).alias('macd'),
            pl.when(pl.col('dif') - pl.col('dea') >= 0).then(pl.lit('red')).otherwise(pl.lit('green')).alias('color')
        )
        new_data.write_parquet(Config['Paths']['DataPath'] / product / symbol / f'macd_{period}.parquet')
        print(new_data)

    @classmethod
    def bollinger_bands(cls, symbol, period, product, adjust='raw'):
        # 计算布林线 20 周期 + 2 倍标准差
        mean, std = 20, 2
        raw_data = pl.read_parquet(Config['Paths']['DataPath'] / product / symbol / f'{period}.parquet')
        new_data = raw_data.select([
            pl.col('date'),
            pl.col(f'{adjust}_close').rolling_mean(mean).alias('boll_mid'),
            (pl.col(f'{adjust}_close').rolling_mean(mean) + pl.col(f'{adjust}_close').rolling_std(mean) * std).alias('boll_upper'),
            (pl.col(f'{adjust}_close').rolling_mean(mean) - pl.col(f'{adjust}_close').rolling_std(mean) * std).alias('boll_lower'),
        ])
        new_data.write_parquet(Config['Paths']['DataPath'] / product / symbol / f'boll_{period}.parquet')
        print(new_data)


if __name__ == '__main__':
    FetchLimitUp.run(selected_date=datetime.date(2026, 3, 13))

    # Plotting('sh600036', period='day')

