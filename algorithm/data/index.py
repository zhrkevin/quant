#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import polars as pl

from project.configuration import Config
from algorithm.data.product import Stocks, ETFs


pl.Config(tbl_rows=20, tbl_cols=-1)


class Index:

    def __init__(self):
        for period in ['day', 'week', 'month', 'quarter']:
            for stock in Stocks:
                self.moving_average(stock, period, product='stock')
                self.moving_average_convergence_divergence(stock, period, product='stock')
                self.bollinger_bands(stock, period, product='stock')
            for etf in ETFs:
                self.moving_average(etf, period, product='etf')
                self.moving_average_convergence_divergence(etf, period, product='etf')
                self.bollinger_bands(etf, period, product='etf')

    def moving_average(self, symbol, period, product, adjust='raw'):
        # 计算移动平均线 MA
        raw_data = pl.read_parquet(Config.Paths.DataPath / product / symbol / f'{period}.parquet')
        new_data = raw_data.select([
            pl.col('date'),
            pl.col(f'{adjust}_close').rolling_mean(5).alias('ma_5'),      # 005 trading period
            pl.col(f'{adjust}_close').rolling_mean(10).alias('ma_10'),    # 010 trading period
            pl.col(f'{adjust}_close').rolling_mean(20).alias('ma_20'),    # 020 trading period
            pl.col(f'{adjust}_close').rolling_mean(30).alias('ma_30'),    # 030 trading period
            pl.col(f'{adjust}_close').rolling_mean(60).alias('ma_60'),    # 060 trading period
            pl.col(f'{adjust}_close').rolling_mean(250).alias('ma_250'),  # 250 trading period
        ])
        new_data.write_parquet(Config.Paths.DataPath / product / symbol / f'ma_{period}.parquet')
        print(new_data)

    def moving_average_convergence_divergence(self, symbol, period, product, adjust='raw'):
        # 计算指数移动平均线 EMA、快线 DIF、慢线 DEA、柱状图 MACD 
        fast, slow, span = 12, 26, 9
        raw_data = pl.read_parquet(Config.Paths.DataPath / product / symbol / f'{period}.parquet')
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
        new_data.write_parquet(Config.Paths.DataPath / product / symbol / f'macd_{period}.parquet')
        print(new_data)

    def bollinger_bands(self, symbol, period, product, adjust='raw'):
        # 计算布林线 20 周期 + 2 倍标准差
        mean, std = 20, 2
        raw_data = pl.read_parquet(Config.Paths.DataPath / product / symbol / f'{period}.parquet')
        new_data = raw_data.select([
            pl.col('date'),
            pl.col(f'{adjust}_close').rolling_mean(mean).alias('boll_mid'),
            (pl.col(f'{adjust}_close').rolling_mean(mean) + pl.col(f'{adjust}_close').rolling_std(mean) * std).alias('boll_upper'),
            (pl.col(f'{adjust}_close').rolling_mean(mean) - pl.col(f'{adjust}_close').rolling_std(mean) * std).alias('boll_lower'),
        ])
        new_data.write_parquet(Config.Paths.DataPath / product / symbol / f'boll_{period}.parquet')
        print(new_data)


if __name__ == '__main__':
    # for stock in Stocks:
    #     for period in ['day', 'week', 'month', 'quarter']:
    #         MovingAverage(stock, period=period)
    #         MovingAverageConvergenceDivergence(stock, period=period)
    #         BollingerBands(stock, period=period)

    Index()

    # Plotting('sh600036', period='day')
