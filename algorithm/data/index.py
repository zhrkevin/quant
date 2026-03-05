#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import polars as pl

from project.configuration import Config
from algorithm.data.market import Stocks


pl.Config(tbl_rows=20, tbl_cols=-1)


class MovingAverage:

    def __init__(self, symbol, period='day', adjust='raw', types='stock'):
        self.symbol = symbol
        self.period = period
        self.adjust = adjust
        self.types = types

        self.moving_average()

    def moving_average(self):
        raw_data = pl.read_parquet(
            Config.Paths.DataPath / self.types / self.symbol / f'{self.types}_{self.period}.parquet'
        )

        new_data = raw_data.select(
            [
                pl.col('date'),
                pl.col(f'{self.adjust}_close').rolling_mean(5).alias('ma_5'),      # 5 trading period
                pl.col(f'{self.adjust}_close').rolling_mean(10).alias('ma_10'),    # 10 trading period
                pl.col(f'{self.adjust}_close').rolling_mean(20).alias('ma_20'),    # 20 trading period
                pl.col(f'{self.adjust}_close').rolling_mean(30).alias('ma_30'),    # 30 trading period
                pl.col(f'{self.adjust}_close').rolling_mean(60).alias('ma_60'),    # 60 trading period
                pl.col(f'{self.adjust}_close').rolling_mean(250).alias('ma_250'),  # 250 trading period
            ]
        )
        print(new_data)
        new_data.write_parquet(Config.Paths.DataPath / self.types / self.symbol / f'ma_{self.period}.parquet')


class MovingAverageConvergenceDivergence:

    def __init__(self, symbol, period='day', adjust='raw', types='stock'):
        self.symbol = symbol
        self.period = period
        self.adjust = adjust
        self.types = types

        self.macd()

    def macd(self, fast=12, slow=26, span=9):
        raw_data = pl.read_parquet(
            Config.Paths.DataPath / self.types / self.symbol / f'{self.types}_{self.period}.parquet'
        )
        # 计算指数移动平均线（EMA）
        ema = pl.DataFrame({
            'date': raw_data['date'],
            'ema_fast': raw_data.select(
                [pl.col(f'{self.adjust}_close').ewm_mean(span=fast, adjust=False)]
            ),
            'ema_slow': raw_data.select(
                [pl.col(f'{self.adjust}_close').ewm_mean(span=slow, adjust=False)]
            ),
        })
        # 计算 DIF 快线, DEA 慢线
        data = pl.DataFrame({
            'date': ema['date'],
            'dif': ema['ema_fast'] - ema['ema_slow'],
            'dea': (ema['ema_fast'] - ema['ema_slow']).ewm_mean(span=span, adjust=False),
        })
        # 计算 MACD 柱状图
        data = data.with_columns(
            pl.col('date'),
            (pl.col('dif') - pl.col('dea')).alias('macd'),
            pl.when(pl.col('dif') - pl.col('dea') >= 0).then(pl.lit('red')).otherwise(pl.lit('green')).alias('color')
        )
        print(data)
        data.write_parquet(Config.Paths.DataPath / self.types / self.symbol / f'macd_{self.period}.parquet')


class BollingerBands:

    def __init__(self, symbol, period='day', adjust='raw', types='stock'):
        self.symbol = symbol
        self.period = period
        self.adjust = adjust
        self.types = types

        self.bollinger_bands()

    def bollinger_bands(self, std_dev: int = 2):
        # 计算布林线（20 周期，2 倍标准差）
        raw_data = pl.read_parquet(
            Config.Paths.DataPath / self.types / self.symbol / f'{self.types}_{self.period}.parquet'
        )
        data = raw_data.select(
            [
                pl.col('date'),
                pl.col(f'{self.adjust}_close').rolling_mean(20).alias('boll_mid'),
                (pl.col(f'{self.adjust}_close').rolling_mean(20) + pl.col(f'{self.adjust}_close').rolling_std(20) * std_dev).alias('boll_upper'),
                (pl.col(f'{self.adjust}_close').rolling_mean(20) - pl.col(f'{self.adjust}_close').rolling_std(20) * std_dev).alias('boll_lower'),
            ]
        )
        print(data)
        data.write_parquet(Config.Paths.DataPath / self.types / self.symbol / f'boll_{self.period}.parquet')


if __name__ == '__main__':
    # for stock in Stocks:
    #     for period in ['day', 'week', 'month', 'quarter']:
    #         MovingAverage(stock, period=period)
    #         MovingAverageConvergenceDivergence(stock, period=period)
    #         BollingerBands(stock, period=period)

    MovingAverageConvergenceDivergence('sh600036', period='month', types='stock')

    # Plotting('sh600036', period='day')
