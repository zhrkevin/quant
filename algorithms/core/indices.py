#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import polars as pl
from algorithms.core.plot import Plotting

from project.configuration import Config


pl.Config(tbl_rows=20, tbl_cols=-1)


class MovingAverage:

    def __init__(self, symbol: str, period: str = 'day'):
        self.symbol = symbol
        self.period = period

        self.stock_data = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / f'stock_{self.period}.parquet')
        self.data = None
        
        self.moving_average()

    def moving_average(self):
        self.data = self.stock_data.select(
            [
                pl.col('date'),
                pl.col('raw_close').rolling_mean(5).alias('ma_5'),      # 005 trading period
                pl.col('raw_close').rolling_mean(10).alias('ma_10'),    # 010 trading period
                pl.col('raw_close').rolling_mean(20).alias('ma_20'),    # 020 trading period
                pl.col('raw_close').rolling_mean(30).alias('ma_30'),    # 030 trading period
                pl.col('raw_close').rolling_mean(60).alias('ma_60'),    # 060 trading period
                pl.col('raw_close').rolling_mean(250).alias('ma_250'),  # 250 trading period
            ]
        )
        print(self.data)
        self.data.write_parquet(Config.Paths.DataPath / 'input' / self.symbol / f'ma_{self.period}.parquet')


class MovingAverageConvergenceDivergence:

    def __init__(self, symbol: str, period: str = 'day'):
        self.symbol = symbol
        self.period = period
        self.data = None
        self.stock_data = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / f'stock_{self.period}.parquet')
        
        self.macd()

    def macd(self, fast: int = 12, slow: int = 26, span: int = 9):
        # 计算指数移动平均线（EMA）
        ema = pl.DataFrame(
            {
                'date': self.stock_data['date'],
                'ema_fast': self.stock_data.select(
                    [pl.col('raw_close').ewm_mean(span=fast, adjust=False)]
                ),
                'ema_slow': self.stock_data.select(
                    [pl.col('raw_close').ewm_mean(span=slow, adjust=False)]
                ),
            }
        )
        # 计算 DIF 快线, DEA 慢线
        self.data = pl.DataFrame(
            {
                'date': ema['date'],
                'dif': ema['ema_fast'] - ema['ema_slow'],
                'dea': (ema['ema_fast'] - ema['ema_slow']).ewm_mean(span=span, adjust=False),
            }
        )
        # 计算 MACD 柱状图
        self.data = self.data.with_columns(
            pl.col('date'),
            (pl.col('dif') - pl.col('dea')).alias('macd'),
            pl.when(pl.col('dif') - pl.col('dea') >= 0).then(pl.lit('red')).otherwise(pl.lit('green')).alias('color')
        )
        print(self.data)
        self.data.write_parquet(Config.Paths.DataPath / 'input' / self.symbol / f'macd_{self.period}.parquet')


class BollingerBands:

    def __init__(self, symbol: str, period: str = 'day'):
        self.symbol = symbol
        self.period = period
        self.data = None
        self.stock_data = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / f'stock_{self.period}.parquet')

        self.bollinger_bands()

    def bollinger_bands(self, std_dev: int = 2):
        # 计算布林线（20 周期，2 倍标准差）
        self.data = self.stock_data.select(
            [
                pl.col('date'),
                pl.col('raw_close').rolling_mean(20).alias('boll_mid'),
                (pl.col('raw_close').rolling_mean(20) + pl.col('raw_close').rolling_std(20) * std_dev).alias('boll_upper'),
                (pl.col('raw_close').rolling_mean(20) - pl.col('raw_close').rolling_std(20) * std_dev).alias('boll_lower'),
            ]
        )
        print(self.data)
        self.data.write_parquet(Config.Paths.DataPath / 'input' / self.symbol / f'boll_{self.period}.parquet')


if __name__ == '__main__':
    for period in ['day', 'week', 'month']:
        MovingAverage('sh600036', period=period)
        MovingAverageConvergenceDivergence('sh600036', period=period)
        BollingerBands('sh600036', period=period)

    Plotting('sh600036', period='day')
