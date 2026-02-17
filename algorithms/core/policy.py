#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

from re import S
import polars as pl
from algorithms.core.plot import Plotting

from project.configuration import Config


class PolicyUp:

    def __init__(self, symbol: str):
        self.symbol = symbol

    def run(self, selected_date: pl.Date = None):
        self.condition1(selected_date)
        self.condition2(selected_date)

        for period in ['day', 'week', 'month']:
            self.condition3(selected_date, period)

    def condition1(self, selected_date):
        selected_date = selected_date or pl.date(2020, 9, 10) 

        stock_data = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / f'stock_day.parquet')
        ma_data_week = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / f'ma_week.parquet')
        ma_data_month = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / f'ma_month.parquet')
        
        selected_stock_data = stock_data.filter(pl.col('date') <= selected_date)
        stock_low = selected_stock_data['raw_low'][-1]

        selected_week_data = ma_data_week.filter(pl.col('date') <= selected_date)
        latest_ma_20_week = selected_week_data['ma_20'][-1]
        latest_ma_60_week = selected_week_data['ma_60'][-1]
        previous_ma_20_week = selected_week_data['ma_20'][-2]
        previous_ma_60_week = selected_week_data['ma_60'][-2]
        
        selected_month_data = ma_data_month.filter(pl.col('date') <= selected_date)
        latest_ma_30_month = selected_month_data['ma_30'][-1]

        print(f"\n当前周: {selected_week_data['date'][-1].strftime('%Y-%m-%d')}")
        print(f"本周 MA-20-Week: {latest_ma_20_week:.4f}, MA-60-Week: {latest_ma_60_week:.4f}")
        print(f"上周 MA-20-Week: {previous_ma_20_week:.4f}, MA-60-Week: {previous_ma_60_week:.4f}")

        print(f"当前月: {selected_month_data['date'][-1].strftime('%Y-%m-%d')}")
        print(f"本月 MA-30-Month: {latest_ma_30_month:.4f}, 本月蜡烛最低点 Stock Low: {stock_low:.4f}")

        print(f"\n今日: {pl.select(selected_date).item()}")        
        if latest_ma_20_week > latest_ma_60_week and previous_ma_20_week < previous_ma_60_week:
            if stock_low > latest_ma_30_month:
                print('MA-20-Week 上穿突破 MA-60-Week 并站上 MA-30-Month 线')
        else:
            print('未触发')

    def condition2(self, selected_date):
        selected_date = selected_date or pl.date(2020, 10, 27) 

        stock_data = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / f'stock_day.parquet')
        ma_data_week = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / f'ma_week.parquet')
        ma_data_month = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / f'ma_month.parquet')
        
        selected_stock_data = stock_data.filter(pl.col('date') <= selected_date)
        stock_low = selected_stock_data['raw_low'][-1]

        selected_week_data = ma_data_week.filter(pl.col('date') <= selected_date)
        latest_ma_30_week = selected_week_data['ma_30'][-1]
        latest_ma_60_week = selected_week_data['ma_60'][-1]
        previous_ma_30_week = selected_week_data['ma_30'][-2]
        previous_ma_60_week = selected_week_data['ma_60'][-2]
        
        selected_month_data = ma_data_month.filter(pl.col('date') <= selected_date)
        latest_ma_30_month = selected_month_data['ma_30'][-1]

        print(f"\n今日: {selected_date}")
        print(f"本周 MA-30-Week: {latest_ma_30_week:.4f}, MA-60-Week: {latest_ma_60_week:.4f}")
        print(f"上周 MA-30-Week: {previous_ma_30_week:.4f}, MA-60-Week: {previous_ma_60_week:.4f}")
        print(f"当前月: {selected_month_data['date'][-1].strftime('%Y-%m-%d')}")
        print(f"本月 MA-30-Month: {latest_ma_30_month:.4f}, 本月蜡烛最低点 Stock Low: {stock_low:.4f}")
        
        if latest_ma_30_week > latest_ma_60_week and previous_ma_30_week < previous_ma_60_week:
            if stock_low > latest_ma_30_month:
                print(f"触发日: {selected_week_data['date'][-1].strftime('%Y-%m-%d')}")
                print('MA-30-Week 上穿突破 MA-60-Week 并站上 MA-30-Month 线')
        else:
            print('未触发')

    def condition3(self, selected_date, period: str = 'day'):
        selected_date = selected_date if selected_date else pl.date(2025, 12, 30) 

        macd_data = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / f'macd_{period}.parquet')
        ma_data = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / f'ma_{period}.parquet')
        
        print(f"\n今日: {pl.select(selected_date).item()}")
        selected_macd_data = macd_data.filter(pl.col('date') <= selected_date)

        latest_dif = selected_macd_data['dif'][-1]
        latest_dea = selected_macd_data['dea'][-1]
        print(f"当前 MACD 线: DIF={latest_dif:.4f}, DEA={latest_dea:.4f}")
        
        if latest_dif > 0 and latest_dif > latest_dea:
            print('MACD 线突破 0 轴')
        else:
            print('未触发')

        selected_ma_data = ma_data.filter(pl.col('date') <= selected_date)
        latest_ma_5_day = selected_ma_data['ma_5'][-1]
        latest_ma_10_day = selected_ma_data['ma_10'][-1]
        latest_ma_20_day = selected_ma_data['ma_20'][-1]
        latest_ma_30_day = selected_ma_data['ma_30'][-1]
        latest_ma_60_day = selected_ma_data['ma_60'][-1]
        print(f"当前 MA-5-Day: {latest_ma_5_day:.4f}, MA-10-Day: {latest_ma_10_day:.4f}, MA-20-Day: {latest_ma_20_day:.4f}, MA-30-Day: {latest_ma_30_day:.4f}, MA-60-Day: {latest_ma_60_day:.4f}")

        if latest_ma_5_day > latest_ma_10_day > latest_ma_20_day > latest_ma_30_day > latest_ma_60_day:
            print(f'{period} 趋势线呈现多头排列 Yes')
        else:
            print(f'{period} 趋势线未呈现多头排列 No')


class PolicyDown:

    def __init__(self, symbol: str):
        self.symbol = symbol

    def run(self, selected_date=None):
        self.condition1(selected_date)
        self.condition2(selected_date)

    def condition1(self, selected_date):
        selected_date = pl.date(2025, 11, 15) if not selected_date else selected_date

        ma_data_day = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / f'ma_day.parquet')

        selected_ma_data = ma_data_day.filter(pl.col('date') <= selected_date)
        latest_ma_60_day = selected_ma_data['ma_60'][-1]
        latest_ma_250_day = selected_ma_data['ma_250'][-1]
        previous_ma_60_day = selected_ma_data['ma_60'][-2]
        previous_ma_250_day = selected_ma_data['ma_250'][-2]

        print(f"\n今日: {pl.select(selected_date).item()}")
        print(f"本日 MA-60-Day: {latest_ma_60_day:.4f}, MA-250-Day: {latest_ma_250_day:.4f}")
        print(f"前日 MA-60-Day: {previous_ma_60_day:.4f}, MA-250-Day: {previous_ma_250_day:.4f}")

        if latest_ma_60_day < latest_ma_250_day and previous_ma_60_day > previous_ma_250_day:
            print(f"触发日: {selected_ma_data['date'][-1].strftime('%Y-%m-%d')}")
            print('MA-60-Day 下穿跌破 MA-250-Day 线')
        else:
            print('未触发')

    def condition2(self, selected_date):
        selected_date = selected_date or pl.date(2020, 9, 10) 
                
        ma_data_week = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / f'ma_week.parquet')

        selected_ma_data = ma_data_week.filter(pl.col('date') <= selected_date)
        latest_ma_20_week = selected_ma_data['ma_20'][-1]
        latest_ma_60_week = selected_ma_data['ma_60'][-1]
        previous_ma_20_week = selected_ma_data['ma_20'][-2]
        previous_ma_60_week = selected_ma_data['ma_60'][-2]

        print(f"\n今日: {pl.select(selected_date).item()}")
        print(f"本日 MA-20-Week: {latest_ma_20_week:.4f}, MA-60-Week: {latest_ma_60_week:.4f}")
        print(f"前日 MA-20-Week: {previous_ma_20_week:.4f}, MA-60-Week: {previous_ma_60_week:.4f}")

        if latest_ma_20_week < latest_ma_60_week and previous_ma_20_week > previous_ma_60_week:
            print(f"触发日: {selected_ma_data['date'][-1].strftime('%Y-%m-%d')}")
            print('MA-20-Week 下穿跌破 MA-60-Week 线')
        else:
            print('未触发')

        Plotting('sh600036', period='week')


if __name__ == '__main__':
    policy = PolicyDown('sh600036')
    policy.run(selected_date=pl.date(2025, 11, 15))
