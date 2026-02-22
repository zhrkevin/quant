#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import polars as pl
from copy import deepcopy
from datetime import date
from algorithms.basic.plot import Plotting

from project.configuration import Config


class AscendTrend:

    def __init__(self, symbol: str, selected_date: date = None):
        self.symbol = symbol
        self.selected_date = selected_date

        print(f'\n趋势判断:')
        print(f'\n1、上升趋势')
        self.condition1()
        self.condition2()
        for period in ['day', 'week', 'month']:
            self.condition3(period)

    def condition1(self):
        print(f'(1) 周 20 价格突破周 60 价格 (首突), 同步判断是否站上 30 月线。')
        selected_date = date(2020, 9, 10) if self.selected_date is None else deepcopy(self.selected_date)
        
        stock_day = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / 'stock_day.parquet')
        selected_stock_day = stock_day.filter(pl.col('date') <= selected_date)
        stock_low = selected_stock_day['raw_low'][-1]

        ma_week = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / 'ma_week.parquet')
        selected_week_data = ma_week.filter(pl.col('date') <= selected_date)
        latest_ma_week_20, latest_ma_week_60 = selected_week_data['ma_20'][-1], selected_week_data['ma_60'][-1]
        previous_ma_week_20, previous_ma_week_60 = selected_week_data['ma_20'][-2], selected_week_data['ma_60'][-2]
        
        ma_month = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / 'ma_month.parquet')
        selected_ma_month = ma_month.filter(pl.col('date') <= selected_date)
        latest_ma_month_30 = selected_ma_month['ma_30'][-1]
   
        if latest_ma_week_20 > latest_ma_week_60 and previous_ma_week_20 < previous_ma_week_60 and stock_low > latest_ma_month_30:
            print(f'\n今日 ({selected_date}) 触发 MA-Week-20 上穿突破 MA-Week-60 并站上 MA-Month-30 线')
            print(f"本周 MA-Week-20 ({latest_ma_week_20:.4f}) MA-Week-60 ({latest_ma_week_60:.4f})")
            print(f"上周 MA-Week-20 ({previous_ma_week_20:.4f}) MA-Week-60 ({previous_ma_week_60:.4f})")
            print(f"本月 MA-Month-30 ({latest_ma_month_30:.4f}) 今日最低点 Stock Low ({stock_low:.4f})")

    def condition2(self):
        selected_date = date(2020, 10, 27) if self.selected_date is None else deepcopy(self.selected_date)

        stock_day = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / 'stock_day.parquet')
        selected_stock_day = stock_day.filter(pl.col('date') <= selected_date)
        stock_low = selected_stock_day['raw_low'][-1]

        ma_week = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / 'ma_week.parquet')
        selected_week_data = ma_week.filter(pl.col('date') <= selected_date)
        latest_ma_week_30, latest_ma_week_60 = selected_week_data['ma_30'][-1], selected_week_data['ma_60'][-1]
        previous_ma_week_30, previous_ma_week_60 = selected_week_data['ma_30'][-2], selected_week_data['ma_60'][-2]

        ma_month = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / 'ma_month.parquet')
        selected_ma_month = ma_month.filter(pl.col('date') <= selected_date)
        latest_ma_month_30 = selected_ma_month['ma_30'][-1]

        if latest_ma_week_30 > latest_ma_week_60 and previous_ma_week_30 < previous_ma_week_60 and stock_low > latest_ma_month_30:
            print(f'\n今日 ({selected_date}) 触发 MA-Week-30 上穿突破 MA-Week-60 并站上 MA-Month-30 线')
            print(f"本周 MA-Week-30 ({latest_ma_week_30:.4f}) MA-Week-60 ({latest_ma_week_60:.4f})")
            print(f"上周 MA-Week-30 ({previous_ma_week_30:.4f}) MA-Week-60 ({previous_ma_week_60:.4f})")
            print(f"本月 MA-Month-30 ({latest_ma_month_30:.4f}) 今日最低点 Stock Low ({stock_low:.4f})")

    def condition3(self, period: str = 'day'):
        selected_date = date(2024, 10, 14) if self.selected_date is None else deepcopy(self.selected_date)

        macd_data = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / f'macd_{period}.parquet')
        selected_macd_data = macd_data.filter(pl.col('date') <= selected_date)
        latest_dif = selected_macd_data['dif'][-1]
        latest_dea = selected_macd_data['dea'][-1]

        ma = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / f'ma_{period}.parquet')
        selected_ma = ma.filter(pl.col('date') <= selected_date)
        latest_ma_10 = selected_ma['ma_10'][-1]
        latest_ma_20 = selected_ma['ma_20'][-1]
        latest_ma_30 = selected_ma['ma_30'][-1]
        latest_ma_60 = selected_ma['ma_60'][-1]

        if latest_dif > 0 and latest_dif > latest_dea and latest_ma_10 > latest_ma_20 > latest_ma_30 > latest_ma_60:
            print(f'\n今日 ({selected_date}) {period.title()} 趋势触发 MACD 线突破 0 轴且呈现多头排列')
            print(f"当前 MACD 线 DIF ({latest_dif:.4f}), DEA ({latest_dea:.4f})")
            print(f"当前 MA-10 ({latest_ma_10:.4f}) MA-20 ({latest_ma_20:.4f}) MA-30 ({latest_ma_30:.4f}) MA-60 ({latest_ma_60:.4f})")


class DescendSignal:

    def __init__(self, symbol: str, selected_date: date = None):
        self.symbol = symbol
        self.selected_date = selected_date

        self.condition1()
        self.condition2()
        self.condition3()
        self.condition4()

    def condition1(self):
        selected_date = date(2025, 11, 15) if self.selected_date is None else deepcopy(self.selected_date)

        ma_day = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / 'ma_day.parquet')
        selected_ma_day = ma_day.filter(pl.col('date') <= selected_date)
        latest_ma_day_60, latest_ma_day_250 = selected_ma_day['ma_60'][-1], selected_ma_day['ma_250'][-1]
        previous_ma_day_60, previous_ma_day_250 = selected_ma_day['ma_60'][-2], selected_ma_day['ma_250'][-2]

        if latest_ma_day_60 < latest_ma_day_250 and previous_ma_day_60 > previous_ma_day_250:
            print(f'\n今日 ({selected_date}) 触发 MA-Day-60 下穿跌破 MA-250-Day 线')
            print(f"本日 MA-Day-60 ({latest_ma_day_60:.4f}) MA-250-Day ({latest_ma_day_250:.4f})")
            print(f"前日 MA-Day-60 ({previous_ma_day_60:.4f}) MA-250-Day ({previous_ma_day_250:.4f})")

    def condition2(self):
        selected_date = date(2022, 1, 25) if self.selected_date is None else deepcopy(self.selected_date)

        ma_week = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / 'ma_week.parquet')
        selected_ma_week = ma_week.filter(pl.col('date') <= selected_date)
        latest_ma_week_20, latest_ma_week_60 = selected_ma_week['ma_20'][-1], selected_ma_week['ma_60'][-1]
        previous_ma_week_20, previous_ma_week_60 = selected_ma_week['ma_20'][-2], selected_ma_week['ma_60'][-2]

        if latest_ma_week_20 < latest_ma_week_60 and previous_ma_week_20 > previous_ma_week_60:
            print(f'\n今日 ({selected_date}) 触发 MA-Week-20 下穿跌破 MA-Week-60 线')
            print(f"本周 MA-Week-20 ({latest_ma_week_20:.4f}) MA-Week-60 ({latest_ma_week_60:.4f})")
            print(f"前周 MA-Week-20 ({previous_ma_week_20:.4f}) MA-Week-60 ({previous_ma_week_60:.4f})")

    def condition3(self):
        selected_date = date(2022, 1, 11) if self.selected_date is None else deepcopy(self.selected_date)
                
        ma_week = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / 'ma_week.parquet')
        selected_ma_week = ma_week.filter(pl.col('date') <= selected_date)
        latest_ma_week_30, previous_ma_week_30 = selected_ma_week['ma_30'][-1], selected_ma_week['ma_30'][-2]
        latest_ma_week_60, previous_ma_week_60 = selected_ma_week['ma_60'][-1], selected_ma_week['ma_60'][-2]

        if latest_ma_week_30 < latest_ma_week_60 and previous_ma_week_30 > previous_ma_week_60:
            print(f'\n今日 ({selected_date}) 触发 MA-Week-30 下穿跌破 MA-Week-60 线')
            print(f"本周 MA-Week-30 ({latest_ma_week_30:.4f}) MA-Week-60 ({latest_ma_week_60:.4f})")
            print(f"前周 MA-Week-30 ({previous_ma_week_30:.4f}) MA-Week-60 ({previous_ma_week_60:.4f})")
        
    def condition4(self):
        selected_date = date(2022, 3, 9) if self.selected_date is None else deepcopy(self.selected_date)
                
        ma_month = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / 'ma_month.parquet')
        selected_ma_month = ma_month.filter(pl.col('date') <= selected_date)
        latest_ma_month_30, previous_ma_month_30 = selected_ma_month['ma_30'][-1], selected_ma_month['ma_30'][-2]

        stock_data = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / 'stock_day.parquet')
        selected_stock_data = stock_data.filter(pl.col('date') <= selected_date)
        latest_stock_low, previous_stock_low = selected_stock_data['raw_low'][-1], selected_stock_data['raw_low'][-2]
        
        if latest_stock_low < latest_ma_month_30 and previous_stock_low > previous_ma_month_30:
            print(f'\n今日 ({selected_date}) 触发下穿跌破 MA-Month-30 线')
            print(f"本月 MA-Month-30 ({latest_ma_month_30:.4f})")
            print(f"今日最低点 Stock Low ({latest_stock_low:.4f})")
            print(f"昨日最低点 Stock Low ({previous_stock_low:.4f})")
        
        Plotting('sh600036', period='week', window=500)


class ConsolidationSignal:

    def __init__(self, symbol: str, selected_date: date = None, adjust: str = 'raw'):
        self.symbol = symbol
        self.selected_date = selected_date
        self.adjust = adjust

        self.condition1()
        self.condition2()
        self.condition3()

    def condition1(self):
        selected_date = date(2021, 11, 1) if self.selected_date is None else deepcopy(self.selected_date)

        macd_data = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / f'macd_month.parquet')
        selected_macd_data = macd_data.filter(pl.col('date') <= selected_date)
        latest_dif = selected_macd_data['dif'][-1]
        latest_dea = selected_macd_data['dea'][-1]
        latest_macd = selected_macd_data['macd'][-1]

        ma_month = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / 'ma_month.parquet')
        selected_ma_month = ma_month.filter(pl.col('date') <= selected_date)
        latest_ma_month_30 = selected_ma_month['ma_30'][-1]

        stock_data = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / 'stock_day.parquet')
        selected_stock_data = stock_data.filter(pl.col('date') <= selected_date)
        latest_stock_low = selected_stock_data[f'{self.adjust}_low'][-1]

        if latest_dif > 0 and latest_dif <= latest_dea and latest_macd < 0:
            print(f'\n今日 ({selected_date}) 触发 MACD 月小坑')
            print(f"今日 DIF ({latest_dif:.4f}) DEA ({latest_dea:.4f}) MACD ({latest_macd:.4f})")
            if latest_stock_low < latest_ma_month_30:
                print(f"蜡烛 Stock Low ({latest_stock_low:.4f}) < MA-Month-30 ({latest_ma_month_30:.4f})")
            elif latest_stock_low > latest_ma_month_30:
                print(f"蜡烛 Stock Low ({latest_stock_low:.4f}) > MA-Month-30 ({latest_ma_month_30:.4f})")
            else:
                print(f"蜡烛 Stock Low ({latest_stock_low:.4f}) = MA-Month-30 ({latest_ma_month_30:.4f})")

    def condition2(self):
        selected_date = date(2025, 1, 2) if self.selected_date is None else deepcopy(self.selected_date)

        macd_data = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / f'macd_month.parquet')
        selected_macd_data = macd_data.filter(pl.col('date') <= selected_date)
        latest_dif = selected_macd_data['dif'][-1]
        latest_dea = selected_macd_data['dea'][-1]
        latest_macd = selected_macd_data['macd'][-1]
        previous_dif = selected_macd_data['dif'][-2]
        previous_dea = selected_macd_data['dea'][-2]
        previous_macd = selected_macd_data['macd'][-2]

        boll_week = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / 'boll_week.parquet')
        selected_boll_week = boll_week.filter(pl.col('date') <= selected_date)
        latest_boll_week = selected_boll_week['boll_lower'][-1]

        stock_data = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / 'stock_day.parquet')
        selected_stock_data = stock_data.filter(pl.col('date') <= selected_date)
        latest_stock_low = selected_stock_data[f'{self.adjust}_low'][-1]
        
        # sh.600036 无数据点回测，待其他票用例测试
        if latest_dif > 0 and previous_dif < 0 and latest_dea < 0 and previous_dea < 0 and latest_stock_low < latest_boll_week:
            print(f'\n今日 ({selected_date}) 触发坑内出且蜡烛 Stock Low ({latest_stock_low:.4f}) 碰 Boll 周下轨 ({latest_boll_week:.4f})')
            print(f"今日 DIF ({latest_dif:.4f}) DEA ({latest_dea:.4f}) MACD ({latest_macd:.4f})")
            print(f"前日 DIF ({previous_dif:.4f}) DEA ({previous_dea:.4f}) MACD ({previous_macd:.4f})")

    def condition3(self):
        selected_date = date(2024, 3, 1) if self.selected_date is None else deepcopy(self.selected_date)

        macd_data = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / f'macd_month.parquet')
        selected_macd_data = macd_data.filter(pl.col('date') <= selected_date)
        latest_dif = selected_macd_data['dif'][-1]
        latest_dea = selected_macd_data['dea'][-1]
        latest_macd = selected_macd_data['macd'][-1]
        previous_dif = selected_macd_data['dif'][-2]
        previous_dea = selected_macd_data['dea'][-2]
        previous_macd = selected_macd_data['macd'][-2]

        if 0 >= latest_dif >= latest_dea and 0 >= previous_dea >= previous_dif:
            print(f"\n今日 ({selected_date}) 触发水下金叉")
            print(f"今日 DIF ({latest_dif:.4f}) DEA ({latest_dea:.4f}) MACD ({latest_macd:.4f})")
            print(f"前日 DIF ({previous_dif:.4f}) DEA ({previous_dea:.4f}) MACD ({previous_macd:.4f})")


if __name__ == '__main__':
    AscendSignal('sh600036')
    DescendSignal('sh600036')
    ConsolidationSignal('sh600036')
    Plotting('sh600036', period='week', window=100)
