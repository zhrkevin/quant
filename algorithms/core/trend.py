#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import polars as pl
from copy import deepcopy
from datetime import date, timedelta
from algorithms.basic.plot import Plotting
from algorithms.basic.stocks import Stocks

from project.configuration import Config


print(f'\n趋势判断:')

class AscendTrend:

    def __init__(self, symbol: str, selected_date: date = None, window: int = 2, adjust: str = 'raw'):
        self.symbol = symbol
        self.selected_date = selected_date
        self.window = window
        self.adjust = adjust

        print(f'\n1、上升趋势')
        self.condition1()
        self.condition2()
        self.condition3()

    def condition1(self):
        print(f'\n(1) 周 20 价格突破周 60 价格 (首突)，同步判断是否站上 30 月线。')
        selected_date = date(2026, 1, 3) if self.selected_date is None else deepcopy(self.selected_date)
        limited_date = selected_date - timedelta(days=365*self.window + 7)

        date_filter = (pl.col('date') <= selected_date) & (pl.col('date') >= limited_date)
        selected_stock_month = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / 'stock_month.parquet').filter(date_filter)
        selected_ma_week = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / f'ma_week.parquet').filter(date_filter)
        selected_ma_month = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / 'ma_month.parquet').filter(date_filter)

        breakthrough = selected_ma_week.filter(
            (pl.col('ma_20').shift(1) < pl.col('ma_60').shift(1)) & 
            (pl.col('ma_20') > pl.col('ma_60'))
        ).drop_nulls()['date']
        print(f'@ 触发 MA-Week-20 首次突破 MA-Week-60：\n{[d.strftime("%Y-%m-%d") for d in breakthrough]}') if not breakthrough.is_empty() else None

        standup = selected_stock_month.join(
            selected_ma_month.select('date', 'ma_30'), on='date'
        ).filter(
            (pl.col(f'{self.adjust}_low').shift(1) < pl.col('ma_30').shift(1)) & 
            (pl.col(f'{self.adjust}_low') > pl.col('ma_30'))
        ).drop_nulls()['date']
        print(f'@ 触发 Stock-Low 站上 MA-Month-30 线：\n{[d.strftime("%Y-%m-%d") for d in standup]}') if not standup.is_empty() else None

    def condition2(self):
        print(f'\n(2) 周 30 价格突破周 60 价格 (再次确认首突)，同步判断是否站上 30 月线。')
        selected_date = date(2026, 1, 3) if self.selected_date is None else deepcopy(self.selected_date)
        limited_date = selected_date - timedelta(days=365*self.window + 7)

        date_filter = (pl.col('date') <= selected_date) & (pl.col('date') >= limited_date)
        selected_stock_month = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / 'stock_month.parquet').filter(date_filter)
        selected_ma_week = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / 'ma_week.parquet').filter(date_filter)
        selected_ma_month = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / 'ma_month.parquet').filter(date_filter)

        breakthrough = selected_ma_week.filter(
            (pl.col('ma_30').shift(1) < pl.col('ma_60').shift(1)) & 
            (pl.col('ma_30') > pl.col('ma_60'))
        ).drop_nulls()['date']
        print(f'@ 触发 MA-Week-30 首次突破 MA-Week-60：\n{[d.strftime("%Y-%m-%d") for d in breakthrough]}') if not breakthrough.is_empty() else None

        standup = selected_stock_month.join(
            selected_ma_month.select('date', 'ma_30'), on='date'
        ).filter(
            (pl.col(f'{self.adjust}_low').shift(1) < pl.col('ma_30').shift(1)) & 
            (pl.col(f'{self.adjust}_low') > pl.col('ma_30'))
        ).drop_nulls()['date']
        print(f'@ 触发 Stock-Low 站上 MA-Month-30 线：\n{[d.strftime("%Y-%m-%d") for d in standup]}') if not standup.is_empty() else None

    def condition3(self):
        print(f'\n(3) MACD 突破0轴后回踩均线，缠绕，变成多头趋势 (短线看日线，长线看周、月、季线)。')
        selected_date = date(2026, 1, 3) if self.selected_date is None else deepcopy(self.selected_date)
        limited_date = selected_date - timedelta(days=365*self.window + 7)

        for period in ['week', 'month', 'quarter']:
            date_filter = (pl.col('date') <= selected_date) & (pl.col('date') >= limited_date)
            selected_macd = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / f'macd_{period}.parquet').filter(date_filter)
            selected_ma = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / f'ma_{period}.parquet').filter(date_filter)

            breakthrough = selected_macd.filter(
                (pl.col('dif').shift(1) < 0) & 
                (pl.col('dif') >= 0) & 
                (pl.col('dif') > pl.col('dea'))
            ).drop_nulls()['date']
            print(f'@ 触发 MACD-{period.title()} 突破 0 轴：\n{[d.strftime("%Y-%m-%d") for d in breakthrough]}') if not breakthrough.is_empty() else None

            longposition = selected_ma.filter(
                (pl.col('ma_10') > pl.col('ma_20')) & 
                (pl.col('ma_20') > pl.col('ma_30')) & 
                (pl.col('ma_30') > pl.col('ma_60'))
            )['date']
            print(f'@ 触发 MA-{period.title()} 多头排列：') if not longposition.is_empty() else None 

            groups, date_indices = [], {date: idx for idx, date in enumerate(selected_ma['date'])}
            for i, d in enumerate(longposition):
                if i == 0 or date_indices[d] != date_indices[longposition[i-1]] + 1:
                    groups.append([d])
                else:
                    groups[-1].append(d)
            for i, group in enumerate(groups):
                print(f'时间区间 {i+1}：[{group[0] if len(group) == 1 else f"{group[0]} ~ {group[-1]}"}]')


class DescendTrend:

    def __init__(self, symbol: str, selected_date: date = None, window: int = 2, adjust: str = 'raw'):
        self.symbol = symbol
        self.selected_date = selected_date
        self.window = window
        self.adjust = adjust

        print(f'\n3、下跌趋势')
        self.condition1()
        self.condition2()
        self.condition3()
        self.condition4()

    def condition1(self):
        print(f'\n(1) 日 60 跌破日 250。')
        selected_date = date(2026, 1, 3) if self.selected_date is None else deepcopy(self.selected_date)
        limited_date = selected_date - timedelta(days=365*self.window + 7)

        date_filter = (pl.col('date') <= selected_date) & (pl.col('date') >= limited_date)
        selected_ma_day = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / 'ma_day.parquet').filter(date_filter)

        fallbelow = selected_ma_day.filter(
            (pl.col('ma_60').shift(1) > pl.col('ma_250').shift(1)) & 
            (pl.col('ma_60') < pl.col('ma_250'))
        ).drop_nulls()['date']
        print(f'@ 触发 MA-Day-60 跌破 MA-Day-250：\n{[d.strftime("%Y-%m-%d") for d in fallbelow]}') if not fallbelow.is_empty() else None

    def condition2(self):
        print(f'\n(2) 周 20 跌破周 60。')
        selected_date = date(2026, 1, 3) if self.selected_date is None else deepcopy(self.selected_date)
        limited_date = selected_date - timedelta(days=365*self.window + 7)

        date_filter = (pl.col('date') <= selected_date) & (pl.col('date') >= limited_date)
        selected_ma_week = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / 'ma_week.parquet').filter(date_filter)

        fallbelow = selected_ma_week.filter(
            (pl.col('ma_20').shift(1) > pl.col('ma_60').shift(1)) & 
            (pl.col('ma_20') < pl.col('ma_60'))
        ).drop_nulls()['date']
        print(f'@ 触发 MA-Week-20 跌破 MA-Week-60：\n{[d.strftime("%Y-%m-%d") for d in fallbelow]}') if not fallbelow.is_empty() else None

    def condition3(self):
        print(f'\n(3) 周 30 跌破周 60。')
        selected_date = date(2026, 1, 3) if self.selected_date is None else deepcopy(self.selected_date)
        limited_date = selected_date - timedelta(days=365*self.window + 7)

        date_filter = (pl.col('date') <= selected_date) & (pl.col('date') >= limited_date)
        selected_ma_week = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / 'ma_week.parquet').filter(date_filter)

        fallbelow = selected_ma_week.filter(
            (pl.col('ma_30').shift(1) > pl.col('ma_60').shift(1)) & 
            (pl.col('ma_30') < pl.col('ma_60'))
        ).drop_nulls()['date']
        print(f'@ 触发 MA-Week-30 跌破 MA-Week-60：\n{[d.strftime("%Y-%m-%d") for d in fallbelow]}') if not fallbelow.is_empty() else None
        
    def condition4(self):
        print(f'\n(4) 跌破月 30。')
        selected_date = date(2026, 1, 3) if self.selected_date is None else deepcopy(self.selected_date)
        limited_date = selected_date - timedelta(days=365*self.window + 7)

        date_filter = (pl.col('date') <= selected_date) & (pl.col('date') >= limited_date)
        selected_stock_month = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / 'stock_month.parquet').filter(date_filter)
        selected_ma_month = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / 'ma_month.parquet').filter(date_filter)

        fallbelow = selected_stock_month.join(
            selected_ma_month.select('date', 'ma_30'), on='date'
        ).filter(
            (pl.col(f'{self.adjust}_low').shift(1) > pl.col('ma_30').shift(1)) & 
            (pl.col(f'{self.adjust}_low') < pl.col('ma_30'))
        ).drop_nulls()['date']
        print(f'@ 触发 Stock-Low 下穿跌破 MA-Month-30 线：\n{[d.strftime("%Y-%m-%d") for d in fallbelow]}') if not fallbelow.is_empty() else None


class SmallFluctuations:

    def __init__(self, symbol: str, selected_date: date = None, window: int = 2, adjust: str = 'raw'):
        self.symbol = symbol
        self.selected_date = selected_date
        self.window = window
        self.adjust = adjust

        print(f'\n4、小调整')
        self.condition1()
        self.condition2()

    def condition1(self):
        print(f'(1) 月小坑，参考月 30 价格。')
        selected_date = date(2026, 1, 3) if self.selected_date is None else deepcopy(self.selected_date)
        limited_date = selected_date - timedelta(days=365*self.window + 7)

        date_filter = (pl.col('date') <= selected_date) & (pl.col('date') >= limited_date)
        selected_macd_month = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / f'macd_month.parquet').filter(date_filter)
        selected_ma_month = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / 'ma_month.parquet').filter(date_filter)
        selected_stock_month = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / 'stock_month.parquet').filter(date_filter)

        smallmonthhole = selected_macd_month.filter(
            (pl.col('macd') < 0) & 
            (pl.col('dif') > 0) & 
            (pl.col('dif') < pl.col('dea'))
        ).drop_nulls()['date']
        print(f'@ 触发 MACD-Month 月小坑：') if not smallmonthhole.is_empty() else None
        if not smallmonthhole.is_empty():         
            groups, date_indices = [], {date: idx for idx, date in enumerate(selected_macd_month['date'])}
            for i, d in enumerate(smallmonthhole):
                if i == 0 or date_indices[d] != date_indices[smallmonthhole[i-1]] + 1:
                    groups.append([d])
                else:
                    groups[-1].append(d)
            for i, group in enumerate(groups):
                print(f'时间区间 {i+1}: [{group[0] if len(group) == 1 else f"{group[0]} ~ {group[-1]}"}]')

        fallbelow = selected_stock_month.join(
            selected_ma_month.select('date', 'ma_30'), on='date'
        ).filter(
            (pl.col(f'{self.adjust}_low') >= pl.col('ma_30'))
        ).drop_nulls()['date']
        intersection = smallmonthhole.filter(smallmonthhole.is_in(fallbelow.implode()))
        print(f'@ 始终站上 MA-Month-30 线：') if not intersection.is_empty() else None

        if not intersection.is_empty():
            groups, date_indices = [], {date: idx for idx, date in enumerate(selected_macd_month['date'])}
            for i, d in enumerate(intersection):
                if i == 0 or date_indices[d] != date_indices[intersection[i-1]] + 1:    
                    groups.append([d])
                else:
                    groups[-1].append(d)
            for i, group in enumerate(groups):
                print(f'时间区间 {i+1}: [{group[0] if len(group) == 1 else f"{group[0]} ~ {group[-1]}"}]')

    def condition2(self):
        print(f'\n(2) 坑内出来以后，Boll 周下是买点。')
        selected_date = date(2026, 1, 3) if self.selected_date is None else deepcopy(self.selected_date)
        limited_date = selected_date - timedelta(days=365*self.window + 7)

        date_filter = (pl.col('date') <= selected_date) & (pl.col('date') >= limited_date)
        selected_macd_month = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / f'macd_month.parquet').filter(date_filter)
        selected_boll_week = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / f'boll_week.parquet').filter(date_filter)

        outtingmonthhole = selected_macd_month.filter(
            (pl.col('dif') > 0) & 
            (pl.col('dif').shift(1) < 0) & 
            (pl.col('dea') < 0) & 
            (pl.col('dea').shift(1) < 0) & 
            (pl.col('dif') > pl.col('dea'))
        ).drop_nulls()['date']
        print(f'@ 触发 MACD-Month 坑内出：{[d.strftime("%Y-%m-%d") for d in outtingmonthhole]}') if not outtingmonthhole.is_empty() else None

        # latest_stock_low < latest_boll_week


if __name__ == '__main__':
    for stock in Stocks:
        AscendTrend(stock)
        DescendTrend(stock)
        SmallFluctuations(stock)
    
    # Plotting('sh600036', period='week', window=200)
    # Plotting('sh600036', period='month')
