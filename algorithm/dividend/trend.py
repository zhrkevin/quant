#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import copy
import datetime
import polars as pl

from project.configuration import Config
from algorithm.basic.printf import Printf


class AscendTrend:

    def __init__(self, symbol, product='stock', selected_date=None, adjust='raw', output='report-test.xlsx'):
        self.symbol = symbol
        self.selected_date = selected_date or datetime.date.today()
        self.adjust = adjust
        self.output = output
        self.product = product

        Printf.info(f'\n1、上升趋势')
        self.condition1()
        self.condition2()
        self.condition3()

    def condition1(self, window=60):
        Printf.info(f'(1) 周 20 价格突破周 60 价格 (首突)，同步判断是否站上 30 月线。')
        selected_date = datetime.date(2026, 1, 3) if self.selected_date is None else copy.deepcopy(self.selected_date)
        limited_date = selected_date - datetime.timedelta(days=window + 7)

        date_filter = (pl.col('date') <= selected_date) & (pl.col('date') >= limited_date)
        selected_stock_month = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / 'month.parquet').filter(date_filter)
        selected_ma_week = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / f'ma_week.parquet').filter(date_filter)
        selected_ma_month = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / 'ma_month.parquet').filter(date_filter)

        breakthrough = selected_ma_week.filter(
            (pl.col('ma_20').shift(1) < pl.col('ma_60').shift(1)) & 
            (pl.col('ma_20') > pl.col('ma_60'))
        ).drop_nulls()['date']
        Printf.info(f'@ 触发 MA-Week-20 首次突破 MA-Week-60：\n{[d.strftime("%Y-%m-%d") for d in breakthrough]}') if not breakthrough.is_empty() else None

        if selected_date in breakthrough:
            report = pl.read_excel(Config['Paths']['DataPath'] / 'output' / self.output)
            report = report.with_columns(
                pl.when(pl.col('编号') == self.symbol).then(pl.lit(True)).otherwise(pl.col('上升趋势 1-1')).alias('上升趋势 1-1')
            )
            report.write_excel(Config['Paths']['DataPath'] / 'output' / self.output)

        standup = selected_stock_month.join(
            selected_ma_month.select('date', 'ma_30'), on='date'
        ).filter(
            (pl.col(f'{self.adjust}_low').shift(1) < pl.col('ma_30').shift(1)) & 
            (pl.col(f'{self.adjust}_low') > pl.col('ma_30'))
        ).drop_nulls()['date']
        Printf.info(f'@ 触发 Stock-Low 站上 MA-Month-30 线：\n{[d.strftime("%Y-%m-%d") for d in standup]}') if not standup.is_empty() else None

        if selected_date in standup:
            report = pl.read_excel(Config['Paths']['DataPath'] / 'output' / self.output)
            report = report.with_columns(
                pl.when(pl.col('编号') == self.symbol).then(pl.lit(True)).otherwise(pl.col('上升趋势 1-2')).alias('上升趋势 1-2')
            )
            report.write_excel(Config['Paths']['DataPath'] / 'output' / self.output)

    def condition2(self, window=60):
        Printf.info(f'(2) 周 30 价格突破周 60 价格 (再次确认首突)，同步判断是否站上 30 月线。')
        selected_date = datetime.date(2026, 1, 3) if self.selected_date is None else copy.deepcopy(self.selected_date)
        limited_date = selected_date - datetime.timedelta(days=window + 7)

        date_filter = (pl.col('date') <= selected_date) & (pl.col('date') >= limited_date)
        selected_stock_month = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / 'month.parquet').filter(date_filter)
        selected_ma_week = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / 'ma_week.parquet').filter(date_filter)
        selected_ma_month = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / 'ma_month.parquet').filter(date_filter)

        breakthrough = selected_ma_week.filter(
            (pl.col('ma_30').shift(1) < pl.col('ma_60').shift(1)) & 
            (pl.col('ma_30') > pl.col('ma_60'))
        ).drop_nulls()['date']
        Printf.info(f'@ 触发 MA-Week-30 首次突破 MA-Week-60：\n{[d.strftime("%Y-%m-%d") for d in breakthrough]}') if not breakthrough.is_empty() else None

        if selected_date in breakthrough:
            report = pl.read_excel(Config['Paths']['DataPath'] / 'output' / self.output)
            report = report.with_columns(
                pl.when(pl.col('编号') == self.symbol).then(pl.lit(True)).otherwise(pl.col('上升趋势 2-1')).alias('上升趋势 2-1')
            )
            report.write_excel(Config['Paths']['DataPath'] / 'output' / self.output)

        standup = selected_stock_month.join(
            selected_ma_month.select('date', 'ma_30'), on='date'
        ).filter(
            (pl.col(f'{self.adjust}_low').shift(1) < pl.col('ma_30').shift(1)) & 
            (pl.col(f'{self.adjust}_low') > pl.col('ma_30'))
        ).drop_nulls()['date']
        Printf.info(f'@ 触发 Stock-Low 站上 MA-Month-30 线：\n{[d.strftime("%Y-%m-%d") for d in standup]}') if not standup.is_empty() else None

        if selected_date in standup:
            report = pl.read_excel(Config['Paths']['DataPath'] / 'output' / self.output)
            report = report.with_columns(
                pl.when(pl.col('编号') == self.symbol).then(pl.lit(True)).otherwise(pl.col('上升趋势 2-2')).alias('上升趋势 2-2')
            )
            report.write_excel(Config['Paths']['DataPath'] / 'output' / self.output)

    def condition3(self, window=60):
        Printf.info(f'(3) MACD 突破0轴后回踩均线，缠绕，变成多头趋势 (短线看日线，长线看周、月、季线)。')
        selected_date = datetime.date(2026, 1, 3) if self.selected_date is None else copy.deepcopy(self.selected_date)
        limited_date = selected_date - datetime.timedelta(days=window + 7)

        date_filter = (pl.col('date') <= selected_date) & (pl.col('date') >= limited_date)
        
        period = 'week'
        selected_macd = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / f'macd_{period}.parquet').filter(date_filter)
        selected_ma = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / f'ma_{period}.parquet').filter(date_filter)

        breakthrough = selected_macd.filter(
            (pl.col('dif').shift(1) < 0) & 
            (pl.col('dif') >= 0) & 
            (pl.col('dif') > pl.col('dea'))
        ).drop_nulls()['date']
        Printf.info(f'@ 触发 MACD-{period.title()} 突破 0 轴：\n{[d.strftime("%Y-%m-%d") for d in breakthrough]}') if not breakthrough.is_empty() else None

        if selected_date in breakthrough:
            report = pl.read_excel(Config['Paths']['DataPath'] / 'output' / self.output)
            report = report.with_columns(
                pl.when(pl.col('编号') == self.symbol).then(pl.lit(True)).otherwise(pl.col('上升趋势 3-1')).alias('上升趋势 3-1')
            )
            report.write_excel(Config['Paths']['DataPath'] / 'output' / self.output)

        longposition = selected_ma.filter(
            (pl.col('ma_10') > pl.col('ma_20')) & 
            (pl.col('ma_20') > pl.col('ma_30')) & 
            (pl.col('ma_30') > pl.col('ma_60'))
        )['date']
        Printf.info(f'@ 触发 MA-{period.title()} 多头排列：') if not longposition.is_empty() else None 

        groups, date_indices = [], {date: idx for idx, date in enumerate(selected_ma['date'])}
        for i, d in enumerate(longposition):
            if i == 0 or date_indices[d] != date_indices[longposition[i-1]] + 1:
                groups.append([d])
            else:
                groups[-1].append(d)
        for i, group in enumerate(groups):
            Printf.info(f'时间区间 {i+1}：[{group[0] if len(group) == 1 else f"{group[0]} ~ {group[-1]}"}]')

        if selected_date in longposition:
            report = pl.read_excel(Config['Paths']['DataPath'] / 'output' / self.output)
            report = report.with_columns(
                pl.when(pl.col('编号') == self.symbol).then(pl.lit(True)).otherwise(pl.col('上升趋势 3-2')).alias('上升趋势 3-2')
            )
            report.write_excel(Config['Paths']['DataPath'] / 'output' / self.output)

        period = 'month'
        selected_macd = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / f'macd_{period}.parquet').filter(date_filter)
        selected_ma = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / f'ma_{period}.parquet').filter(date_filter)

        breakthrough = selected_macd.filter(
            (pl.col('dif').shift(1) < 0) & 
            (pl.col('dif') >= 0) & 
            (pl.col('dif') > pl.col('dea'))
        ).drop_nulls()['date']
        Printf.info(f'@ 触发 MACD-{period.title()} 突破 0 轴：\n{[d.strftime("%Y-%m-%d") for d in breakthrough]}') if not breakthrough.is_empty() else None

        if selected_date in breakthrough:
            report = pl.read_excel(Config['Paths']['DataPath'] / 'output' / self.output)
            report = report.with_columns(
                pl.when(pl.col('编号') == self.symbol).then(pl.lit(True)).otherwise(pl.col('上升趋势 4-1')).alias('上升趋势 4-1')
            )
            report.write_excel(Config['Paths']['DataPath'] / 'output' / self.output)

        longposition = selected_ma.filter(
            (pl.col('ma_10') > pl.col('ma_20')) & 
            (pl.col('ma_20') > pl.col('ma_30')) & 
            (pl.col('ma_30') > pl.col('ma_60'))
        )['date']
        Printf.info(f'@ 触发 MA-{period.title()} 多头排列：') if not longposition.is_empty() else None 

        groups, date_indices = [], {date: idx for idx, date in enumerate(selected_ma['date'])}
        for i, d in enumerate(longposition):
            if i == 0 or date_indices[d] != date_indices[longposition[i-1]] + 1:
                groups.append([d])
            else:
                groups[-1].append(d)
        for i, group in enumerate(groups):
            Printf.info(f'时间区间 {i+1}：[{group[0] if len(group) == 1 else f"{group[0]} ~ {group[-1]}"}]')

        if selected_date in longposition:
            report = pl.read_excel(Config['Paths']['DataPath'] / 'output' / self.output)
            report = report.with_columns(
                pl.when(pl.col('编号') == self.symbol).then(pl.lit(True)).otherwise(pl.col('上升趋势 4-2')).alias('上升趋势 4-2')
            )
            report.write_excel(Config['Paths']['DataPath'] / 'output' / self.output)

        period = 'quarter'
        selected_macd = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / f'macd_{period}.parquet').filter(date_filter)
        selected_ma = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / f'ma_{period}.parquet').filter(date_filter)

        breakthrough = selected_macd.filter(
            (pl.col('dif').shift(1) < 0) & 
            (pl.col('dif') >= 0) & 
            (pl.col('dif') > pl.col('dea'))
        ).drop_nulls()['date']
        Printf.info(f'@ 触发 MACD-{period.title()} 突破 0 轴：\n{[d.strftime("%Y-%m-%d") for d in breakthrough]}') if not breakthrough.is_empty() else None

        if selected_date in breakthrough:
            report = pl.read_excel(Config['Paths']['DataPath'] / 'output' / self.output)
            report = report.with_columns(
                pl.when(pl.col('编号') == self.symbol).then(pl.lit(True)).otherwise(pl.col('上升趋势 5-1')).alias('上升趋势 5-1')
            )
            report.write_excel(Config['Paths']['DataPath'] / 'output' / self.output)
        
        longposition = selected_ma.filter(
            (pl.col('ma_10') > pl.col('ma_20')) & 
            (pl.col('ma_20') > pl.col('ma_30')) & 
            (pl.col('ma_30') > pl.col('ma_60'))
        )['date']
        Printf.info(f'@ 触发 MA-{period.title()} 多头排列：') if not longposition.is_empty() else None 

        groups, date_indices = [], {date: idx for idx, date in enumerate(selected_ma['date'])}
        for i, d in enumerate(longposition):
            if i == 0 or date_indices[d] != date_indices[longposition[i-1]] + 1:
                groups.append([d])
            else:
                groups[-1].append(d)
        for i, group in enumerate(groups):
            Printf.info(f'时间区间 {i+1}：[{group[0] if len(group) == 1 else f"{group[0]} ~ {group[-1]}"}]')

        if selected_date in longposition:
            report = pl.read_excel(Config['Paths']['DataPath'] / 'output' / self.output)
            report = report.with_columns(
                pl.when(pl.col('编号') == self.symbol).then(pl.lit(True)).otherwise(pl.col('上升趋势 5-2')).alias('上升趋势 5-2')
            )
            report.write_excel(Config['Paths']['DataPath'] / 'output' / self.output)


class DescendTrend:

    def __init__(self, symbol, product='stock', selected_date=None, adjust='raw', output='report-test.xlsx'):
        self.symbol = symbol
        self.selected_date = selected_date or date.today()
        self.adjust = adjust
        self.output = output
        self.product = product

        Printf.info(f'\n3、下跌趋势')
        self.condition1()
        self.condition2()
        self.condition3()
        self.condition4()

    def condition1(self, window=60):
        Printf.info(f'(1) 日 60 跌破日 250。')
        selected_date = datetime.date(2026, 1, 3) if self.selected_date is None else copy.deepcopy(self.selected_date)
        limited_date = selected_date - datetime.timedelta(days=window + 7)

        date_filter = (pl.col('date') <= selected_date) & (pl.col('date') >= limited_date)
        selected_ma_day = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / 'ma_day.parquet').filter(date_filter)

        fallbelow = selected_ma_day.filter(
            (pl.col('ma_60').shift(1) > pl.col('ma_250').shift(1)) & 
            (pl.col('ma_60') < pl.col('ma_250'))
        ).drop_nulls()['date']
        Printf.info(f'@ 触发 MA-Day-60 跌破 MA-Day-250：\n{[d.strftime("%Y-%m-%d") for d in fallbelow]}') if not fallbelow.is_empty() else None

        if selected_date in fallbelow:
            report = pl.read_excel(Config['Paths']['DataPath'] / 'output' / self.output)
            report = report.with_columns(
                pl.when(pl.col('编号') == self.symbol).then(pl.lit(True)).otherwise(pl.col('下降趋势 1-1')).alias('下降趋势 1-1')
            )
            report.write_excel(Config['Paths']['DataPath'] / 'output' / self.output)
        
    def condition2(self, window=60):
        Printf.info(f'(2) 周 20 跌破周 60。')
        selected_date = datetime.date(2026, 1, 3) if self.selected_date is None else copy.deepcopy(self.selected_date)
        limited_date = selected_date - datetime.timedelta(days=window + 7)

        date_filter = (pl.col('date') <= selected_date) & (pl.col('date') >= limited_date)
        selected_ma_week = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / 'ma_week.parquet').filter(date_filter)

        fallbelow = selected_ma_week.filter(
            (pl.col('ma_20').shift(1) > pl.col('ma_60').shift(1)) & 
            (pl.col('ma_20') < pl.col('ma_60'))
        ).drop_nulls()['date']
        Printf.info(f'@ 触发 MA-Week-20 跌破 MA-Week-60：\n{[d.strftime("%Y-%m-%d") for d in fallbelow]}') if not fallbelow.is_empty() else None
        
        if selected_date in fallbelow:
            report = pl.read_excel(Config['Paths']['DataPath'] / 'output' / self.output)
            report = report.with_columns(
                pl.when(pl.col('编号') == self.symbol).then(pl.lit(True)).otherwise(pl.col('下降趋势 2-1')).alias('下降趋势 2-1')
            )
            report.write_excel(Config['Paths']['DataPath'] / 'output' / self.output)
        
    def condition3(self, window=60):
        Printf.info(f'(3) 周 30 跌破周 60。')
        selected_date = datetime.date(2026, 1, 3) if self.selected_date is None else copy.deepcopy(self.selected_date)
        limited_date = selected_date - datetime.timedelta(days=window + 7)

        date_filter = (pl.col('date') <= selected_date) & (pl.col('date') >= limited_date)
        selected_ma_week = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / 'ma_week.parquet').filter(date_filter)

        fallbelow = selected_ma_week.filter(
            (pl.col('ma_30').shift(1) > pl.col('ma_60').shift(1)) & 
            (pl.col('ma_30') < pl.col('ma_60'))
        ).drop_nulls()['date']
        Printf.info(f'@ 触发 MA-Week-30 跌破 MA-Week-60：\n{[d.strftime("%Y-%m-%d") for d in fallbelow]}') if not fallbelow.is_empty() else None
        
        if selected_date in fallbelow:
            report = pl.read_excel(Config['Paths']['DataPath'] / 'output' / self.output)
            report = report.with_columns(
                pl.when(pl.col('编号') == self.symbol).then(pl.lit(True)).otherwise(pl.col('下降趋势 3-1')).alias('下降趋势 3-1')
            )
            report.write_excel(Config['Paths']['DataPath'] / 'output' / self.output)
        
    def condition4(self, window=60):
        Printf.info(f'(4) 跌破月 30。')
        selected_date = datetime.date(2026, 1, 3) if self.selected_date is None else copy.deepcopy(self.selected_date)
        limited_date = selected_date - datetime.timedelta(days=window + 7)

        date_filter = (pl.col('date') <= selected_date) & (pl.col('date') >= limited_date)
        selected_stock_month = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / 'month.parquet').filter(date_filter)
        selected_ma_month = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / 'ma_month.parquet').filter(date_filter)

        fallbelow = selected_stock_month.join(
            selected_ma_month.select('date', 'ma_30'), on='date'
        ).filter(
            (pl.col(f'{self.adjust}_low').shift(1) > pl.col('ma_30').shift(1)) & 
            (pl.col(f'{self.adjust}_low') < pl.col('ma_30'))
        ).drop_nulls()['date']
        Printf.info(f'@ 触发 Stock-Low 下穿跌破 MA-Month-30 线：\n{[d.strftime("%Y-%m-%d") for d in fallbelow]}') if not fallbelow.is_empty() else None
        
        if selected_date in fallbelow:
            report = pl.read_excel(Config['Paths']['DataPath'] / 'output' / self.output)
            report = report.with_columns(
                pl.when(pl.col('编号') == self.symbol).then(pl.lit(True)).otherwise(pl.col('下降趋势 4-1')).alias('下降趋势 4-1')
            )
            report.write_excel(Config['Paths']['DataPath'] / 'output' / self.output)


class SmallFluctuations:

    def __init__(self, symbol, product='stock', selected_date=None, adjust='raw', output='report-test.xlsx'):
        self.symbol = symbol
        self.selected_date = selected_date or date.today()
        self.adjust = adjust
        self.output = output
        self.product = product

        Printf.info(f'\n4、小调整')
        self.condition1()
        self.condition2()

    def condition1(self, window=60):
        Printf.info(f'(1) 月小坑，参考月 30 价格。')
        selected_date = datetime.date(2026, 1, 3) if self.selected_date is None else copy.deepcopy(self.selected_date)
        limited_date = selected_date - datetime.timedelta(days=window + 7)

        date_filter = (pl.col('date') <= selected_date) & (pl.col('date') >= limited_date)
        selected_macd_month = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / f'macd_month.parquet').filter(date_filter)
        selected_ma_month = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / 'ma_month.parquet').filter(date_filter)
        selected_stock_month = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / 'month.parquet').filter(date_filter)

        smallmonthhole = selected_macd_month.filter(
            (pl.col('macd') < 0) & 
            (pl.col('dif') > 0) & 
            (pl.col('dif') < pl.col('dea'))
        ).drop_nulls()['date']
        Printf.info(f'@ 触发 MACD-Month 月小坑：') if not smallmonthhole.is_empty() else None

        if not smallmonthhole.is_empty():         
            groups, date_indices = [], {date: idx for idx, date in enumerate(selected_macd_month['date'])}
            for i, d in enumerate(smallmonthhole):
                if i == 0 or date_indices[d] != date_indices[smallmonthhole[i-1]] + 1:
                    groups.append([d])
                else:
                    groups[-1].append(d)
            for i, group in enumerate(groups):
                Printf.info(f'时间区间 {i+1}: [{group[0] if len(group) == 1 else f"{group[0]} ~ {group[-1]}"}]')

        if selected_date in smallmonthhole:
            report = pl.read_excel(source=Config['Paths']['DataPath'] / 'output' / self.output)
            report = report.with_columns(
                pl.when(pl.col('编号') == self.symbol).then(pl.lit(True)).otherwise(pl.col('小调整 1-1')).alias('小调整 1-1')
            )
            report.write_excel(Config['Paths']['DataPath'] / 'output' / self.output)

        fallbelow = selected_stock_month.join(
            selected_ma_month.select('date', 'ma_30'), on='date'
        ).filter(
            (pl.col(f'{self.adjust}_low') >= pl.col('ma_30'))
        ).drop_nulls()['date']
        intersection = smallmonthhole.filter(smallmonthhole.is_in(fallbelow.implode()))
        Printf.info(f'@ 始终站上 MA-Month-30 线：') if not intersection.is_empty() else None

        if not intersection.is_empty():
            groups, date_indices = [], {date: idx for idx, date in enumerate(selected_macd_month['date'])}
            for i, d in enumerate(intersection):
                if i == 0 or date_indices[d] != date_indices[intersection[i-1]] + 1:    
                    groups.append([d])
                else:
                    groups[-1].append(d)
            for i, group in enumerate(groups):
                Printf.info(f'时间区间 {i+1}: [{group[0] if len(group) == 1 else f"{group[0]} ~ {group[-1]}"}]')

        if selected_date in intersection:
            report = pl.read_excel(Config['Paths']['DataPath'] / 'output' / self.output)
            report = report.with_columns(
                pl.when(pl.col('编号') == self.symbol).then(pl.lit(True)).otherwise(pl.col('小调整 1-2')).alias('小调整 1-2')
            )
            report.write_excel(Config['Paths']['DataPath'] / 'output' / self.output)

    def condition2(self, window=60):
        Printf.info(f'(2) 坑内出来以后，Boll 周下是买点。')
        selected_date = datetime.date(2026, 1, 3) if self.selected_date is None else copy.deepcopy(self.selected_date)
        limited_date = selected_date - datetime.timedelta(days=window + 7)

        date_filter = (pl.col('date') <= selected_date) & (pl.col('date') >= limited_date)
        selected_macd_month = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / f'macd_month.parquet').filter(date_filter)
        # selected_boll_week = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / f'boll_week.parquet').filter(date_filter)

        climbupmonthhole = selected_macd_month.filter(
            (pl.col('dif') > 0) & 
            (pl.col('dif').shift(1) < 0) & 
            (pl.col('dea') < 0) & 
            (pl.col('dea').shift(1) < 0) & 
            (pl.col('dif') > pl.col('dea'))
        ).drop_nulls()['date']
        Printf.info(f'@ 触发 MACD-Month 坑内出：{[d.strftime("%Y-%m-%d") for d in climbupmonthhole]}') if not climbupmonthhole.is_empty() else None

        if selected_date in climbupmonthhole:
            report = pl.read_excel(Config['Paths']['DataPath'] / 'output' / self.output)
            report = report.with_columns(
                pl.when(pl.col('编号') == self.symbol).then(pl.lit(True)).otherwise(pl.col('小调整 2-1')).alias('小调整 2-1')
            )
            report.write_excel(Config['Paths']['DataPath'] / 'output' / self.output)
        
        # latest_stock_low < latest_boll_week


if __name__ == '__main__':
    pass
