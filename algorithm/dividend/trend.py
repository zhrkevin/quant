#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import polars as pl
from datetime import date, timedelta

from project.configuration import Config
from algorithm.middleware.logger import Logger


class AscendTrend:

    def __init__(self, symbol, product='stock', selected_date=None, adjust='raw', output='report-test.xlsx'):
        self.symbol = symbol
        self.adjust = adjust
        self.product = product
        self.selected_date = selected_date or date.today()
        self.filter = (pl.col('date') <= self.selected_date) & (pl.col('date') >= self.selected_date - timedelta(days=60 + 7))

        Logger.info(f'\n1、上升趋势')
        self.report = pl.read_excel(Config['Paths']['DataPath'] / 'output' / output)
        self.condition1()
        self.condition2()
        self.condition3()
        self.report.write_excel(Config['Paths']['DataPath'] / 'output' / output)

    def condition1(self):
        Logger.info(f'(1) 周 20 价格突破周 60 价格 (首突)，同步判断是否站上 30 月线。')

        selected_stock_month = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / 'month.parquet').filter(self.filter)
        selected_ma_week = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / f'ma_week.parquet').filter(self.filter)
        selected_ma_month = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / 'ma_month.parquet').filter(self.filter)

        breakthrough = selected_ma_week.filter(
            (pl.col('ma_20').shift(1) < pl.col('ma_60').shift(1)) & 
            (pl.col('ma_20') > pl.col('ma_60'))
        ).drop_nulls()['date']
        Logger.info(f'@ 触发 MA-Week-20 首次突破 MA-Week-60：\n{[d.strftime("%Y-%m-%d") for d in breakthrough]}') if not breakthrough.is_empty() else None

        if self.selected_date in breakthrough:
            self.report = self.report.with_columns(
                pl.when(pl.col('编号') == self.symbol).then(pl.lit(True)).otherwise(pl.col('上升趋势 1-1')).alias('上升趋势 1-1')
            )

        standup = selected_stock_month.join(
            selected_ma_month.select('date', 'ma_30'), on='date'
        ).filter(
            (pl.col(f'{self.adjust}_low').shift(1) < pl.col('ma_30').shift(1)) & 
            (pl.col(f'{self.adjust}_low') > pl.col('ma_30'))
        ).drop_nulls()['date']
        Logger.info(f'@ 触发 Stock-Low 站上 MA-Month-30 线：\n{[d.strftime("%Y-%m-%d") for d in standup]}') if not standup.is_empty() else None

        if self.selected_date in standup:
            self.report = self.report.with_columns(
                pl.when(pl.col('编号') == self.symbol).then(pl.lit(True)).otherwise(pl.col('上升趋势 1-2')).alias('上升趋势 1-2')
            )

    def condition2(self):
        Logger.info(f'(2) 周 30 价格突破周 60 价格 (再次确认首突)，同步判断是否站上 30 月线。')

        selected_stock_month = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / 'month.parquet').filter(self.filter)
        selected_ma_week = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / 'ma_week.parquet').filter(self.filter)
        selected_ma_month = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / 'ma_month.parquet').filter(self.filter)

        breakthrough = selected_ma_week.filter(
            (pl.col('ma_30').shift(1) < pl.col('ma_60').shift(1)) & 
            (pl.col('ma_30') > pl.col('ma_60'))
        ).drop_nulls()['date']
        Logger.info(f'@ 触发 MA-Week-30 首次突破 MA-Week-60：\n{[d.strftime("%Y-%m-%d") for d in breakthrough]}') if not breakthrough.is_empty() else None

        if self.selected_date in breakthrough:
            self.report = self.report.with_columns(
                pl.when(pl.col('编号') == self.symbol).then(pl.lit(True)).otherwise(pl.col('上升趋势 2-1')).alias('上升趋势 2-1')
            )

        standup = selected_stock_month.join(
            selected_ma_month.select('date', 'ma_30'), on='date'
        ).filter(
            (pl.col(f'{self.adjust}_low').shift(1) < pl.col('ma_30').shift(1)) & 
            (pl.col(f'{self.adjust}_low') > pl.col('ma_30'))
        ).drop_nulls()['date']
        Logger.info(f'@ 触发 Stock-Low 站上 MA-Month-30 线：\n{[d.strftime("%Y-%m-%d") for d in standup]}') if not standup.is_empty() else None

        if self.selected_date in standup:
            self.report = self.report.with_columns( 
                pl.when(pl.col('编号') == self.symbol).then(pl.lit(True)).otherwise(pl.col('上升趋势 2-2')).alias('上升趋势 2-2')
            )

    def condition3(self):
        Logger.info(f'(3) MACD 突破0轴后回踩均线，缠绕，变成多头趋势 (短线看日线，长线看周、月、季线)。')

        for period in ['week', 'month', 'quarter']:
            selected_macd = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / f'macd_{period}.parquet').filter(self.filter)
            selected_ma = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / f'ma_{period}.parquet').filter(self.filter)

            breakthrough = selected_macd.filter(
                (pl.col('dif').shift(1) < 0) & 
                (pl.col('dif') >= 0) & 
                (pl.col('dif') > pl.col('dea'))
            ).drop_nulls()['date']
            Logger.info(f'@ 触发 MACD-{period.title()} 突破 0 轴：\n{[d.strftime("%Y-%m-%d") for d in breakthrough]}') if not breakthrough.is_empty() else None

            if self.selected_date in breakthrough:
                if period == 'week':
                    column = '上升趋势 3-1'
                elif period == 'month':
                    column = '上升趋势 4-1'
                elif period == 'quarter':
                    column = '上升趋势 5-1'
                else:
                    Logger.error(f'未知的时间周期 {period}。')  
                self.report = self.report.with_columns(
                    pl.when(pl.col('编号') == self.symbol).then(pl.lit(True)).otherwise(pl.col(column)).alias(column)
                )

            longposition = selected_ma.filter(
                (pl.col('ma_10') > pl.col('ma_20')) & 
                (pl.col('ma_20') > pl.col('ma_30')) & 
                (pl.col('ma_30') > pl.col('ma_60'))
            )['date']
            Logger.info(f'@ 触发 MA-{period.title()} 多头排列：') if not longposition.is_empty() else None

            if self.selected_date in longposition:
                if period == 'week':
                    column = '上升趋势 3-2'
                elif period == 'month':
                    column = '上升趋势 4-2'
                elif period == 'quarter':
                    column = '上升趋势 5-2'
                else:
                    Logger.error(f'未知的时间周期 {period}。')  
                self.report = self.report.with_columns(
                    pl.when(pl.col('编号') == self.symbol).then(pl.lit(True)).otherwise(pl.col(column)).alias(column)
                )

            groups, date_indices = [], {date: idx for idx, date in enumerate(selected_ma['date'])}
            for i, d in enumerate(longposition):
                if i == 0 or date_indices[d] != date_indices[longposition[i-1]] + 1:
                    groups.append([d])
                else:
                    groups[-1].append(d)
            for i, group in enumerate(groups):
                Logger.info(f'时间区间 {i+1}：[{group[0] if len(group) == 1 else f"{group[0]} ~ {group[-1]}"}]')


class DescendTrend:

    def __init__(self, symbol, product='stock', selected_date=None, adjust='raw', output='report-test.xlsx'):
        self.symbol = symbol
        self.adjust = adjust
        self.product = product
        self.selected_date = selected_date or date.today()
        self.filter = (pl.col('date') <= self.selected_date) & (pl.col('date') >= self.selected_date - timedelta(days=60 + 7))

        Logger.info(f'\n3、下跌趋势')
        self.report = pl.read_excel(Config['Paths']['DataPath'] / 'output' / output)
        self.condition1()
        self.condition2()
        self.condition3()
        self.condition4()
        self.report.write_excel(Config['Paths']['DataPath'] / 'output' / output)

    def condition1(self):
        Logger.info(f'(1) 日 60 跌破日 250。')

        selected_ma_day = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / 'ma_day.parquet').filter(self.filter)

        fallbelow = selected_ma_day.filter(
            (pl.col('ma_60').shift(1) > pl.col('ma_250').shift(1)) & 
            (pl.col('ma_60') < pl.col('ma_250'))
        ).drop_nulls()['date']
        Logger.info(f'@ 触发 MA-Day-60 跌破 MA-Day-250：\n{[d.strftime("%Y-%m-%d") for d in fallbelow]}') if not fallbelow.is_empty() else None

        if self.selected_date in fallbelow:
            self.report = self.report.with_columns(
                pl.when(pl.col('编号') == self.symbol).then(pl.lit(True)).otherwise(pl.col('下降趋势 1-1')).alias('下降趋势 1-1')
            )
        
    def condition2(self):
        Logger.info(f'(2) 周 20 跌破周 60。')

        selected_ma_week = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / 'ma_week.parquet').filter(self.filter)

        fallbelow = selected_ma_week.filter(
            (pl.col('ma_20').shift(1) > pl.col('ma_60').shift(1)) & 
            (pl.col('ma_20') < pl.col('ma_60'))
        ).drop_nulls()['date']
        Logger.info(f'@ 触发 MA-Week-20 跌破 MA-Week-60：\n{[d.strftime("%Y-%m-%d") for d in fallbelow]}') if not fallbelow.is_empty() else None
        
        if self.selected_date in fallbelow:
            self.report = self.report.with_columns(
                pl.when(pl.col('编号') == self.symbol).then(pl.lit(True)).otherwise(pl.col('下降趋势 2-1')).alias('下降趋势 2-1')
            )
        
    def condition3(self):
        Logger.info(f'(3) 周 30 跌破周 60。')

        selected_ma_week = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / 'ma_week.parquet').filter(self.filter)   

        fallbelow = selected_ma_week.filter(
            (pl.col('ma_30').shift(1) > pl.col('ma_60').shift(1)) & 
            (pl.col('ma_30') < pl.col('ma_60'))
        ).drop_nulls()['date']
        Logger.info(f'@ 触发 MA-Week-30 跌破 MA-Week-60：\n{[d.strftime("%Y-%m-%d") for d in fallbelow]}') if not fallbelow.is_empty() else None
        
        if self.selected_date in fallbelow:
            self.report = self.report.with_columns(
                pl.when(pl.col('编号') == self.symbol).then(pl.lit(True)).otherwise(pl.col('下降趋势 3-1')).alias('下降趋势 3-1')
            )
        
    def condition4(self):
        Logger.info(f'(4) 跌破月 30。')

        selected_stock_month = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / 'month.parquet').filter(self.filter)
        selected_ma_month = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / 'ma_month.parquet').filter(self.filter)

        fallbelow = selected_stock_month.join(
            selected_ma_month.select('date', 'ma_30'), on='date'
        ).filter(
            (pl.col(f'{self.adjust}_low').shift(1) > pl.col('ma_30').shift(1)) & 
            (pl.col(f'{self.adjust}_low') < pl.col('ma_30'))
        ).drop_nulls()['date']
        Logger.info(f'@ 触发 Stock-Low 下穿跌破 MA-Month-30 线：\n{[d.strftime("%Y-%m-%d") for d in fallbelow]}') if not fallbelow.is_empty() else None
        
        if self.selected_date in fallbelow:
            self.report = self.report.with_columns(
                pl.when(pl.col('编号') == self.symbol).then(pl.lit(True)).otherwise(pl.col('下降趋势 4-1')).alias('下降趋势 4-1')
            )


class SmallFluctuations:

    def __init__(self, symbol, selected_date=None, product='stock', adjust='raw', output='report-test.xlsx'):
        self.symbol = symbol
        self.selected_date = selected_date or date.today()
        self.adjust = adjust
        self.product = product
        self.filter = (pl.col('date') <= self.selected_date) & (pl.col('date') >= self.selected_date - timedelta(days=60 + 7))

        Logger.info(f'\n4、小调整')
        self.report = pl.read_excel(Config['Paths']['DataPath'] / 'output' / output)
        self.condition1()
        self.condition2()
        self.report.write_excel(Config['Paths']['DataPath'] / 'output' / output)

    def condition1(self):
        Logger.info(f'(1) 月小坑，参考月 30 价格。')

        selected_macd_month = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / f'macd_month.parquet').filter(self.filter)
        selected_ma_month = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / 'ma_month.parquet').filter(self.filter)
        selected_stock_month = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / 'month.parquet').filter(self.filter)

        smallmonthhole = selected_macd_month.filter(
            (pl.col('macd') < 0) & 
            (pl.col('dif') > 0) & 
            (pl.col('dif') < pl.col('dea'))
        ).drop_nulls()['date']
        Logger.info(f'@ 触发 MACD-Month 月小坑：') if not smallmonthhole.is_empty() else None

        if not smallmonthhole.is_empty():         
            groups, date_indices = [], {date: idx for idx, date in enumerate(selected_macd_month['date'])}
            for i, d in enumerate(smallmonthhole):
                if i == 0 or date_indices[d] != date_indices[smallmonthhole[i-1]] + 1:
                    groups.append([d])
                else:
                    groups[-1].append(d)
            for i, group in enumerate(groups):
                Logger.info(f'时间区间 {i+1}: [{group[0] if len(group) == 1 else f"{group[0]} ~ {group[-1]}"}]')

        if self.selected_date in smallmonthhole:
            self.report = self.report.with_columns(
                pl.when(pl.col('编号') == self.symbol).then(pl.lit(True)).otherwise(pl.col('小调整 1-1')).alias('小调整 1-1')
            )

        fallbelow = selected_stock_month.join(
            selected_ma_month.select('date', 'ma_30'), on='date'
        ).filter(
            (pl.col(f'{self.adjust}_low') >= pl.col('ma_30'))
        ).drop_nulls()['date']
        intersection = smallmonthhole.filter(smallmonthhole.is_in(fallbelow.implode()))
        Logger.info(f'@ 始终站上 MA-Month-30 线：') if not intersection.is_empty() else None

        if not intersection.is_empty():
            groups, date_indices = [], {date: idx for idx, date in enumerate(selected_macd_month['date'])}
            for i, d in enumerate(intersection):
                if i == 0 or date_indices[d] != date_indices[intersection[i-1]] + 1:    
                    groups.append([d])
                else:
                    groups[-1].append(d)
            for i, group in enumerate(groups):
                Logger.info(f'时间区间 {i+1}: [{group[0] if len(group) == 1 else f"{group[0]} ~ {group[-1]}"}]')

        if self.selected_date in intersection:
            self.report = self.report.with_columns(
                pl.when(pl.col('编号') == self.symbol).then(pl.lit(True)).otherwise(pl.col('小调整 1-2')).alias('小调整 1-2')
            )

    def condition2(self):
        Logger.info(f'(2) 坑内出来以后，Boll 周下是买点。')

        selected_macd_month = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / f'macd_month.parquet').filter(self.filter)
        # selected_boll_week = pl.read_parquet(Config['Paths']['DataPath'] / self.product / self.symbol / f'boll_week.parquet').filter(self.filter) 

        climbupmonthhole = selected_macd_month.filter(
            (pl.col('dif') > 0) & 
            (pl.col('dif').shift(1) < 0) & 
            (pl.col('dea') < 0) & 
            (pl.col('dea').shift(1) < 0) & 
            (pl.col('dif') > pl.col('dea'))
        ).drop_nulls()['date']
        Logger.info(f'@ 触发 MACD-Month 坑内出：{[d.strftime("%Y-%m-%d") for d in climbupmonthhole]}') if not climbupmonthhole.is_empty() else None

        if self.selected_date in climbupmonthhole:
            self.report = self.report.with_columns(
                pl.when(pl.col('编号') == self.symbol).then(pl.lit(True)).otherwise(pl.col('小调整 2-1')).alias('小调整 2-1')
            )
        
        # latest_stock_low < latest_boll_week


if __name__ == '__main__':
    pass
