#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import polars as pl
from datetime import date, timedelta

from project import Config
from algorithm.middleware.logger import Logger


class AscendTrend:

    @classmethod
    def run(cls, product, symbol, today, report):
        cls.product = product
        cls.symbol = symbol
        cls.today = today
        cls.report = report
        cls.filter = (pl.col('日期') <= cls.today) & (pl.col('日期') >= cls.today - timedelta(days=60 + 7))

        Logger.info(f'\n1、上升趋势')

        cls.condition1()
        cls.condition2()
        cls.condition3()
        return cls.report

    @classmethod
    def condition1(cls):
        Logger.info(f'(1) 周 20 价格突破周 60 价格 (首突)，同步判断是否站上 30 月线。')

        selected_stock_month = pl.read_parquet(Config['Paths']['DataPath'] / cls.product / cls.symbol / 'month.parquet').filter(cls.filter)
        selected_ma_week = pl.read_parquet(Config['Paths']['DataPath'] / cls.product / cls.symbol / f'ma_week.parquet').filter(cls.filter)
        selected_ma_month = pl.read_parquet(Config['Paths']['DataPath'] / cls.product / cls.symbol / 'ma_month.parquet').filter(cls.filter)

        breakthrough = selected_ma_week.filter(
            (pl.col('MA20').shift(1) < pl.col('MA60').shift(1)) & (pl.col('MA20') > pl.col('MA60'))
        ).drop_nulls()['日期']

        if cls.today in breakthrough:
            cls.report = cls.report.with_columns(
                pl.when(pl.col('编号') == '编号')
                  .then(pl.lit('上升趋势 1-1'))
                  .when(pl.col('编号') == cls.symbol)
                  .then(pl.lit(True))
                  .otherwise(pl.lit(''))
                  .alias('上升趋势 1-1')
            )
            Logger.info(f'@ 触发 MA-Week-20 首次突破 MA-Week-60：\n{[d.strftime("%Y-%m-%d") for d in breakthrough]}')

        standup = selected_stock_month.join(selected_ma_month.select('日期', 'MA30'), on='日期').filter(
            (pl.col('最低').shift(1) < pl.col('MA30').shift(1)) & (pl.col('最低') > pl.col('MA30'))
        ).drop_nulls()['日期']

        if cls.today in standup:
            cls.report = cls.report.with_columns(
                pl.when(pl.col('编号') == '编号')
                  .then(pl.lit('上升趋势 1-2'))
                  .when(pl.col('编号') == cls.symbol)
                  .then(pl.lit(True))
                  .otherwise(pl.lit(''))
                  .alias('上升趋势 1-2')
            )
            Logger.info(f'@ 触发 Stock-Low 站上 MA-Month-30 线：\n{[d.strftime("%Y-%m-%d") for d in standup]}')

    @classmethod
    def condition2(cls):
        Logger.info(f'(2) 周 30 价格突破周 60 价格 (再次确认首突)，同步判断是否站上 30 月线。')

        selected_stock_month = pl.read_parquet(Config['Paths']['DataPath'] / cls.product / cls.symbol / 'month.parquet').filter(cls.filter)
        selected_ma_week = pl.read_parquet(Config['Paths']['DataPath'] / cls.product / cls.symbol / 'ma_week.parquet').filter(cls.filter)
        selected_ma_month = pl.read_parquet(Config['Paths']['DataPath'] / cls.product / cls.symbol / 'ma_month.parquet').filter(cls.filter)

        breakthrough = selected_ma_week.filter(
            (pl.col('MA30').shift(1) < pl.col('MA60').shift(1)) & (pl.col('MA30') > pl.col('MA60'))
        ).drop_nulls()['日期']

        if cls.today in breakthrough:
            cls.report = cls.report.with_columns(
                pl.when(pl.col('编号') == '编号')
                  .then(pl.lit('上升趋势 2-1'))
                  .when(pl.col('编号') == cls.symbol)
                  .then(pl.lit(True))
                  .otherwise(pl.lit(''))
                  .alias('上升趋势 2-1')
            )
            Logger.info(f'@ 触发 MA-Week-30 首次突破 MA-Week-60：\n{[d.strftime("%Y-%m-%d") for d in breakthrough]}')

        standup = selected_stock_month.join(selected_ma_month.select('日期', 'MA30'), on='日期').filter(
            (pl.col('最低').shift(1) < pl.col('MA30').shift(1)) & (pl.col('最低') > pl.col('MA30'))
        ).drop_nulls()['日期']

        if cls.today in standup:
            cls.report = cls.report.with_columns( 
                pl.when(pl.col('编号') == '编号')
                  .then(pl.lit('上升趋势 2-2'))
                  .when(pl.col('编号') == cls.symbol)
                  .then(pl.lit(True))
                  .otherwise(pl.lit(''))
                  .alias('上升趋势 2-2')
            )
            Logger.info(f'@ 触发 Stock-Low 站上 MA-Month-30 线：\n{[d.strftime("%Y-%m-%d") for d in standup]}')

    @classmethod
    def condition3(cls):
        Logger.info(f'(3) MACD 突破0轴后回踩均线，缠绕，变成多头趋势 (短线看日线，长线看周、月、季线)。')

        for period in ['week', 'month', 'quarter']:
            selected_macd = pl.read_parquet(Config['Paths']['DataPath'] / cls.product / cls.symbol / f'macd_{period}.parquet').filter(cls.filter)
            selected_ma = pl.read_parquet(Config['Paths']['DataPath'] / cls.product / cls.symbol / f'ma_{period}.parquet').filter(cls.filter)

            breakthrough = selected_macd.filter(
                (pl.col('DIF').shift(1) < 0) & (pl.col('DIF') >= 0) & (pl.col('DIF') > pl.col('DEA'))
            ).drop_nulls()['日期']

            if cls.today in breakthrough:
                column = {'week': '上升趋势 3-1', 'month': '上升趋势 4-1', 'quarter': '上升趋势 5-1'}[period]
                cls.report = cls.report.with_columns(
                    pl.when(pl.col('编号') == '编号')
                      .then(pl.lit(column))
                      .when(pl.col('编号') == cls.symbol)
                      .then(pl.lit(True))
                      .otherwise(pl.lit(''))
                      .alias(column)
                )
                Logger.info(f'@ 触发 MACD-{period.title()} 突破 0 轴：\n{[d.strftime("%Y-%m-%d") for d in breakthrough]}') if not breakthrough.is_empty() else None

            longposition = selected_ma.filter(
                (pl.col('MA10') > pl.col('MA20')) & (pl.col('MA20') > pl.col('MA30')) & (pl.col('MA30') > pl.col('MA60'))
            )['日期']

            if cls.today in longposition:
                column = {'week': '上升趋势 3-2', 'month': '上升趋势 4-2', 'quarter': '上升趋势 5-2'}[period]
                cls.report = cls.report.with_columns(
                    pl.when(pl.col('编号') == '编号')
                      .then(pl.lit(column))
                      .when(pl.col('编号') == cls.symbol)
                      .then(pl.lit(True))
                      .otherwise(pl.lit(''))
                      .alias(column)
                )
                Logger.info(f'@ 触发 MA-{period.title()} 多头排列：')
                groups, date_indices = [], {date: idx for idx, date in enumerate(selected_ma['日期'])}
                for i, d in enumerate(longposition):
                    if i == 0 or date_indices[d] != date_indices[longposition[i-1]] + 1:
                        groups.append([d])
                    else:
                        groups[-1].append(d)
                for i, group in enumerate(groups):
                    Logger.info(f'时间区间 {i+1}：[{group[0] if len(group) == 1 else f"{group[0]} ~ {group[-1]}"}]')


class DescendTrend:

    @classmethod
    def run(cls, product, symbol, today, report):
        cls.product = product
        cls.symbol = symbol
        cls.today = today
        cls.report = report
        cls.filter = (pl.col('日期') <= cls.today) & (pl.col('日期') >= cls.today - timedelta(days=60 + 7))

        Logger.info(f'\n3、下跌趋势')
        cls.condition1()
        cls.condition2()
        cls.condition3()
        cls.condition4()
        return cls.report

    @classmethod
    def condition1(cls):
        Logger.info(f'(1) 日 60 跌破日 250。')

        selected_ma_day = pl.read_parquet(Config['Paths']['DataPath'] / cls.product / cls.symbol / 'ma_day.parquet').filter(cls.filter)

        fallbelow = selected_ma_day.filter(
            (pl.col('MA60').shift(1) > pl.col('MA250').shift(1)) & (pl.col('MA60') < pl.col('MA250'))
        ).drop_nulls()['日期']

        if cls.today in fallbelow:
            cls.report = cls.report.with_columns(
                pl.when(pl.col('编号') == '编号')
                  .then(pl.lit('下降趋势 1-1'))
                  .when(pl.col('编号') == cls.symbol)
                  .then(pl.lit(True))
                  .otherwise(pl.lit(''))
                  .alias('下降趋势 1-1')
            )
            Logger.info(f'@ 触发 MA-Day-60 跌破 MA-Day-250：\n{[d.strftime("%Y-%m-%d") for d in fallbelow]}')

    @classmethod
    def condition2(cls):
        Logger.info(f'(2) 周 20 跌破周 60。')

        selected_ma_week = pl.read_parquet(Config['Paths']['DataPath'] / cls.product / cls.symbol / 'ma_week.parquet').filter(cls.filter)

        fallbelow = selected_ma_week.filter(
            (pl.col('MA20').shift(1) > pl.col('MA60').shift(1)) & (pl.col('MA20') < pl.col('MA60'))
        ).drop_nulls()['日期']

        if cls.today in fallbelow:
            cls.report = cls.report.with_columns(
                pl.when(pl.col('编号') == '编号')
                  .then(pl.lit('下降趋势 2-1'))
                  .when(pl.col('编号') == cls.symbol)
                  .then(pl.lit(True))
                  .otherwise(pl.lit(''))
                  .alias('下降趋势 2-1')
            )
            Logger.info(f'@ 触发 MA-Week-20 跌破 MA-Week-60：\n{[d.strftime("%Y-%m-%d") for d in fallbelow]}')

    @classmethod
    def condition3(cls):
        Logger.info(f'(3) 周 30 跌破周 60。')

        selected_ma_week = pl.read_parquet(Config['Paths']['DataPath'] / cls.product / cls.symbol / 'ma_week.parquet').filter(cls.filter)   

        fallbelow = selected_ma_week.filter(
            (pl.col('MA30').shift(1) > pl.col('MA60').shift(1)) & (pl.col('MA30') < pl.col('MA60'))
        ).drop_nulls()['日期']

        if cls.today in fallbelow:
            cls.report = cls.report.with_columns(
                pl.when(pl.col('编号') == '编号')
                  .then(pl.lit('下降趋势 3-1'))
                  .when(pl.col('编号') == cls.symbol)
                  .then(pl.lit(True))
                  .otherwise(pl.lit(''))
                  .alias('下降趋势 3-1')
            )
            Logger.info(f'@ 触发 MA-Week-30 跌破 MA-Week-60：\n{[d.strftime("%Y-%m-%d") for d in fallbelow]}')

    @classmethod
    def condition4(cls):
        Logger.info(f'(4) 跌破月 30。')

        selected_stock_month = pl.read_parquet(Config['Paths']['DataPath'] / cls.product / cls.symbol / 'month.parquet').filter(cls.filter)
        selected_ma_month = pl.read_parquet(Config['Paths']['DataPath'] / cls.product / cls.symbol / 'ma_month.parquet').filter(cls.filter)

        fallbelow = selected_stock_month.join(selected_ma_month.select('日期', 'MA30'), on='日期').filter(
            (pl.col('最低').shift(1) > pl.col('MA30').shift(1)) & (pl.col('最低') < pl.col('MA30'))
        ).drop_nulls()['日期']
        
        if cls.today in fallbelow:
            cls.report = cls.report.with_columns(
                pl.when(pl.col('编号') == '编号')
                  .then(pl.lit('下降趋势 4-1'))
                  .when(pl.col('编号') == cls.symbol)
                  .then(pl.lit(True))
                  .otherwise(pl.lit(''))
                  .alias('下降趋势 4-1')
            )
            Logger.info(f'@ 触发 Stock-Low 下穿跌破 MA-Month-30 线：\n{[d.strftime("%Y-%m-%d") for d in fallbelow]}')


class SmallFluctuation:

    @classmethod
    def run(cls, product, symbol, today, report):
        cls.product = product
        cls.symbol = symbol
        cls.today = today
        cls.report = report
        cls.filter = (pl.col('日期') <= cls.today) & (pl.col('日期') >= cls.today - timedelta(days=60 + 7))

        Logger.info(f'\n4、小调整')

        cls.condition1()
        cls.condition2()
        return cls.report

    @classmethod
    def condition1(cls):
        Logger.info(f'(1) 月小坑，参考月 30 价格。')

        selected_macd_month = pl.read_parquet(Config['Paths']['DataPath'] / cls.product / cls.symbol / f'macd_month.parquet').filter(cls.filter)
        selected_ma_month = pl.read_parquet(Config['Paths']['DataPath'] / cls.product / cls.symbol / 'ma_month.parquet').filter(cls.filter)
        selected_stock_month = pl.read_parquet(Config['Paths']['DataPath'] / cls.product / cls.symbol / 'month.parquet').filter(cls.filter)

        smallmonthhole = selected_macd_month.filter(
            (pl.col('MACD') < 0) & (pl.col('DIF') > 0) & (pl.col('DIF') < pl.col('DEA'))
        ).drop_nulls()['日期']
        
        if cls.today in smallmonthhole:
            cls.report = cls.report.with_columns(
                pl.when(pl.col('编号') == '编号')
                  .then(pl.lit('小调整 1-1'))
                  .when(pl.col('编号') == cls.symbol)
                  .then(pl.lit(True))
                  .otherwise(pl.lit(''))
                  .alias('小调整 1-1')
            )
            Logger.info(f'@ 触发 MACD-Month 月小坑：\n{[d.strftime("%Y-%m-%d") for d in smallmonthhole]}')
            
            groups, date_indices = [], {date: idx for idx, date in enumerate(selected_macd_month['日期'])}
            for i, d in enumerate(smallmonthhole):
                if i == 0 or date_indices[d] != date_indices[smallmonthhole[i-1]] + 1:
                    groups.append([d])
                else:
                    groups[-1].append(d)
            for i, group in enumerate(groups):
                Logger.info(f'时间区间 {i+1}: [{group[0] if len(group) == 1 else f"{group[0]} ~ {group[-1]}"}]')

        fallbelow = selected_stock_month.join(selected_ma_month.select('日期', 'MA30'), on='日期').filter(
            (pl.col('最低') >= pl.col('MA30'))
        ).drop_nulls()['日期']
        intersection = smallmonthhole.filter(smallmonthhole.is_in(fallbelow.implode()))

        if cls.today in intersection:
            cls.report = cls.report.with_columns(
                pl.when(pl.col('编号') == '编号')
                  .then(pl.lit('小调整 1-2'))
                  .when(pl.col('编号') == cls.symbol)
                  .then(pl.lit(True))
                  .otherwise(pl.lit(''))
                  .alias('小调整 1-2')
            )
            Logger.info(f'@ 始终站上 MA-Month-30 线：')
            groups, date_indices = [], {date: idx for idx, date in enumerate(selected_macd_month['日期'])}
            for i, d in enumerate(intersection):
                if i == 0 or date_indices[d] != date_indices[intersection[i-1]] + 1:    
                    groups.append([d])
                else:
                    groups[-1].append(d)
            for i, group in enumerate(groups):
                Logger.info(f'时间区间 {i+1}: [{group[0] if len(group) == 1 else f"{group[0]} ~ {group[-1]}"}]')

    @classmethod
    def condition2(cls):
        Logger.info(f'(2) 坑内出来以后，Boll 周下是买点。')

        selected_macd_month = pl.read_parquet(Config['Paths']['DataPath'] / cls.product / cls.symbol / f'macd_month.parquet').filter(cls.filter)

        climbupmonthhole = selected_macd_month.filter(
            (pl.col('DIF') > 0) & (pl.col('DIF').shift(1) < 0) & (pl.col('DEA') < 0) & (pl.col('DEA').shift(1) < 0) & (pl.col('DIF') > pl.col('DEA'))
        ).drop_nulls()['日期']

        if cls.today in climbupmonthhole:
            cls.report = cls.report.with_columns(
                pl.when(pl.col('编号') == '编号')
                  .then(pl.lit('小调整 2-1'))
                  .when(pl.col('编号') == cls.symbol)
                  .then(pl.lit(True))
                  .otherwise(pl.lit(''))
                  .alias('小调整 2-1')
            )
            Logger.info(f'@ 触发 MACD-Month 坑内出：{[d.strftime("%Y-%m-%d") for d in climbupmonthhole]}')

        # latest_stock_low < latest_boll_week


class CycleFluctuation:

    @classmethod
    def run(cls, product, symbol, today, report):
        cls.product = product
        cls.symbol = symbol
        cls.today = today
        cls.report = report
        cls.filter = (pl.col('日期') <= cls.today) & (pl.col('日期') >= cls.today - timedelta(days=60 + 7))

        Logger.info(f'\n2、周期判断')

        cls.condition1()
        cls.condition2()
        return cls.report

    @classmethod
    def condition1(cls):
        Logger.info(f'(1) 收盘价跌破 250 日。')

        selected_stock_day = pl.read_parquet(Config['Paths']['DataPath'] / cls.product / cls.symbol / 'day.parquet').filter(cls.filter)
        selected_ma_day = pl.read_parquet(Config['Paths']['DataPath'] / cls.product / cls.symbol / 'ma_day.parquet').filter(cls.filter)

        fallbelow = selected_stock_day.join(selected_ma_day.select('日期', 'MA250'), on='日期').filter(
            (pl.col('收盘').shift(1) > pl.col('MA250').shift(1)) & (pl.col('收盘') < pl.col('MA250'))
        ).drop_nulls()['日期']

        if cls.today in fallbelow:
            cls.report = cls.report.with_columns(
                pl.when(pl.col('编号') == '编号')
                  .then(pl.lit('周期 1-1'))
                  .when(pl.col('编号') == cls.symbol)
                  .then(pl.lit(True))
                  .otherwise(pl.lit(''))
                  .alias('周期 1-1')
            )
            Logger.info(f'@ 触发跌破 MA-Day-250：\n{[d.strftime("%Y-%m-%d") for d in fallbelow]}')

    @classmethod
    def condition2(cls):
        Logger.info(f'(2) 收盘价跌破 250 周。')

        selected_stock_week = pl.read_parquet(Config['Paths']['DataPath'] / cls.product / cls.symbol / 'week.parquet').filter(cls.filter)
        selected_ma_week = pl.read_parquet(Config['Paths']['DataPath'] / cls.product / cls.symbol / 'ma_week.parquet').filter(cls.filter)

        fallbelow = selected_stock_week.join(selected_ma_week.select('日期', 'MA250'), on='日期').filter(
            (pl.col('收盘').shift(1) > pl.col('MA250').shift(1)) & (pl.col('收盘') < pl.col('MA250'))
        ).drop_nulls()['日期']

        if cls.today in fallbelow:
            cls.report = cls.report.with_columns(
                pl.when(pl.col('编号') == '编号')
                  .then(pl.lit('周期 2-1'))
                  .when(pl.col('编号') == cls.symbol)
                  .then(pl.lit(True))
                  .otherwise(pl.lit(''))
                  .alias('周期 2-1')
            )
            Logger.info(f'@ 触发跌破 MA-Week-250：\n{[d.strftime("%Y-%m-%d") for d in fallbelow]}')


if __name__ == '__main__':
    AscendTrend(product='stock', stock='600025', today=date(2026, 4, 13), report=None)
