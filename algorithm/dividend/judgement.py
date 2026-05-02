#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import datetime
import polars as pl

from project import Config
from algorithm.middleware.logger import Logger


class ValuationSignal:

    @classmethod
    def run(cls, product, symbol, today, report):
        cls.product = product
        cls.symbol = symbol
        cls.today = today
        cls.report = report
        
        Logger.info(f'\n股票 {cls.symbol} 日期 ({cls.today})')

        cls.condition()
        return cls.report

    @classmethod
    def condition(cls):
        stock_dividend = pl.read_excel(Config['Paths']['DataPath'] / f'dividend/{cls.product}' / 'dividends.xlsx')    
        stock_dividend = stock_dividend.filter(
            (pl.col('编号') == cls.symbol) & 
            (pl.col('年份') <= cls.today.year)
        )
        
        stock_data = pl.read_parquet(Config['Paths']['DataPath'] / f'dividend/{cls.product}/{cls.symbol}' / 'day.parquet')
        stock_close = stock_data.filter(pl.col('日期') <= cls.today)['收盘'][-1] 
        dividend_ratio = stock_dividend['分红'][-1] / stock_close * 100
        public_service = stock_dividend['公用事业'][-1]

        condition = '低估' if (
            (public_service and 3.6 < dividend_ratio <= 4.5) or 
            (not public_service and 4 < dividend_ratio <= 6)
        ) else '正常' if (
            (public_service and dividend_ratio <= 3.6) or 
            (not public_service and 3.5 < dividend_ratio <= 4)
        ) else '高估' if (
            (not public_service and 3 < dividend_ratio <= 3.5)
        ) else '异常'

        cls.report = cls.report.with_columns(
            pl.when(pl.col('编号') == '编号')
              .then(pl.lit('估值'))
              .when(pl.col('编号') == cls.symbol)
              .then(pl.lit(condition))
              .otherwise(pl.col('估值')).alias('估值'),
            pl.when(pl.col('编号') == '编号')
              .then(pl.lit('分红率'))
              .when(pl.col('编号') == cls.symbol)
              .then(pl.lit(f'{dividend_ratio:.4f}%'))
              .otherwise(pl.col('分红率')).alias('分红率'),
        )
        Logger.info(f'最新分红 {stock_dividend["分红"][-1]:.4f} 最新股价 {stock_close:.4f} 最新分红率 {dividend_ratio:.4f}%')


class BottomSignal:

    @classmethod
    def run(cls, product, symbol, today, report):
        cls.product = product
        cls.symbol = symbol
        cls.today = today
        cls.report = report
        
        Logger.info(f'\n股票 {cls.symbol} 日期 ({cls.today})')

        cls.condition1()
        cls.condition2()
        return cls.report

    @classmethod
    def condition1(cls):
        begin_date = cls.today.replace(year=cls.today.year - 6)

        stock_data = pl.read_parquet(Config['Paths']['DataPath'] / f'dividend/{cls.product}/{cls.symbol}' / 'day.parquet')
        selected_stock_data = stock_data.filter((pl.col('日期') >= begin_date) & (pl.col('日期') <= cls.today))
        
        toppest_stock_price = selected_stock_data['最高'].max()
        latest_stock_close = selected_stock_data['收盘'][-1]
        ratio = latest_stock_close / toppest_stock_price * 100

        cls.report = cls.report.with_columns(
            pl.when(pl.col('编号') == '编号')
              .then(pl.lit('较过去6年的最高股价的跌幅超过50%'))
              .when((pl.col('编号') == cls.symbol) & (ratio < 50))
              .then(pl.lit(True))
              .otherwise(pl.lit(''))
              .alias('跌幅超过50%'),
            pl.when(pl.col('编号') == '编号')
              .then(pl.lit('当前股价与6年内最高股价的比值'))
              .when((pl.col('编号') == cls.symbol))
              .then(pl.lit(f"{ratio:.3f}%"))
              .otherwise(pl.col('股价比值'))
              .alias('股价比值'),
        )

        Logger.info(f"\n股票 {cls.symbol} 底部参考值 ({ratio:.3f}% / 50%)")
        Logger.info(f"6 年最高股价 ({toppest_stock_price:.4f}) 当前股价 ({latest_stock_close:.4f})")

    @classmethod
    def condition2(cls):
        stock_data = pl.read_parquet(Config['Paths']['DataPath'] / f'dividend/{cls.product}/{cls.symbol}' / 'day.parquet')
        selected_stock_data = stock_data.filter(pl.col('日期') <= cls.today)
        latest_stock_high = selected_stock_data['最高'][-1]
        latest_stock_low = selected_stock_data['最低'][-1]

        boll_month = pl.read_parquet(Config['Paths']['DataPath'] / f'dividend/{cls.product}/{cls.symbol}' / 'boll_month.parquet')
        latest_boll_month_lower = boll_month.filter(pl.col('日期') <= cls.today)['Boll下轨'][-1]

        boll_quarter = pl.read_parquet(Config['Paths']['DataPath'] / f'dividend/{cls.product}/{cls.symbol}' / 'boll_quarter.parquet')
        selected_boll_quarter = boll_quarter.filter(pl.col('日期') <= cls.today)
        latest_boll_quarter_mid = selected_boll_quarter['Boll中轨'][-1] or 0
        latest_boll_quarter_lower = selected_boll_quarter['Boll下轨'][-1] or 0

        if latest_boll_month_lower >= latest_stock_low >= latest_boll_quarter_lower and latest_stock_high <= latest_boll_quarter_mid:
            Logger.info(f"\n股票 {cls.symbol} 触发底部信号 Boll 月线下轨 + 季线中下轨")
            Logger.info(f"当前 ({cls.today}) 股价最高值 ({latest_stock_high:.4f}) 最低值 ({latest_stock_low:.4f})")
            Logger.info(f"Boll 月线下轨 ({latest_boll_month_lower:.4f})")
            Logger.info(f"Boll 季线中下轨 ({latest_boll_quarter_mid:.4f} {latest_boll_quarter_lower:.4f})")

            cls.report = cls.report.with_columns(
                pl.when(pl.col('编号') == '编号')
                  .then(pl.lit('底部信号:Boll月线下轨+季线中下轨'))
                  .when(pl.col('编号') == cls.symbol)
                  .then(pl.lit(True))
                  .otherwise(pl.lit(''))
                  .alias('底部信号')
            )


if __name__ == '__main__':
    report = pl.read_excel(Config['Paths']['DataPath'] / 'output' / 'sh600941-today.xlsx')
    valuation_signal = ValuationSignal('sh600941', report=report)
    bottom_signal = BottomSignal('sh600941', today=datetime.date(2026, 2, 25), report=report)
