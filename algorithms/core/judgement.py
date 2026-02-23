#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import copy
import datetime
import polars as pl
from algorithms.basic.plot import Plotting

from project.configuration import Config


class ValuationSignal:

    def __init__(self, symbol: str, selected_date: datetime.date = None):
        self.symbol = symbol
        self.selected_date = selected_date

        self.condition()

    def condition(self):
        selected_date = datetime.date(2026, 1, 1) if self.selected_date is None else copy.deepcopy(self.selected_date)
        
        selected_stock_dividend = pl.read_excel(Config.Paths.DataPath / 'input' / 'dividends.xlsx', sheet_name='Sheet1')
        selected_stock_dividend = selected_stock_dividend.filter(
            (pl.col('股票编号') == self.symbol) & 
            (pl.col('年份') <= selected_date.year)
        )
        
        stock_data = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / 'stock_day.parquet')
        latest_stock_close = stock_data.filter(pl.col('date') <= selected_date)['qfq_close'][-1]

        latest_dividend_ratio = selected_stock_dividend['分红'][-1] / latest_stock_close
        latest_shiye = selected_stock_dividend['公用事业'][-1]

        condition = '低估' if (
            (latest_shiye and 0.036 < latest_dividend_ratio <= 0.045) or 
            (not latest_shiye and 0.04 < latest_dividend_ratio <= 0.06)
        ) else '正常' if (
            (latest_shiye and latest_dividend_ratio <= 0.036) or 
            (not latest_shiye and 0.035 < latest_dividend_ratio <= 0.04)
        ) else '高估' if (
            (not latest_shiye and 0.03 < latest_dividend_ratio <= 0.035)
        ) else '异常'

        print(f'股票 {self.symbol} 日期 ({selected_date}) {condition}')
        print(f'最新分红 {selected_stock_dividend["分红"][-1]:.4f} 最新股价 {latest_stock_close:.4f} 最新分红率 {latest_dividend_ratio:.4f}')


class BottomSignal:

    def __init__(self, symbol: str, selected_date: datetime.date = None, adjust: str = 'raw'):
        self.symbol = symbol
        self.selected_date = selected_date
        self.adjust = adjust

        self.condition1()
        self.condition2()
    
    def condition1(self):
        selected_date = datetime.date(2026, 1, 1) if self.selected_date is None else copy.deepcopy(self.selected_date)
        
        begin_date = selected_date.replace(year=selected_date.year - 10)

        stock_data = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / 'stock_day.parquet')
        selected_stock_data = stock_data.filter((pl.col('date') >= begin_date) & (pl.col('date') <= selected_date))
        
        toppest_stock_price = selected_stock_data[f'{self.adjust}_high'].max()
        latest_stock_close = selected_stock_data[f'{self.adjust}_close'][-1]
        ratio = latest_stock_close / toppest_stock_price * 100

        print(f"\n股票 {self.symbol} 底部参考值 ({ratio:.2f}% / 50%)")
        print(f"10 年最高股价 ({toppest_stock_price:.4f}) 当前股价 ({latest_stock_close:.4f})")

    def condition2(self):
        selected_date = datetime.date(2022, 11, 2) if self.selected_date is None else copy.deepcopy(self.selected_date)
        
        stock_data = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / 'stock_day.parquet')
        selected_stock_data = stock_data.filter(pl.col('date') <= selected_date)
        latest_stock_high = selected_stock_data[f'{self.adjust}_high'][-1]
        latest_stock_low = selected_stock_data[f'{self.adjust}_low'][-1]

        boll_month = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / 'boll_month.parquet')
        latest_boll_month_lower = boll_month.filter(pl.col('date') <= selected_date)['boll_lower'][-1]

        boll_quarter = pl.read_parquet(Config.Paths.DataPath / 'input' / self.symbol / 'boll_quarter.parquet')
        selected_boll_quarter = boll_quarter.filter(pl.col('date') <= selected_date)
        latest_boll_quarter_mid = selected_boll_quarter['boll_mid'][-1]
        latest_boll_quarter_lower = selected_boll_quarter['boll_lower'][-1]

        if latest_boll_month_lower >= latest_stock_low >= latest_boll_quarter_lower and latest_stock_high <= latest_boll_quarter_mid:
            print(f"\n股票 {self.symbol} 触发底部信号 Boll 月线下轨 + 季线中下轨")
            print(f"当前 ({selected_date}) 股价最高值 ({latest_stock_high:.4f}) 最低值 ({latest_stock_low:.4f})")
            print(f"Boll 月线下轨 ({latest_boll_month_lower:.4f})")
            print(f"Boll 季线中下轨 ({latest_boll_quarter_mid:.4f} {latest_boll_quarter_lower:.4f})")


if __name__ == '__main__':
    ValuationSignal('sh600036')
    BottomSignal('sh600036')
    Plotting('sh600036', period='month', window=10000)
    Plotting('sh600036', period='quarter', window=10000)
