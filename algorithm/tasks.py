#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import shortuuid
import polars as pl
from datetime import date

import xlsxwriter

from project.configuration import Config
from algorithm.data.market import Stocks, ETFs
from algorithm.data.fetch import WriteETFs, SplitETFs, WriteStocks, SplitStocks
from algorithm.data.index import MovingAverage, MovingAverageConvergenceDivergence, BollingerBands

from algorithm.basic.printf import Printf
from algorithm.core.trend import AscendTrend, DescendTrend, SmallFluctuations
from algorithm.core.judgement import ValuationSignal, BottomSignal


class MergeTask:

    def __init__(self, today=None):
        self.today = today or date.today()
        self.etf = f'etf-{self.today.strftime("%Y%m%d")}.xlsx'
        self.stock = f'stock-{self.today.strftime("%Y%m%d")}.xlsx'
        self.report = f'report-{self.today.strftime("%Y%m%d")}.xlsx'

    def run(self):
        etf = pl.read_excel(Config.Paths.DataPath / 'output' / self.etf)
        stock = pl.read_excel(Config.Paths.DataPath / 'output' / self.stock)

        with xlsxwriter.Workbook(Config.Paths.DataPath / 'output' / self.report) as workbook:
            stock.write_excel(workbook, worksheet="Sheet1")
            stock.write_excel(workbook, worksheet="Sheet2")


class DataTask:

    def __init__(self):
        self.stocks_renew = False
        self.etfs_renew = False
    
    def run(self):
        WriteStocks(renew=self.stocks_renew)
        SplitStocks()
        
        WriteETFs(renew=self.etfs_renew)
        SplitETFs()

        for period in ['day', 'week', 'month', 'quarter']:
            for stock in Stocks:
                MovingAverage(stock, period=period, types='stock')
                MovingAverageConvergenceDivergence(stock, period=period, types='stock')
                BollingerBands(stock, period=period, types='stock')

            for etf in ETFs:
                MovingAverage(etf, period=period, types='etf')
                MovingAverageConvergenceDivergence(etf, period=period, types='etf')
                BollingerBands(etf, period=period, types='etf')


class StockAlgorithmTask:

    def __init__(self, today=None):
        self.today = today or date.today()
        self.input = 'stock.xlsx'
        self.output = f'stock-{self.today.strftime("%Y%m%d")}.xlsx'

    def run(self):
        report = pl.read_excel(Config.Paths.DataPath / 'output' / self.input)
        report.write_excel(Config.Paths.DataPath / 'output' / self.output)

        for stock, name in Stocks.items():
            self.debug(stock, name)
    
    def debug(self, stock: str = 'sh600025', name: str = '华能水电'):
        Printf.set(stock)
        Printf.info(f'\n{'='*16} {stock} {name} {'='*16}')

        Printf.info(f'\n{'-'*20} 趋势判断 {'-'*20} ')
        AscendTrend(stock, selected_date=self.today, output=self.output)
        DescendTrend(stock, selected_date=self.today, output=self.output)
        SmallFluctuations(stock, selected_date=self.today, output=self.output)

        Printf.info(f'\n{'-'*20} 策略分类 {'-'*20} ')
        ValuationSignal(stock, selected_date=self.today, output=self.output)
        BottomSignal(stock, selected_date=self.today, output=self.output)


if __name__ == '__main__':
    DataTask().run()
    StockAlgorithmTask().run()

    # MergeTask(
    #     taskid=shortuuid.random(24),
    #     today=date(2026, 2, 27),
    # ).run()
