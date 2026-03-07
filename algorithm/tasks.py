#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import xlsxwriter
import polars as pl
from datetime import date

from project.configuration import Config
from algorithm.data.product import Stocks, ETFs
from algorithm.data.fetch import WriteData, SplitData
from algorithm.data.index import Index
from algorithm.basic.printf import Printf
from algorithm.core.trend import AscendTrend, DescendTrend, SmallFluctuations
from algorithm.core.judgement import ValuationSignal, BottomSignal


class DataTask:

    def __init__(self):
        # WriteData()
        SplitData()
        Index()


class AlgorithmTask:

    def __init__(self, today=date.today()):
        self.today = today
        self.run()

    def run(self):
        stock_report = pl.read_excel(Config.Paths.DataPath / 'output' / 'stock.xlsx')
        stock_report.write_excel(Config.Paths.DataPath / 'output' / f'stock-today.xlsx')
        for stock, name in Stocks.items():
            self.stock_algorithm(stock, name)
    
        etf_report = pl.read_excel(Config.Paths.DataPath / 'output' / 'etf.xlsx')
        etf_report.write_excel(Config.Paths.DataPath / 'output' / f'etf-today.xlsx')
        for etf, name in ETFs.items():
            self.etf_algorithm(etf, name)

        with xlsxwriter.Workbook(Config.Paths.DataPath / 'output' / f'{self.today.strftime("%Y%m%d")}.xlsx') as workbook:
            stock_report.write_excel(workbook, worksheet="Sheet1")
            etf_report.write_excel(workbook, worksheet="Sheet2")

    def stock_algorithm(self, stock='sh600025', name='华能水电', output=f'stock-today.xlsx'):
        Printf.info(f'\n{'='*16} {stock} {name} {'='*16}')

        Printf.info(f'\n{'-'*20} 趋势判断 {'-'*20} ')
        AscendTrend(stock, product='stock', selected_date=self.today, output=output)
        DescendTrend(stock, product='stock', selected_date=self.today, output=output)
        SmallFluctuations(stock, product='stock', selected_date=self.today, output=output)

        Printf.info(f'\n{'-'*20} 策略分类 {'-'*20} ')
        ValuationSignal(stock, product='stock', selected_date=self.today, output=output)
        BottomSignal(stock, product='stock', selected_date=self.today, output=output)

    def etf_algorithm(self, etf='sh600025', name='华能水电', output=f'etf-today.xlsx'):
        Printf.info(f'\n{'='*16} {etf} {name} {'='*16}')
    
        Printf.info(f'\n{'-'*20} 趋势判断 {'-'*20} ')
        AscendTrend(etf, product='etf', selected_date=self.today, output=output)
        DescendTrend(etf, product='etf', selected_date=self.today, output=output)
        SmallFluctuations(etf, product='etf', selected_date=self.today, output=output)
    
        Printf.info(f'\n{'-'*20} 策略分类 {'-'*20} ')
        # ValuationSignal(etf, product='etf', selected_date=self.today, output=output)
        BottomSignal(etf, product='etf', selected_date=self.today, output=output)


if __name__ == '__main__':
    # DataTask()
    AlgorithmTask(
        today=date(2026, 3, 6),
    )
