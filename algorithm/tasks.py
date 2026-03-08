#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import shortuuid
import traceback
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
from algorithm.middleware import Callback, Logger, Process


class DataTask:

    taskid, callback = None, None

    @classmethod
    async def create(cls, body):
        cls.taskid = body['taskid'] or shortuuid.uuid()
        cls.callback = body['callback'] or Config.Callbacks.Mock

        data_task_process = Process(taskid=cls.taskid, function=cls.data_task)
        message = await data_task_process.start()
        # await data_task_process.join()
        return message

    @classmethod
    def data_task(cls):
        try:
            # WriteData()
            SplitData()
            Index()
            message = Logger(code=200, taskid=cls.taskid, information=f"数据处理任务成功结束。")
        except Exception as error:
            message = Logger(code=500, taskid=cls.taskid, information=f"错误信息: {error}\n{traceback.format_exc()}")

        Callback(url=cls.callback, message=message)


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

        with xlsxwriter.Workbook(Config.Paths.DataPath / 'output' / f'report-{self.today.strftime("%Y%m%d")}.xlsx') as workbook:
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
    DataTask(
        body={
            'taskid': None,
            'callback': None
        }
    )
    # AlgorithmTask(
    #     # today=date(2026, 3, 6),
    # )
