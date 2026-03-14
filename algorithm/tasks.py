#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------


import copy
import shutil
import traceback
import xlsxwriter
import polars as pl
from datetime import date

from project.configuration import Config
from algorithm.basic.printf import Printf
from algorithm.middleware import Callback, Logger, Process

from algorithm.core.product import Stocks, ETFs
from algorithm.core.fetch import WriteData, SplitData, Index
from algorithm.core.trend import AscendTrend, DescendTrend, SmallFluctuations
from algorithm.core.judgement import ValuationSignal, BottomSignal


class DataTask:

    taskid, callback = None, Config['Callbacks']['Mock']

    @classmethod
    async def run(cls, body):
        """创建并启动数据任务"""
        cls.taskid = body['taskid']
        cls.callback = body['callback']

        data_task_process = Process(taskid=cls.taskid, function=cls.main)
        message = await data_task_process.start()
        return message

    @classmethod
    def main(cls):
        """运行数据处理任务"""
        try:
            for symbol in Stocks:
                WriteData().stocks(symbol)
                SplitData().stocks(symbol)

            for symbol in ETFs:
                WriteData().etfs(symbol)
                SplitData().etfs(symbol)

            Index.run()
            message = Logger(code=200, taskid=cls.taskid, information=f"数据处理任务成功完成。")
        except Exception as error:
            message = Logger(code=500, taskid=cls.taskid, information=f"错误信息: {error}\n{traceback.format_exc()}")
        
        Callback(url=cls.callback, message=message)


class AlgorithmTask:

    today, taskid, callback = None, None, Config['Callbacks']['Mock']

    @classmethod
    async def run(cls, body):
        cls.taskid = body['taskid']
        cls.callback = body['callback']
        cls.today = date.fromisoformat(body['today'])

        algorithm_process = Process(taskid=cls.taskid, function=cls.main)
        message = await algorithm_process.start()
        return message

    @classmethod
    def main(cls, today=date.today()):
        """运行算法任务"""
        try:
            today = cls.today if cls.today else copy.deepcopy(today)

            shutil.copy(
                Config['Paths']['DataPath'] / 'output' / 'stock.xlsx',
                Config['Paths']['DataPath'] / 'output' / 'stock-today.xlsx'
            )
            for stock, name in Stocks.items():
                cls.stock(stock, name, today=today)
        
            shutil.copy(
                Config['Paths']['DataPath'] / 'output' / 'etf.xlsx',
                Config['Paths']['DataPath'] / 'output' / 'etf-today.xlsx'
            )
            for etf, name in ETFs.items():
                cls.etf(etf, name, today=today)

            stock_report = pl.read_excel(Config['Paths']['DataPath'] / 'output' / f'stock-today.xlsx')
            etf_report = pl.read_excel(Config['Paths']['DataPath'] / 'output' / f'etf-today.xlsx')
            with xlsxwriter.Workbook(Config['Paths']['DataPath'] / 'output' / f'report-{today.strftime("%Y%m%d")}.xlsx') as workbook:
                stock_report.write_excel(workbook, worksheet="Stocks")
                etf_report.write_excel(workbook, worksheet="ETFs")

            message = Logger(code=200, taskid=cls.taskid, information=f"算法任务成功完成。")
        except Exception as error:
            message = Logger(code=500, taskid=cls.taskid, information=f"错误信息: {error}\n{traceback.format_exc()}")
        
        Callback(url=cls.callback, message=message)

    @classmethod
    def stock(cls, stock='sh600025', name='华能水电', today=date.today(), output=f'stock-today.xlsx'):
        Printf.info(f'\n{'='*16} {stock} {name} {'='*16}')

        Printf.info(f'\n{'-'*20} 趋势判断 {'-'*20} ')
        AscendTrend(stock, product='stock', selected_date=today, output=output)
        DescendTrend(stock, product='stock', selected_date=today, output=output)
        SmallFluctuations(stock, product='stock', selected_date=today, output=output)

        Printf.info(f'\n{'-'*20} 策略分类 {'-'*20} ')
        ValuationSignal(stock, product='stock', selected_date=today, output=output)
        BottomSignal(stock, product='stock', selected_date=today, output=output)

    @classmethod
    def etf(cls, etf='sh600025', name='华能水电', today=date.today(), output=f'etf-today.xlsx'):
        Printf.info(f'\n{'='*16} {etf} {name} {'='*16}')
    
        Printf.info(f'\n{'-'*20} 趋势判断 {'-'*20} ')
        AscendTrend(etf, product='etf', selected_date=today, output=output)
        DescendTrend(etf, product='etf', selected_date=today, output=output)
        SmallFluctuations(etf, product='etf', selected_date=today, output=output)
    
        Printf.info(f'\n{'-'*20} 策略分类 {'-'*20} ')
        # ValuationSignal(etf, product='etf', selected_date=today, output=output)
        BottomSignal(etf, product='etf', selected_date=today, output=output)


class MainScheduler:

    @classmethod
    def run(cls):
        if date.today().weekday() >= 5:
            DataTask.main()            
            AlgorithmTask.main()
        else:
            Printf.info(f'\n{'-'*20} 今天非交易日 {'-'*20} ')


if __name__ == '__main__':
    DataTask.main()
    AlgorithmTask.main()
