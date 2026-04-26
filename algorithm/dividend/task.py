#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import traceback
import pandas as pd
import polars as pl
from datetime import date, datetime

from project import Config
from algorithm.middleware import Callback, Logger, Process
from algorithm.basic.fetch import WriteData, SplitData, Indices
from algorithm.dividend.product import Stocks, ETFs
from algorithm.dividend.trend import AscendTrend, DescendTrend, SmallFluctuation, CycleFluctuation
from algorithm.dividend.judgement import ValuationSignal, BottomSignal


pl.Config(tbl_rows=-1, tbl_cols=-1)


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
                WriteData.stocks(symbol)
                SplitData.stocks(symbol)

            for symbol in ETFs:
                WriteData.etfs(symbol)
                SplitData.etfs(symbol)

            Indices.run()
            message = Logger.info(taskid=cls.taskid, information="数据处理任务成功完成。")
        except Exception as error:
            message = Logger.error(taskid=cls.taskid, information=f"错误信息: {error}\n{traceback.format_exc()}")
        
        Callback(url=cls.callback, message=message)


class AlgorithmTask:

    today, taskid, callback = None, None, Config['Callbacks']['Mock']

    @classmethod
    async def run(cls, body):
        cls.taskid = body['taskid']
        cls.callback = body['callback']
        cls.today = date.fromisoformat(body['today'])

        algorithm_process = Process(taskid=cls.taskid, function=cls.main, kwargs={'today': cls.today})
        message = await algorithm_process.start()
        return message

    @classmethod
    def main(cls, today):
        """运行算法任务"""
        try:
            stock_report = cls.stock(today)
            etf_report = cls.etf(today)

            with pd.ExcelWriter(Config['Paths']['DataPath'] / 'output' / f'report-{today.strftime("%Y%m%d")}.xlsx', mode='w+', engine='openpyxl') as xlsx:
                stock_report.to_pandas().to_excel(xlsx, sheet_name='stock', index=False)
                etf_report.to_pandas().to_excel(xlsx, sheet_name='etf', index=False)

            message = Logger.info(taskid=cls.taskid, information=f"算法任务成功完成。")
        except Exception as error:
            message = Logger.error(taskid=cls.taskid, information=f"错误信息: {error}\n{traceback.format_exc()}")
        
        Callback(url=cls.callback, message=message)

    @classmethod
    def stock(cls, today):
        report = pl.read_excel(Config['Paths']['DataPath'] / 'output' / 'all.xlsx', sheet_name='stock')

        for stock, name in Stocks.items():
            print(f'\n{'='*20} {stock} {name} {'='*20}')

            print(f'\n{'-'*20} 趋势判断 {'-'*20} ')
            report = AscendTrend.run(product='stock', symbol=stock, today=today, report=report)
            report = DescendTrend.run(product='stock', symbol=stock, today=today, report=report)
            report = SmallFluctuation.run(product='stock', symbol=stock, today=today, report=report)
            report = CycleFluctuation.run(product='stock', symbol=stock, today=today, report=report)

            print(f'\n{'-'*20} 策略分类 {'-'*20} ')
            report = ValuationSignal.run(product='stock', symbol=stock, today=today, report=report)
            report = BottomSignal.run(product='stock', symbol=stock, today=today, report=report)

        return report

    @classmethod
    def etf(cls, today):  
        report = pl.read_excel(Config['Paths']['DataPath'] / 'output' / 'all.xlsx', sheet_name='etf')

        for etf, name in ETFs.items():
            print(f'\n{'='*20} {etf} {name} {'='*20}')

            print(f'\n{'-'*20} 趋势判断 {'-'*20} ')
            report = AscendTrend.run(product='etf', symbol=etf, today=today, report=report)
            report = DescendTrend.run(product='etf', symbol=etf, today=today, report=report)
            report = SmallFluctuation.run(product='etf', symbol=etf, today=today, report=report)
            report = CycleFluctuation.run(product='etf', symbol=etf, today=today, report=report)

            print(f'\n{'-'*20} 策略分类 {'-'*20} ')
            report = BottomSignal.run(product='etf', symbol=etf, today=today, report=report)

        return report


class MainScheduler:

    @classmethod
    def run(cls, today=date.today()):
        DataTask.main()
        AlgorithmTask.main(today)
        print(f'\n{'-'*20} {datetime.now()} 星期{ today.weekday()+1} {'-'*20}')


if __name__ == '__main__':
    # MainScheduler.run(today=date(2026, 4, 24))
    MainScheduler.run()
