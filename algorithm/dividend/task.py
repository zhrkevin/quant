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
from algorithm.middleware import Logger, Callback, Process
from algorithm.dividend.product import Stocks, ETFs
from algorithm.dividend.fetch import WriteData, SplitData, Indices
from algorithm.dividend.trend import AscendTrend, DescendTrend, SmallFluctuation, CycleFluctuation
from algorithm.dividend.judgement import ValuationSignal, BottomSignal


pl.Config(tbl_rows=12, tbl_cols=8)


class DataTask:

    taskid, callback = None, Config['Callbacks']['Mock']

    @classmethod
    async def main(cls, body):
        """创建并启动数据任务"""
        cls.taskid = body['taskid']
        cls.callback = body['callback']

        data_task_process = Process(taskid=cls.taskid, function=cls.run)
        message = await data_task_process.start()
        return message

    @classmethod
    def run(cls, today=date.today()):
        """运行数据处理任务"""
        try:
            for symbol in Stocks:
                WriteData.run('stock', symbol, today)
                SplitData.run('stock', symbol, today)
                Indices.run('stock', symbol)

            for symbol in ETFs:
                WriteData.run('etf', symbol, today)
                SplitData.run('etf', symbol, today)
                Indices.run('etf', symbol)

            message = Logger.info(taskid=cls.taskid, information="数据处理任务成功完成。")
        except Exception as error:
            message = Logger.error(taskid=cls.taskid, information=f"错误信息: {error}\n{traceback.format_exc()}")
        
        Callback(url=cls.callback, message=message)


class AlgorithmTask:

    today, taskid, callback = None, None, Config['Callbacks']['Mock']

    @classmethod
    async def main(cls, body):
        cls.taskid = body['taskid']
        cls.callback = body['callback']
        cls.today = date.fromisoformat(body['today'])

        algorithm_process = Process(taskid=cls.taskid, function=cls.run, kwargs={'today': cls.today})
        message = await algorithm_process.start()
        return message

    @classmethod
    def run(cls, today):
        """运行算法任务"""
        try:
            stock_report = cls.stocks(today)
            etf_report = cls.etfs(today)

            with pd.ExcelWriter(Config['Paths']['DataPath'] / f'dividend/report' / f'report-{today.strftime("%Y%m%d")}.xlsx', mode='w+', engine='openpyxl') as xlsx:
                stock_report.to_pandas().to_excel(xlsx, sheet_name='stock', index=False)
                etf_report.to_pandas().to_excel(xlsx, sheet_name='etf', index=False)

            message = Logger.info(taskid=cls.taskid, information=f"算法任务成功完成。")
        except Exception as error:
            message = Logger.error(taskid=cls.taskid, information=f"错误信息: {error}\n{traceback.format_exc()}")
        
        Callback(url=cls.callback, message=message)

    @classmethod
    def stocks(cls, today):
        report = pl.read_excel(Config['Paths']['DataPath'] / f'dividend/report' / 'all.xlsx', sheet_name='stock')

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
    def etfs(cls, today):  
        report = pl.read_excel(Config['Paths']['DataPath'] / f'dividend/report' / 'all.xlsx', sheet_name='etf')

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
        # DataTask.run(today)
        AlgorithmTask.run(today)
        print(f'\n{'-'*20} {datetime.now()} 星期{ today.weekday()+1} {'-'*20}')


if __name__ == '__main__':
    # MainScheduler.run(today=date(2026, 4, 30))
    MainScheduler.run()
