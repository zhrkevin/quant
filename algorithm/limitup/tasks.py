#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import copy
import traceback
import polars as pl
from datetime import date

from project.configuration import Config
from algorithm.middleware import Callback, Logger, Process

from algorithm.limitup.fetch import WriteData, SplitData, Indices
from algorithm.limitup.trend import Trend


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
    def main(cls, symbol = '600036'):
        """运行数据处理任务"""
        try:
            WriteData.run(symbol)
            SplitData.run(symbol)
            Indices.run(symbol)
            message = Logger.success(code=200, taskid=cls.taskid, information=f"数据处理任务成功完成。")
        except Exception as error:
            message = Logger.error(code=500, taskid=cls.taskid, information=f"错误信息: {error}\n{traceback.format_exc()}")
        
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
    def main(cls, symbol='600036', today=date.today()):
        """运行算法任务"""
        try:
            today = cls.today if cls.today else copy.deepcopy(today)

            Trend.run(symbol)
            message = Logger.success(code=200, taskid=cls.taskid, information=f"算法任务成功完成。")
        except Exception as error:
            message = Logger.error(code=500, taskid=cls.taskid, information=f"错误信息: {error}\n{traceback.format_exc()}")
        
        Callback(url=cls.callback, message=message)


class MainScheduler:

    @classmethod
    def run(cls, symbol='600036'):
        DataTask.main(symbol)            
        AlgorithmTask.main(symbol)


if __name__ == '__main__':
    MainScheduler.run(symbol='600726')
