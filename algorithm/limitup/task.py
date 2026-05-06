#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import traceback
from datetime import date

from project import Config
from algorithm.middleware import Logger, Callback, Process

from algorithm.limitup.fetch import LimitUp, WriteData
from algorithm.limitup.trend import Analysis


class AlgorithmTask:

    today, taskid, callback = None, None, Config['Callbacks']['Mock']

    @classmethod
    async def main(cls, body):
        cls.taskid = body['taskid']
        cls.callback = body['callback']
        cls.today = date.fromisoformat(body['today'])

        algorithm_process = Process(taskid=cls.taskid, function=cls.main)
        message = await algorithm_process.start()
        return message

    @classmethod
    def run(cls, today):
        """运行算法任务"""
        try:
            LimitUp.run(today)
            Analysis.run()
            WriteData.run()
            message = Logger.success(taskid=cls.taskid, information=f"算法任务成功完成。")
        except Exception as error:
            message = Logger.error(taskid=cls.taskid, information=f"错误信息: {error}\n{traceback.format_exc()}")
        
        Callback(url=cls.callback, message=message)


class MainScheduler:

    @classmethod
    def run(cls, today=date.today()):
        AlgorithmTask.run(today)
        Logger.success(information=f'{'-'*20} {today} 星期{ today.weekday()+1} {'-'*20}')


if __name__ == '__main__':
    MainScheduler.run()
