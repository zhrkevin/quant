#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------------------
# Copyright 2024 for Jingzhi & Level. All Rights Reserved.
# ---------------------------------------------------------

import os
import json
import asyncio

from project.configuration import Config
from algorithm.middleware import Logger


class Process:

    def __init__(self, taskid, function=None, *args, **kwargs):
        self.taskid = taskid
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.task = None

    async def start(self):
        # 在 asyncio 事件循环中运行任务
        self.task = asyncio.create_task(self._run_task())
        pid = os.getpid()
        tid = id(self.task)
        with open(Config['Paths']['DataPath'] / 'system' / f'pid-{self.taskid}.pid', 'w') as file:
            json.dump({'pid': pid, 'tid': tid}, file)

        message = Logger(code=100, taskid=self.taskid, information=f"任务 PID [{pid}] TID [{tid}] 已启动。")
        return message

    async def _run_task(self):
        """在异步环境中运行同步函数"""
        loop = asyncio.get_event_loop()
        # 使用 run_in_executor 在线程池中运行同步函数
        await loop.run_in_executor(None, self.function)

    async def join(self):
        if self.task:
            await self.task

    def is_alive(self):
        if self.task:
            return not self.task.done()
        return False

    def stop(self):
        if self.task and not self.task.done():
            self.task.cancel()
        message = Logger(code=100, taskid=self.taskid, information=f"异步任务已取消。")
        return message
