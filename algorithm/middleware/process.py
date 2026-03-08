#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------------------
# Copyright 2024 for Jingzhi & Level. All Rights Reserved.
# ---------------------------------------------------------

import asyncio
import concurrent.futures


class Process:

    def __init__(self, taskid, function=None, *args, **kwargs):
        self.taskid = taskid
        self.function = function
        self.args = args
        self.kwargs = kwargs
        self.task = None
        self.executor = None

    async def start(self):
        """启动任务"""
        self.executor = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        self.task = asyncio.create_task(self._execute_function())
        return {'code': 100, 'taskid': self.taskid, 'information': f'任务已启动。'}

    async def _execute_function(self):
        """在异步环境中运行同步函数"""
        try:
            loop = asyncio.get_event_loop()
            
            if self.kwargs:
                self.future = loop.run_in_executor(
                    self.executor, 
                    lambda: self.function(*self.args, **self.kwargs)
                )
            else:
                self.future = loop.run_in_executor(
                    self.executor, 
                    self.function, 
                    *self.args
                )
            await self.future
        finally:
            if self.executor:
                self.executor.shutdown(wait=False)

    async def stop(self):
        """停止任务"""
        if self.task and not self.task.done():
            self.task.cancel()
            try:
                await asyncio.wait_for(self.task, timeout=5.0)
            except (asyncio.TimeoutError, asyncio.CancelledError):
                pass
        
        return {'code': 100, 'taskid': self.taskid, 'information': f'任务已取消。'}
