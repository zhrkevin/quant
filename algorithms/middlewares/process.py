#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import copy
import json
import psutil
import multiprocess

from project.configuration import Config
from algorithms.middlewares import Logger


class Process:

    def __init__(self, taskid, function=None, *args, **kwargs):
        self.taskid = taskid
        self.function = function
        self.args = args
        self.kwargs = kwargs

    def start(self):
        process = multiprocess.context.Process(target=self.function, *self.args, **self.kwargs)
        process.start()
        pid = copy.deepcopy(process.pid)
        with open(Config['Paths']['DataPath'] / 'system' / f'pid-{self.taskid}.pid', 'w') as file:
            json.dump(pid, file)

        message = Logger(code=100, taskid=self.taskid, information=f"任务 PID [{pid}] 已启动。")
        return message

    def stop(self):
        with open(Config['Paths']['DataPath'] / 'system' / f'pid-{self.taskid}.pid', 'r') as file:
            pid = json.load(file)
        process = psutil.Process(pid)
        process.kill() if process.status() == 'running' else None
        process.wait()

        message = Logger(code=100, taskid=self.taskid, information=f"任务 PID [{pid}] 已终止。")
        return message
