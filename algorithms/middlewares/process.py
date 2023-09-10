#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------
# Copyright 2023 for Fosun. All Rights Reserved.
# -----------------------------------------------

import copy
import json
import psutil
import multiprocess
from datetime import datetime

from project.configuration import Config


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
        message = {
            'code': 200,
            'taskid': self.taskid,
            'information': f"任务 PID [{pid}] 已启动。",
            'timestamp': datetime.now().strftime('%F %T.%f'),
        }
        return message

    def stop(self):
        with open(Config['Paths']['DataPath'] / 'system' / f'pid-{self.taskid}.pid', 'r') as file:
            pid = json.load(file)
        process = psutil.Process(pid)
        process.kill() if process.status() == 'running' else None
        process.wait()
        message = {
            'code': 200,
            'taskid': self.taskid,
            'information': f"任务 PID [{pid}] 已暂停。",
            'timestamp': datetime.now().strftime('%F %T.%f'),
        }
        return message
