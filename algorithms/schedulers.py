#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import os
import psutil
import traceback
import numpy as np
from datetime import datetime

from project.configuration import Config
from algorithms.middlewares import MessageQueue, Callback
from algorithms.algorithms import AlgorithmStartup


class AlgorithmScheduler:

    def __init__(self, queue):
        self.queue = queue

        if np.sum(np.array(psutil.cpu_percent(percpu=True)) < 80):
            MessageQueue.consume(queue=self.queue, acknowledge=self.acknowledge)
        else:
            print(f"{datetime.now()}: CPU 占用过高，算法定时任务等待中，稍后再进行尝试。")

    def acknowledge(self, body, message):
        try:
            if body:
                message.ack()
                AlgorithmStartup(body=body)
        except Exception as error:
            MessageQueue.produce(queue=self.queue, body=body)
            print(f"{error}\n{traceback.format_exc()}")


class CallbackScheduler:

    def __init__(self, queue):
        self.queue = queue

        if np.sum(np.array(psutil.cpu_percent(percpu=True)) < 80):
            MessageQueue.consume(queue=self.queue, acknowledge=self.acknowledge)
        else:
            print(f"{datetime.now()}: CPU 占用过高，回调定时任务等待中，稍后再进行尝试。")

    def acknowledge(self, body, message):
        try:
            if body:
                message.ack()
                Callback(url=body['url'], information=body['information'])
        except Exception as error:
            MessageQueue.produce(queue=self.queue, body=body)
            print(f"{error}\n{traceback.format_exc()}")


class CleanScheduler:

    def __init__(self):
        if Config['Information']['Mode'] in ['development']:
            print('缓存数据（json, log）清理定时任务处于 Development 模式。')
        else:
            remove_list = [
                Config['Paths']['DataPath'] / '*' / '*.json',
                Config['Paths']['DataPath'] / '*' / '*.log',
            ]
            for file in remove_list:
                os.system(f"date && rm -rfv {file}")
        print(f"缓存数据清理完成。")
