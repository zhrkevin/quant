#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import os
import httpx
import psutil
import traceback
import numpy as np
from sanic.log import logger

from project.configuration import Config
from algorithms.middlewares import MessageQueue
from algorithms.algorithms import AlgorithmStartup


class AlgorithmScheduler:

    def __init__(self, queue):
        self.queue = Config['RabbitMQ']['AlgorithmQueue']
        if np.sum(np.array(psutil.cpu_percent(percpu=True)) < 80):
            MessageQueue.consume(queue=self.queue, acknowledge=self.acknowledge)
        else:
            logger.info('CPU 占用过高，算法定时任务等待中，稍后再进行尝试。')

    def acknowledge(self, body, acknow):
        if body:
            acknow.ack()
            try:
                AlgorithmStartup(body=body)
            except Exception as error:
                MessageQueue.produce(queue=self.queue, body=body)
                print(f"{error}\n{traceback.format_exc()}")


class CallbackScheduler:

    def __init__(self):
        self.queue = Config['RabbitMQ']['CallbackQueue']
        if np.sum(np.array(psutil.cpu_percent(percpu=True)) < 80):
            MessageQueue.consume(queue=self.queue, acknowledge=self.acknowledge)
        else:
            logger.info('CPU 占用过高，回调定时任务等待中，稍后再进行尝试。')

    def acknowledge(self, body, acknow):
        if body:
            acknow.ack()
            try:
                logger.info(f"Httpx URL 请求 {body['url']}，第 {body['connection']} 次尝试。")
                response = httpx.post(url=body['url'], json=body['message'], timeout=4)
                assert response.is_success, response.json()
                logger.info(f"回调后端服务，请求成功 <Response {response.status_code}>: {response.json()}")
            except Exception as error:
                if body['connection'] < 5:
                    body['connection'] += 1
                    MessageQueue.produce(queue=self.queue, body=body)
                logger.error(f"回调后端服务，请求失败 <Response 500>: {error}")


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
