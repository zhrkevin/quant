#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import traceback

from project.configuration import Config
from algorithm.middlewares import MinIO, Logger, Process, Callback
from algorithm.basic.fetch import WriteStocks, SplitStocks


class DataProcessing:

    taskid, algorithm, schema, data, callback = None, None, None, None, None

    def __new__(cls, body):
        cls.taskid = body['taskid']
        cls.algorithm = body['algorithm']
        cls.callback = body['callback']

        data_preprocess_process = Process(taskid=cls.taskid, function=cls.data_processing)
        message = data_preprocess_process.start()
        return message

    @classmethod
    def data_processing(cls):
        try:
            WriteStocks()
            SplitStocks()
            message = Logger(code=200, taskid=cls.taskid, information=f"数据处理任务 [{cls.algorithm}] 成功结束。")
        except Exception as error:
            message = Logger(code=500, taskid=cls.taskid, information=f"错误信息: {error}\n{traceback.format_exc()}")

        Callback(url=cls.callback, message=message)


class AlgorithmRunning:

    taskid, algorithm, callback, models = None, None, None, None

    def __new__(cls, body, models=None):
        cls.taskid = body['taskid']
        cls.algorithm = body['algorithm']
        cls.callback = body['callback']

        algorithm_process = Process(taskid=cls.taskid, function=cls.algorithm_function)
        message = algorithm_process.start()
        return message

    @classmethod
    def algorithm_function(cls):
        try:
            message = Logger(code=200, taskid=cls.taskid, information=f"算法任务 [{cls.algorithm}] 启动开始。")
            AlgorithmsTask(taskid=cls.taskid, algorithm=cls.algorithm, models=cls.models)
            message = Logger(code=200, taskid=cls.taskid, information=f"算法任务 [{cls.algorithm}] 成功结束。")
        except Exception as error:
            message = Logger(code=250, taskid=cls.taskid, information=f"算法任务失败：{error}\n{traceback.format_exc()}。")

        Callback(url=cls.callback, message=message)


class DataDownload:

    taskid, schema, algorithm = None, None, None

    def __new__(cls, body):
        cls.algorithm = body['algorithm']
        cls.taskid = body['taskid']
        cls.schema = body['schema']

        file = cls.data_download()
        return file

    @classmethod
    def data_download(cls):
        if cls.schema in ['system/logs']:
            MinIO.download(filename=f'system/logs-{cls.taskid}.log')
            Logger(code=200, taskid='Default', information=f'下载文件数据 system/logs-{cls.taskid}.log。', mode='w')
            return Config['Paths']['DataPath'] / f'system/logs-{cls.taskid}.log'
        elif cls.schema in Inputs[cls.algorithm] + Outputs[cls.algorithm]:
            MinIO.download(filename=f'{cls.schema}-{cls.taskid}.json')
            return Config['Paths']['DataPath'] / f'{cls.schema}-{cls.taskid}.json'
        else:
            print(f'文件在服务器上不存在。')
            Logger(code=500, taskid='Default', information=f'system/logs-{cls.taskid}.log 不在服务器上。', mode='w')
            return Config['Paths']['DataPath'] / 'system/logs-Default.log'
