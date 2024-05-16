#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import traceback

from project.configuration import Config
from algorithms.middlewares import MinIO, Logger, Process, Callback
from algorithms.tasks import DataTask, AlgorithmsTask


Inputs = {
    "quant": ["input/texts"],
}

Outputs = {
    "quant": ["output/results"],
}


class DataProcessing:

    taskid, algorithm, schema, data, callback = None, None, None, None, None

    def __new__(cls, body):
        cls.algorithm = body['algorithm']
        cls.taskid = body['taskid']
        cls.callback = body['callback']
        cls.schema = body['schema']
        cls.data = body['data']

        data_preprocess_process = Process(taskid=cls.taskid, function=cls.data_processing)
        message = data_preprocess_process.start()
        return message

    @classmethod
    def data_processing(cls):
        try:
            MinIO.write(filename=f"input/{cls.schema}-{cls.taskid}.json", data=cls.data)
            MinIO.upload(filename=f"input/{cls.schema}-{cls.taskid}.json")
            failure = [
                f"{schema}.json" for schema in Inputs[cls.algorithm] if not MinIO.download(filename=f"input/{schema}-{cls.taskid}.json")
            ]
            if failure:
                message = Logger(code=200, taskid=cls.taskid, information=f"以下数据表 {failure} 中的数据未上传至 MinIO 算法暂未启动。")
            else:
                message = Logger(code=300, taskid=cls.taskid, information=f"算法任务 {cls.algorithm} 的所有数据准备完成。")
        except Exception as error:
            message = Logger(code=500, taskid=cls.taskid, information=f"错误信息: {error}\n{traceback.format_exc()}")

        Callback(url=cls.callback, message=message)


class AlgorithmStartup:

    taskid, algorithm, callback, models = None, None, None, None

    def __new__(cls, body, models=None):
        cls.taskid = body['taskid']
        cls.algorithm = body['algorithm']
        cls.callback = body['callback']
        cls.models = models

        algorithm_process = Process(taskid=cls.taskid, function=cls.algorithm_function)
        message = algorithm_process.start()
        return message

    @classmethod
    def algorithm_function(cls):
        try:
            message = Logger(code=180, taskid=cls.taskid, information=f"算法任务 [{cls.algorithm}] 启动开始。")
            inputs = {
                schema: MinIO.read(filename=f"{schema}-{cls.taskid}.json") for schema in Inputs[cls.algorithm]
            }
            outputs = AlgorithmsTask(taskid=cls.taskid, algorithm=cls.algorithm, models=cls.models, **inputs)
            results = {
                schema: MinIO.write(data=outputs[schema], filename=f"{schema}-{cls.taskid}.json") for schema in Outputs[cls.algorithm]
            }
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
