#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------
# Copyright 2023 for Fosun. All Rights Reserved.
# -----------------------------------------------

import traceback

from algorithms.middlewares import MinIO, Logger, Process
from algorithms.tasks import DataTask, AlgorithmsTask


Inputs = {
    "nl-sql": ["input/metadata", "input/question"],
    "generate-content": ["input/texts"],
}

Outputs = {
    "nl-sql": ["output/results"],
    "generate-content": ["output/results"],
}


class DataPreprocess:

    taskid, algorithm = None, None
    schema, data = None, None

    def __new__(cls, body):
        cls.taskid = body['taskid']
        cls.algorithm = body['algorithm']
        cls.schema = body['schema']
        cls.data = body

        data_preprocess_process = Process(taskid=cls.taskid, function=cls.data_preprocess_function)
        message = data_preprocess_process.start()
        return message

    @classmethod
    def data_preprocess_function(cls):
        try:
            MinIO.write(data=cls.data, filename=f"{cls.schema}-{cls.taskid}.json")
            unavailable = [
                f"{schema}-{cls.taskid}.json" for schema in Inputs[cls.algorithm] if not MinIO.check(filename=f"{schema}-{cls.taskid}.json")
            ]
            DataTask(taskid=cls.taskid, algorithm=cls.algorithm, unavailable=unavailable)
        except FileExistsError as warning:
            Logger(code=120, taskid=cls.taskid, information=f"数据任务提醒：{warning}。")
        except Exception as error:
            Logger(code=150, taskid=cls.taskid, information=f"数据任务失败：{error}\n{traceback.format_exc()}。")


class AlgorithmStartup:

    taskid, algorithm, version, models = None, None, None, None

    def __new__(cls, body, models=None):
        cls.taskid = body['taskid']
        cls.algorithm = body['algorithm']
        cls.version = body['version']
        cls.models = models

        algorithm_process = Process(taskid=cls.taskid, function=cls.algorithm_function)
        message = algorithm_process.start()
        return message

    @classmethod
    def algorithm_function(cls):
        try:
            Logger(code=180, taskid=cls.taskid, information=f"算法任务 [{cls.algorithm}] 启动开始。")
            inputs = {
                schema: MinIO.read(filename=f"{schema}-{cls.taskid}.json") for schema in Inputs[cls.algorithm]
            }
            outputs = AlgorithmsTask(taskid=cls.taskid, algorithm=cls.algorithm, version=cls.version, models=cls.models, **inputs)
            results = {
                schema: MinIO.write(data=outputs[schema], filename=f"{schema}-{cls.taskid}.json") for schema in Outputs[cls.algorithm]
            }
            Logger(code=200, taskid=cls.taskid, information=f"算法任务 [{cls.algorithm}] 成功结束。")
        except Exception as error:
            Logger(code=250, taskid=cls.taskid, information=f"算法任务失败：{error}\n{traceback.format_exc()}。")


class DataDownload:

    taskid, schema, algorithm = None, None, None

    def __new__(cls, body):
        cls.algorithm = body['algorithm']
        cls.taskid = body['taskid']
        cls.schema = body['schema']

        if cls.schema in ['system/logs']:
            results = MinIO.read(filename=f'{cls.schema}-{cls.taskid}.log')
            return results, 'text'
        elif cls.schema in Inputs[cls.algorithm] + Outputs[cls.algorithm]:
            results = MinIO.read(filename=f'{cls.schema}-{cls.taskid}.json')
            return results, 'json'
        else:
            return None, 'empty'
