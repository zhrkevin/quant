#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

from project.configuration import Config
from algorithms.middlewares import Logger, MessageQueue
from algorithms.core import ShanghaiStockExchange


class DataTask:

    def __new__(cls, taskid, algorithm, unavailable):
        if unavailable:
            raise FileExistsError(f"以下数据表 [{unavailable}] 中的数据未上传至 MinIO 算法暂未启动")

        if algorithm == 'quant':
            body = {
                'url': Config['Callbacks']['Data']['GenerateContent'],
                'information': Logger(code=100, taskid=taskid, information=f"算法任务 [{algorithm}] 的所有数据准备完成。"),
            }
        else:
            raise TypeError(f"算法任务 [{algorithm}] 类型错误，请检查算法任务名称。")

        MessageQueue.produce(queue=Config['RabbitMQ']['CallbackQueue'], body=body)


class AlgorithmsTask:

    def __new__(cls, taskid, algorithm, models):
        if algorithm == 'quant':
            model = models[algorithm] if models else ShanghaiStockExchange()
            results = model.run()
            message = Logger(code=500, taskid=taskid, information='算法任务完成')
        else:
            results = None
            message = Logger(code=500, taskid=taskid, information=f"算法任务 [{algorithm}] 类型错误，请检查算法任务名称。")
        return results, message


if __name__ == '__main__':
    import shortuuid
    AlgorithmsTask(
        taskid=shortuuid.random(24),
        algorithm='quant',
        models=None
    )
