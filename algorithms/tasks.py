#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

from project.configuration import Config
from algorithms.middlewares import Logger, MessageQueue
from algorithms.core import NLSQL, SSE


class DataTask:

    def __new__(cls, taskid, algorithm, unavailable):
        if unavailable:
            raise FileExistsError(f"以下数据表 [{unavailable}] 中的数据未上传至 MinIO 算法暂未启动")

        if algorithm == 'nl-sql':
            body = {
                'url': Config['Callbacks']['Data']['NLSQL'],
                'information': Logger(code=100, taskid=taskid, information=f"算法任务 [{algorithm}] 的所有数据准备完成。"),
            }
        elif algorithm == 'quant':
            body = {
                'url': Config['Callbacks']['Data']['GenerateContent'],
                'information': Logger(code=100, taskid=taskid, information=f"算法任务 [{algorithm}] 的所有数据准备完成。"),
            }
        else:
            raise TypeError(f"算法任务 [{algorithm}] 类型错误，请检查算法任务名称。")

        MessageQueue.produce(queue=Config['RabbitMQ']['CallbackQueue'], body=body)


class AlgorithmsTask:

    def __new__(cls, taskid, algorithm, version, models, **inputs):
        if algorithm == 'nl-sql':
            model = models[algorithm] if models else NLSQL()
            outputs = model.run(
                taskid,
                version,
                metadata=inputs['input/metadata']['content'],
                question=inputs['input/metadata']['content'],
            )

            body = {
                'url': Config['Callbacks']['Results']['NLSQL'],
                'information': Logger(code=outputs['code'], taskid=outputs['taskid'], information=outputs['information'], sql=outputs['content'])
            }
        elif algorithm == 'quant':
            model = models[algorithm] if models else SSE()
            outputs = model.run()

            body = {
                'url': Config['Callbacks']['Results']['GenerateContent'],
                'information': Logger(code=outputs['code'], taskid=outputs['taskid'], information=outputs['information'])
            }
        else:
            raise TypeError(f"算法任务 [{algorithm}] 类型错误，请检查算法任务名称。")

        MessageQueue.produce(queue=Config['RabbitMQ']['CallbackQueue'], body=body)
        return {"output/results": outputs}
