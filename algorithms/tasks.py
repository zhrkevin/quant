#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------
# Copyright 2023 for Fosun. All Rights Reserved.
# -----------------------------------------------

from project.configuration import Config
from algorithms.middlewares import Logger, MessageQueue
from algorithms.core import NLSQL, GenerateContent


class DataTask:

    def __new__(cls, taskid, algorithm, unavailable):
        if unavailable:
            raise FileExistsError(f"以下数据表 [{unavailable}] 中的数据未上传至 MinIO 算法暂未启动")

        if algorithm == 'nl-sql':
            body = {
                'url': Config['Callbacks']['Data']['NLSQL'],
                'information': Logger(code=100, taskid=taskid, information=f"算法任务 [{algorithm}] 的所有数据准备完成。"),
            }
        elif algorithm == 'generate-content':
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
        elif algorithm == 'generate-content':
            model = models[algorithm] if models else GenerateContent()
            outputs = model.run(
                taskid,
                version,
                system=inputs['input/texts']['system'],
                message=inputs['input/texts']['message'],
                prompt=inputs['input/texts']['prompt'],
            )

            body = {
                'url': Config['Callbacks']['Results']['GenerateContent'],
                'information': Logger(code=outputs['code'], taskid=outputs['taskid'], information=outputs['information'])
            }
        else:
            raise TypeError(f"算法任务 [{algorithm}] 类型错误，请检查算法任务名称。")

        MessageQueue.produce(queue=Config['RabbitMQ']['CallbackQueue'], body=body)
        return {"output/results": outputs}
