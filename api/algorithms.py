#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------
# Copyright 2023 for Fosun. All Rights Reserved.
# -----------------------------------------------

import json
import sanic
from datetime import datetime

from project.configuration import Config
from algorithms.middlewares import MinIO, Logger, MessageQueue, Authorization, Registration, protect
from algorithms.algorithms import DataPreprocess, AlgorithmStartup, DataDownload


# ---------------------------------------------------------------------------------------------------------------------------------------------------


algorithms_blueprint = sanic.Blueprint(name='AlgorithmsBlueprint')


@algorithms_blueprint.main_process_start
async def security_listener(app, loop):
    MinIO()
    Registration()


@algorithms_blueprint.options('/platform/health')
async def health_route(request):
    message = {
        "code": 200,
        "information": f"算法 API 接口健康。",
        "timestamp": datetime.now().strftime("%F %T.%f")
    }
    return sanic.response.json(message)


@algorithms_blueprint.get('/security/signature')
async def signature_route(request):
    message, flag = Authorization.sign()
    return sanic.response.json(message)


@algorithms_blueprint.post('/security/verification')
async def verification_route(request):
    message, flag = Authorization.verify(signature=request.token)
    return sanic.response.json(message)


# ---------------------------------------------------------------------------------------------------------------------------------------------------


@algorithms_blueprint.put(f"/api/<algorithm>/data-preprocess")
@protect()
async def data_preprocess_route(request, algorithm):
    body = {
        "algorithm": algorithm,
        "taskid": request.args.get("taskid"),
        **request.json
    }
    message = DataPreprocess(body=body)
    return sanic.response.json(message)


@algorithms_blueprint.post('/api/<algorithm>/algorithm-startup')
@protect()
async def algorithm_startup_route(request, algorithm):
    body = {
        "algorithm": algorithm,
        "taskid": request.args.get("taskid"),
        "version": request.args.get("version"),
    }
    if Config['Information']['Mode'] in ['development']:
        message = AlgorithmStartup(body=body)
    else:
        MessageQueue.produce(queue=Config['RabbitMQ']['AlgorithmQueue'], body=body)
        message = Logger(code=200, taskid="MessageQueueLogs", information=f"算法任务消息已发出。")
    return sanic.response.json(message)


@algorithms_blueprint.get('/api/<algorithm>/data-download')
@protect()
async def data_download_route(request, algorithm):
    body = {
        "algorithm": algorithm,
        "taskid": request.args.get("taskid"),
        "schema": request.args.get("schema"),
    }
    message, types = DataDownload(body=body)
    if types == 'json':
        return sanic.response.json(message)
    elif types == 'csv':
        return await sanic.response.file(message)
    elif types == 'text':
        return sanic.response.text(message)
    else:
        return sanic.response.empty()


# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------


@algorithms_blueprint.post('/mock/callbacks/<callback>')
async def mock_callback_route(request, callback):
    message = {
        "code": 900,
        "taskid": request.json.get("taskid"),
        "information": f"Mock 端口已收到回调 [{callback}] 的信息。",
        "timestamp": datetime.now().strftime("%F %T.%f"),
    }
    print(json.dumps(message, indent=4, ensure_ascii=False))
    return sanic.response.json(message)
