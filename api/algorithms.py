#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import sanic

from docs.documents import OpenAPI
from algorithm.middleware import Logger, Registration, Authorization, protect
from algorithm.tasks import DataTask, AlgorithmTask


algorithms_blueprint = sanic.Blueprint(name='AlgorithmsBlueprint', url_prefix='/quant')


@algorithms_blueprint.before_server_start
async def algorithm_listener(app):
    OpenAPI()
    Registration()


@algorithms_blueprint.options('/platform/health')
async def health_route(request):
    message = {'code': 200, 'information': '算法平台 API 接口健康'}
    return sanic.response.json(message)


@algorithms_blueprint.post('/platform/security')
async def signature_route(request):
    body = {
        'operation': request.args.get('Operation'),
        'signature': request.token,
    }
    message = Authorization(body)
    return sanic.response.json(message)


@algorithms_blueprint.put('/api/<algorithm>/data-task')
@protect()
async def data_task_route(request, algorithm):
    body = {
        'algorithm': algorithm,
        'taskid': request.args.get('TaskID'),
        'callback': request.args.get('Callback'),
    }
    message = await DataTask.run(body=body)
    return sanic.response.json(message)


@algorithms_blueprint.post('/api/<algorithm>/algorithm-task')
@protect()
async def algorithm_task_route(request, algorithm):
    body = {
        'algorithm': algorithm,
        'taskid': request.args.get('TaskID'),
        'callback': request.args.get('Callback'),
        'today': request.args.get('Today'),
    }
    message = await AlgorithmTask.run(body=body)
    return sanic.response.json(message)


@algorithms_blueprint.post('/callback/<callback>')
async def mock_callback_route(request, callback):
    message = Logger(
        code=900,
        taskid=request.json.get('taskid'),
        information=f"Mock API 收到回调 [{callback}] 的信息。"
    )
    return sanic.response.json(message)
