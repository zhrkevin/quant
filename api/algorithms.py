#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import sanic
from datetime import datetime

from axon.documents import OpenAPI
from algorithms.middlewares import Authorization, Registration, protect
from algorithms.algorithms import DataPreprocess, AlgorithmStartup, DataDownload


# ---------------------------------------------------------------------------------------------------------------------------------------------------


algorithms_blueprint = sanic.Blueprint(name='AlgorithmsBlueprint', url_prefix='/quant')


@algorithms_blueprint.main_process_start
async def security_listener(app, loop):
    OpenAPI()
    Registration()


@algorithms_blueprint.options('/platform/health')
async def health_route(request):
    message = {
        'code': 200,
        'information': '算法 API 接口健康。',
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
        'algorithm': algorithm,
        'taskid': request.args.get('taskid'),
        **request.json
    }
    message = DataPreprocess(body=body)
    return sanic.response.json(message)


@algorithms_blueprint.post('/api/<algorithm>/algorithm-startup')
@protect()
async def algorithm_startup_route(request, algorithm):
    body = {
        'algorithm': algorithm,
        'taskid': request.args.get('taskid'),
        'version': request.args.get('version'),
    }
    message = AlgorithmStartup(body=body)
    return sanic.response.json(message)


@algorithms_blueprint.get('/api/<algorithm>/data-download')
@protect()
async def data_download_route(request, algorithm):
    body = {
        'algorithm': algorithm,
        'taskid': request.args.get('taskid'),
        'schema': request.args.get('schema'),
    }
    filepath = DataDownload(body=body)
    return await sanic.response.file(filepath)


# -----------------------------------------------------------------------------------------------------------------------------------------------------------------------


@algorithms_blueprint.post('/mock/callbacks/<callback>')
async def mock_callback_route(request, callback):
    message = {
        'timestamp': datetime.now().strftime('%F %T.%f'),
        'code': 900,
        'taskid': request.json.get('taskid'),
        'information': f"Mock 端口已收到回调 [{callback}] 的信息。",
    }
    return sanic.response.json(message)
