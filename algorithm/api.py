#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import sanic
from sanic.log import logger
from apscheduler.schedulers.background import BackgroundScheduler

from algorithm.basic.authentication import Registration, Authorization, protect
from algorithm.dividend.task import DataTask, AlgorithmTask, MainScheduler


algorithms_blueprint = sanic.Blueprint(name='AlgorithmsBlueprint', url_prefix='/quant')


@algorithms_blueprint.listener('before_server_start')
async def algorithm_listener(app, loop=None):
    Registration()
    logger.info(f"算法授权注册成功，签名可申请或验证。")


@algorithms_blueprint.listener('after_server_start')
async def scheduler_listener(app, loop=None):
    scheduler = BackgroundScheduler(timezone='Asia/Shanghai')
    scheduler.add_job(MainScheduler.run, trigger='cron', day_of_week='1-5', hour=17, minute=10)
    scheduler.start()
    logger.info(f"定时任务加载成功。触发时间为周一至周五 17:10。")


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
    message = {
        'code': 900,
        'information': f"Mock API 收到回调 [{callback}] 的信息。",
        'taskid': request.json.get('taskid'),
    }
    return sanic.response.json(message)
