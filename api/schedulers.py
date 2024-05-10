#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import sanic
from apscheduler.schedulers.background import BackgroundScheduler

from project.configuration import Config
from algorithms.middlewares import Logger
from algorithms.schedulers import AlgorithmScheduler, CallbackScheduler, CleanScheduler


schedulers_blueprint = sanic.Blueprint(name='SchedulersBlueprint', url_prefix='/quant')


@schedulers_blueprint.after_server_start
async def models_loader(app):
    if Config['Information']['Mode'] in ['development']:
        Logger(code=200, taskid="SystemLogs", information=f"算法模型无需加载。")
    else:
        app.ctx.models = {}
        Logger(code=200, taskid="SystemLogs", information=f"算法模型全部加载成功。")


@schedulers_blueprint.after_server_start
async def listener(app, loop):
    print(app.ctx.models)
    if Config['Information']['Mode'] in ['development']:
        Logger(code=200, taskid="SystemLogs", information=f"定时任务处于 Development 模式。")
    else:
        Logger(code=200, taskid="SystemLogs", information=f"定时任务处于 Production 模式。")
        scheduler = BackgroundScheduler(timezone='Asia/Shanghai')
        scheduler.add_job(
            AlgorithmScheduler,
            kwargs={
                'queue': Config['RabbitMQ']['AlgorithmQueue'],
            },
            trigger='interval',
            seconds=3
        )
        scheduler.add_job(
            CallbackScheduler,
            kwargs={
                'queue': Config['RabbitMQ']['CallbackQueue'],
            },
            trigger='interval',
            seconds=3
        )
        scheduler.add_job(
            CleanScheduler,
            trigger='cron',
            hour=23,
            minute=45
        )
        scheduler.start()
