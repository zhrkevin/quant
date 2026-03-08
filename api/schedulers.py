#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import sanic
from sanic.log import logger
from apscheduler.schedulers.background import BackgroundScheduler

from project.configuration import Config
from algorithm.schedulers import CallbackScheduler, CleanScheduler


schedulers_blueprint = sanic.Blueprint(name='SchedulersBlueprint', url_prefix='/quant')


@schedulers_blueprint.after_server_start
async def models_loader(app):
    app.ctx.models = {}
    logger.info(f"算法模型，加载成功。")


@schedulers_blueprint.after_server_start
async def scheduler_listener(app):
    logger.info(f"算法引擎，启动模式 {Config['Information']['Mode']}。")
    if Config['Information']['Mode'] not in ['development']:
        scheduler = BackgroundScheduler(timezone='Asia/Shanghai')
        scheduler.add_job(CallbackScheduler, trigger='interval', seconds=5)
        scheduler.add_job(CleanScheduler, trigger='cron', day=7, hour=23, minute=45)
        scheduler.start()
