#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import sanic
from sanic.log import logger
from apscheduler.schedulers.background import BackgroundScheduler

from algorithm.dividend.tasks import MainScheduler


schedulers_blueprint = sanic.Blueprint(name='SchedulersBlueprint', url_prefix='/quant')


@schedulers_blueprint.after_server_start
async def scheduler_listener(app):
    scheduler = BackgroundScheduler(timezone='Asia/Shanghai')
    scheduler.add_job(MainScheduler.run, trigger='cron', day_of_week='0-4', hour=17, minute=10, misfire_grace_time=None)
    scheduler.start()
    logger.info(f"定时任务加载成功。触发时间为周一至周五 17:10。")

