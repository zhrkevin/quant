#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------
# Copyright 2023 for Fosun. All Rights Reserved.
# -----------------------------------------------

from sanic import Sanic
from sanic.worker.manager import WorkerManager

from api import algorithms_blueprint, schedulers_blueprint, website_blueprint


Sanic.start_method = 'fork'
WorkerManager.THRESHOLD = 600


Algorithms = Sanic('AlgorithmsApplication')
Algorithms.blueprint(algorithms_blueprint)
Algorithms.blueprint(website_blueprint)


Schedulers = Sanic('SchedulersApplication')
Schedulers.blueprint(schedulers_blueprint)
Schedulers.blueprint(website_blueprint)
