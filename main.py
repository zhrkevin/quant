#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

from sanic import Sanic

from project.configuration import Config
from api import algorithms_blueprint, schedulers_blueprint, website_blueprint


Algorithms = Sanic('Algorithms')
Algorithms.blueprint(algorithms_blueprint)
Algorithms.blueprint(website_blueprint)


Schedulers = Sanic('Schedulers')
Schedulers.blueprint(schedulers_blueprint)
Schedulers.blueprint(website_blueprint)


if __name__ == '__main__':
    Algorithms.prepare(
        host=Config['Information']['Host'],
        port=Config['Information']['AlgorithmPort'],
    )
    Schedulers.prepare(
        host=Config['Information']['Host'],
        port=Config['Information']['SchedulerPort'],
    )
    Sanic.serve()
