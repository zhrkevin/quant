#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

from sanic import Sanic

from api import algorithms_blueprint, schedulers_blueprint, website_blueprint


Algorithms = Sanic('AlgorithmsApplication')
Algorithms.blueprint(algorithms_blueprint)
Algorithms.blueprint(website_blueprint)


Schedulers = Sanic('SchedulersApplication')
Schedulers.blueprint(schedulers_blueprint)
Schedulers.blueprint(website_blueprint)
