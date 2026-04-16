#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

from sanic import Sanic

from project import Config
from algorithm import algorithms_blueprint, website_blueprint


Algorithms = Sanic('Algorithms')
Algorithms.blueprint(algorithms_blueprint)
Algorithms.blueprint(website_blueprint)


if __name__ == '__main__':
    Algorithms.run(
        host=Config['Information']['Host'],
        port=Config['Information']['AlgorithmPort'],
    )
