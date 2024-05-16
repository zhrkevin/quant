#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

from sanic import Sanic

from project import Config, Algorithms, Schedulers


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
