#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------
# Copyright 2023 for Fosun. All Rights Reserved.
# -----------------------------------------------

from sanic import Sanic

from project.configuration import Config
from project.applications import Algorithms, Schedulers


if __name__ == '__main__':
    Algorithms.prepare(
        host=Config['Information']['LocalHost'],
        port=Config['Information']['AlgorithmsPort'],
        workers=Config['Information']['Workers'],
    )
    Schedulers.prepare(
        host=Config['Information']['LocalHost'],
        port=Config['Information']['SchedulersPort'],
        workers=Config['Information']['Workers'],
    )
    Sanic.serve()
