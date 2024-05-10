#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

from api.algorithms import algorithms_blueprint
from api.schedulers import schedulers_blueprint
from api.website import website_blueprint


__all__ = [
    'algorithms_blueprint',
    'schedulers_blueprint',
    'website_blueprint',
]
