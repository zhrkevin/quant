#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

from datetime import date
from project.configuration import Config


class Quant:

    def __init__(self, openapi):
        openapi.path(
            path='/api/dividend-low-volatility-strategy/data-task',
            operations={
                'put': {
                    'summary': '数据处理',
                    'tags': ['Quant'],
                    'security': [{'Bearer': []}],
                    'parameters': [
                        {
                            'name': 'TaskID',
                            'required': 'true',
                            'in': 'query',
                            'schema': {'type': 'string', 'example': 'LeLe9hhWK6FGMbEFcmwjZXVX'}
                        },
                        {
                            'name': 'Callback',
                            'required': 'true',
                            'in': 'query',
                            'schema': {'type': 'string', 'example': Config['Callbacks']['Mock']}
                        },
                    ],
                    'responses': {
                        '200': {
                            'description': 'Success'
                        }
                    }
                }
            },
        )

        openapi.path(
            path='/api/dividend-low-volatility-strategy/algorithm-task',
            operations={
                'post': {
                    'summary': '算法计算',
                    'tags': ['Quant'],
                    'security': [{'Bearer': []}],
                    'parameters': [
                        {
                            'name': 'TaskID',
                            'required': 'true',
                            'in': 'query',
                            'schema': {'type': 'string', 'example': 'LeLe9hhWK6FGMbEFcmwjZXVX'},
                        },
                        {
                            'name': 'Today',
                            'required': 'true',
                            'in': 'query',
                            'schema': {'type': 'string', 'example': date.today().strftime('%Y-%m-%d')},
                        },
                        {
                            'name': 'Callback',
                            'required': 'true',
                            'in': 'query',
                            'schema': {'type': 'string', 'example': Config['Callbacks']['Mock']}
                        },
                    ],
                    'responses': {
                        '200': {
                            'description': 'Success'
                        }
                    }
                }
            },
        )
