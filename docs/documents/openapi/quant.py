#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------


from project.configuration import Config


class Quant:

    def __init__(self, openapi):
        openapi.path(
            path='/api/quant/data-task',
            operations={
                'post': {
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
                        }
                    ],
                    'responses': {'200': {'description': 'Success'}}
                }
            },
        )

        openapi.path(
            path='/api/quant/algorithm-startup',
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
                            'name': 'Version',
                            'required': 'true',
                            'in': 'query',
                            'schema': {'type': 'string', 'enum': ['3.5', '3.5-16k', '4', '4-32k']},
                        },
                    ],
                    'responses': {'200': {'description': 'Success'}}
                }
            },
        )

        openapi.path(
            path='/api/quant/data-download',
            operations={
                'get': {
                    'summary': '文件查询',
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
                            'name': 'Schema',
                            'required': 'true',
                            'in': 'query',
                            'schema': {'type': 'string', 'enum': ['system/logs', 'stocks/texts', 'output/results']},
                        },
                    ],
                    'responses': {'200': {'description': 'Success'}}
                }
            },
        )
