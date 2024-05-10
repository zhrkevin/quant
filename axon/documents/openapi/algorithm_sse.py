#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import json

from project.configuration import Config


class SSE:

    def __init__(self, openapi):
        with open(Config['Paths']['AxonPath'] / 'documents' / 'examples' / 'quant' / 'input-texts.json', encoding='utf-8') as file:
            input_texts = json.load(file)

        openapi.path(
            path='/api/quant/data-preprocess',
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
                        }
                    ],
                    'requestBody': {
                        'required': 'true',
                        'content': {
                            'application/json': {
                                'schema': {'type': 'Any'},
                                'examples': {'input/texts': {'value': input_texts}}
                            },
                            'application/octet-stream': {
                                'schema': {'type': 'string', 'format': 'binary'}
                            },
                        },
                    },
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
                            'schema': {'type': 'string', 'enum': ['system/logs', 'input/texts', 'output/results']},
                        },
                    ],
                    'responses': {'200': {'description': 'Success'}}
                }
            },
        )
