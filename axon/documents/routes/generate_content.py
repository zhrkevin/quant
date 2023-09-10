#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------
# Copyright 2023 for Fosun. All Rights Reserved.
# -----------------------------------------------

import json

from project.configuration import Config


class GenerateContent:

    def __new__(cls, openapi):
        GenerateContentDataPreprocess(openapi)
        GenerateContentAlgorithmStartup(openapi)
        GenerateContentDataDownload(openapi)


class GenerateContentDataPreprocess:

    def __new__(cls, openapi):
        with open(Config['Paths']['AxonPath'] / 'documents' / 'examples' / 'generate-content' / 'input-texts.json') as file:
            input_texts = json.load(file)

        openapi.path(
            path='/api/generate-content/data-preprocess',
            operations={
                'put': {
                    'tags': ['Generate Content (内容生成)'],
                    'security': [{'BearerAuthentication': []}],
                    'parameters': [
                        {
                            'name': 'taskid',
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


class GenerateContentAlgorithmStartup:

    def __new__(cls, openapi):
        openapi.path(
            path='/api/generate-content/algorithm-startup',
            operations={
                'post': {
                    'tags': ['Generate Content (内容生成)'],
                    'security': [{'BearerAuthentication': []}],
                    'parameters': [
                        {
                            'name': 'taskid',
                            'required': 'true',
                            'in': 'query',
                            'schema': {'type': 'string', 'example': 'LeLe9hhWK6FGMbEFcmwjZXVX'},
                        },
                        {
                            'name': 'version',
                            'required': 'true',
                            'in': 'query',
                            'schema': {'type': 'string', 'enum': ['3.5', '3.5-16k', '4', '4-32k']},
                        },
                    ],
                    'responses': {'200': {'description': 'Success'}}
                }
            },
        )


class GenerateContentDataDownload:

    def __new__(cls, openapi):
        openapi.path(
            path='/api/generate-content/data-download',
            operations={
                'get': {
                    'tags': ['Generate Content (内容生成)'],
                    'security': [{'BearerAuthentication': []}],
                    'parameters': [
                        {
                            'name': 'taskid',
                            'required': 'true',
                            'in': 'query',
                            'schema': {'type': 'string', 'example': 'LeLe9hhWK6FGMbEFcmwjZXVX'},
                        },
                        {
                            'name': 'schema',
                            'required': 'true',
                            'in': 'query',
                            'schema': {'type': 'string', 'enum': ['system/logs', 'input/texts', 'output/results']},
                        },
                    ],
                    'responses': {'200': {'description': 'Success'}}
                }
            },
        )