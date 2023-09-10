#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------
# Copyright 2023 for Fosun. All Rights Reserved.
# -----------------------------------------------

import json

from project.configuration import Config


class NLQSL:

    def __new__(cls, openapi):
        NLQSLDataPreprocess(openapi)
        NLQSLAlgorithmStartup(openapi)
        NLQSLDataDownload(openapi)


class NLQSLDataPreprocess:

    def __new__(cls, openapi):
        with open(Config['Paths']['AxonPath'] / 'documents' / 'examples' / 'nl-sql' / 'input-metadata.json') as file:
            input_metadata = json.load(file)
        with open(Config['Paths']['AxonPath'] / 'documents' / 'examples' / 'nl-sql' / 'input-question.json') as file:
            input_question = json.load(file)

        openapi.path(
            path='/api/nl-sql/data-preprocess',
            operations={
                'put': {
                    'tags': ['NLSQL (自然语言转化 SQL)'],
                    'security': [{'BearerAuthentication': []}],
                    'parameters': [
                        {
                            'name': 'taskid',
                            'required': 'true',
                            'in': 'query',
                            'schema': {'type': 'string', 'example': '17929488'}
                        }
                    ],
                    'requestBody': {
                        'required': 'true',
                        'content': {
                            'application/json': {
                                'schema': {'type': 'Any'},
                                'examples': {'input/metadata': {'value': input_metadata}, 'input/question': {'value': input_question}}
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


class NLQSLAlgorithmStartup:

    def __new__(cls, openapi):
        openapi.path(
            path='/api/nl-sql/algorithm-startup',
            operations={
                'post': {
                    'tags': ['NLSQL (自然语言转化 SQL)'],
                    'security': [{'BearerAuthentication': []}],
                    'parameters': [
                        {
                            'name': 'taskid',
                            'required': 'true',
                            'in': 'query',
                            'schema': {'type': 'string', 'example': '17929488'},
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


class NLQSLDataDownload:

    def __new__(cls, openapi):
        openapi.path(
            path='/api/nl-sql/data-download',
            operations={
                'get': {
                    'tags': ['NLSQL (自然语言转化 SQL)'],
                    'security': [{'BearerAuthentication': []}],
                    'parameters': [
                        {
                            'name': 'taskid',
                            'required': 'true',
                            'in': 'query',
                            'schema': {'type': 'string', 'example': '17929488'},
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
