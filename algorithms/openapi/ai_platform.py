#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------
# Copyright 2023 for Fosun. All Rights Reserved.
# -----------------------------------------------


class Platform:

    def __new__(cls, openapi):
        openapi.components.security_scheme(
            'BearerAuthentication',
            component={'type': 'http', 'schema': 'bearer'},
        )
        openapi.path(
            path='/platform/health',
            operations={
                'options': {
                    'tags': ['Platform'],
                    'responses': {'200': {'description': 'Success'}}
                }
            },
        )
        openapi.path(
            path='/security/signature',
            operations={
                'get': {
                    'tags': ['Platform'],
                    'responses': {'200': {'description': 'Success'}}
                }
            },
        )
        openapi.path(
            path='/security/verification',
            operations={
                'post': {
                    'tags': ['Platform'],
                    'security': [{'BearerAuthentication': []}],
                    'responses': {'200': {'description': 'Success'}},
                }
            },
        )
