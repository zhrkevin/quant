#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------


class Platform:

    def __init__(self, openapi):
        openapi.components.security_scheme(
            'Bearer',
            component={'type': 'http', 'scheme': 'bearer'},
        )

        openapi.path(
            path='/platform/health',
            operations={
                'options': {
                    'summary': '健康',
                    'tags': ['Platform'],
                    'responses': {
                        '200': {'description': 'Success'}
                    }
                }
            },
        )

        openapi.path(
            path='/security/signature',
            operations={
                'get': {
                    'summary': '签名',
                    'tags': ['Platform'],
                    'responses': {
                        '200': {'description': 'Success'}
                    }
                }
            },
        )

        openapi.path(
            path='/security/verification',
            operations={
                'post': {
                    'summary': '认证',
                    'tags': ['Platform'],
                    'security': [
                        {'Bearer': []}
                    ],
                    'responses': {
                        '200': {'description': 'Success'}
                    },
                }
            },
        )
