#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------
# Copyright 2023 for Fosun. All Rights Reserved.
# -----------------------------------------------

import json
from apispec import APISpec
from sanic.log import logger
from datetime import datetime

from project.configuration import Config
from algorithms.openapi.algorithm_sse import SSE
from algorithms.openapi.ai_platform import Platform


class OpenAPI:

    apispec = APISpec(
        openapi_version='3.1.0',
        title='Quant API',
        info={'description': 'Project API documents are on [document page](/quant).'},
        servers=[{'url': '/quant', 'description': 'Production'}],
        version=datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
    )

    def __new__(cls):
        Platform(cls.apispec)
        SSE(cls.apispec)

        with open(Config['Paths']['AxonPath'] / 'swagger' / 'project-api.json', 'w') as file:
            json.dump(cls.apispec.to_dict(), file, indent=4, ensure_ascii=False)

        logger.info(f"算法引擎版本: {cls.apispec.version}")


if __name__ == '__main__':
    OpenAPI()
