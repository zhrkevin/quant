#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import os
import json
from apispec import APISpec
from sanic.log import logger

from project.configuration import Config
from axon.documents.openapi.ai_platform import Platform
from axon.documents.openapi.algorithm_sse import SSE


class OpenAPI:

    def __init__(self):
        self.version = Config['Information']['Version']
        self.path = Config['Paths']['AxonPath'] / 'documents'
        self.apispec = APISpec(
            openapi_version='3.1.0',
            title='Non Standard Advanced Planning and Scheduling API',
            info={'description': 'Project API documents are on [document page](/quant).'},
            servers=[{'url': '/quant', 'description': 'Production'}],
            version=self.version,
        )

        self.docs()
        self.api()

    def docs(self):
        with open(Config['Paths']['AxonPath'] / 'documents' / 'version', 'w') as file:
            file.write(f"算法引擎版本: {self.version} \n")
            file.write(f"MinIO 版本: 2024-03-30 \n")
            file.write(f"RabbitMQ 版本: 3.13.0 \n")

        os.system(f"cd {self.path} && mkdocs build --config-file mkdocs.yml")

    def api(self):
        Platform(self.apispec)
        SSE(self.apispec)

        with open(Config['Paths']['AxonPath'] / 'swagger' / 'project-api.json', 'w') as file:
            json.dump(self.apispec.to_dict(), file, indent=4, ensure_ascii=False)

        logger.info(f"算法引擎版本: {self.version}")


if __name__ == '__main__':
    OpenAPI()
