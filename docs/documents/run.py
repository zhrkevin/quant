#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import json
import subprocess
from apispec import APISpec
from sanic.log import logger
from datetime import datetime

from project.configuration import Config
from docs.documents.openapi.platform import Platform
from docs.documents.openapi.quant import Quant


class OpenAPI:

    def __init__(self):
        self.version = datetime.now().strftime('%F %T')
        self.apispec = APISpec(
            openapi_version='3.2.0',
            title='Quant',
            info={'description': 'Project API documents are on [document page](/quant).'},
            servers=[{'url': '/quant', 'description': 'Production'}],
            version=self.version,
        )

        self.docs()
        self.api()

    def docs(self):
        subprocess.run('mkdocs build --config-file mkdocs.yml', cwd=Config['Paths']['DocsPath'] / 'documents', shell=True)

    def api(self):
        Platform(self.apispec)
        Quant(self.apispec)
        with open(Config['Paths']['DocsPath'] / 'swagger' / 'project-api.json', 'w') as file:
            json.dump(self.apispec.to_dict(), file, indent=4, ensure_ascii=False)
        logger.info(f"算法版本 {self.version}。")


if __name__ == '__main__':
    OpenAPI()
