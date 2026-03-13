#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import subprocess
import json
from apispec import APISpec
from sanic.log import logger

from project.configuration import Config
from docs.documents.openapi.platform import Platform
from docs.documents.openapi.quant import Quant


class OpenAPI:

    def __init__(self):
        self.path = Config['Paths']['DocsPath']
        self.apispec = APISpec(
            openapi_version='3.2.0',
            title='Quant',
            info={'description': 'Project API documents are on [document page](/quant).'},
            servers=[{'url': '/quant', 'description': 'Production'}],
            version=datetime.now().strftime('%F %T'),
        )
        self.api()
        self.docs()

    def api(self):
        Platform(self.apispec)
        Quant(self.apispec)
        with open(Config['Paths']['DocsPath'] / 'swagger' / 'project-api.json', 'w') as file:
            json.dump(self.apispec.to_dict(), file, indent=4, ensure_ascii=False)

    def docs(self):
        subprocess.run(f'zensical build --clean --config-file {self.path}/website/zensical.toml', cwd=self.path, shell=True)

if __name__ == '__main__':
    OpenAPI()
