#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------
# Copyright 2023 for Fosun. All Rights Reserved.
# -----------------------------------------------

import os
import json
from apispec import APISpec

from project.configuration import Config
from axon.documents.routes import GenerateContent, NLQSL, Platform


class OpenAPI:

    apispec = APISpec(
        openapi_version='3.1.0',
        title='Project API',
        version='20230701',
        info={'description': 'Project API documents are on [document page](/).'},
        servers=[{'url': '/', 'description': 'Production Server'}]
    )

    def __new__(cls):
        cls.main()
        cls.dump()

    @classmethod
    def main(cls):
        Platform(cls.apispec)
        GenerateContent(cls.apispec)
        NLQSL(cls.apispec)

    @classmethod
    def dump(cls):
        os.system(f"mkdocs build --config-file mkdocs.yml")
        print("Documents 文档生成成功")

        with open(Config['Paths']['AxonPath'] / 'swagger' / 'project-api.json', 'w') as file:
            json.dump(cls.apispec.to_dict(), file, indent=4, ensure_ascii=False)
        print(f"SwaggerUI Yaml 生成成功")


if __name__ == '__main__':
    OpenAPI()
