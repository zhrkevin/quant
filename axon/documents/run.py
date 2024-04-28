#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------
# Copyright 2023 for Fosun. All Rights Reserved.
# -----------------------------------------------

import os


class OpenDocuments:

    def __new__(cls):
        os.system(f"mkdocs build --config-file mkdocs.yml")
        print("Documents 文档生成成功")


if __name__ == '__main__':
    OpenDocuments()
