#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------
# Copyright 2023 for Fosun. All Rights Reserved.
# -----------------------------------------------

import httpx
import traceback


class Callback:

    def __new__(cls, url, information):
        try:
            response = httpx.post(url=url, json=information)
            assert response.is_success, f"后端服务请求失败。\n<{response.status_code}>: {response.text}"
            print(f"后端服务请求成功。\n<{response.status_code}>: {response.json()}")
        except Exception as error:
            print(f"{error}\n{traceback.format_exc()}")
