#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------
# Copyright 2023 for Fosun. All Rights Reserved.
# -----------------------------------------------

import json
import httpx
import shortuuid
import traceback

from project.configuration import Config


class NLSQL:

    # FosunGPT API Documents: https://www.yuque.com/liulin-7hzdz/luu3aw/rog74agsgc4bbior

    def __init__(self):
        with open(Config['Paths']['DataPath'] / 'model' / 'fosun-gpt-secrets.key', "r") as file:
            self.secrets = json.load(file)
        self.url = f"https://tc.fosun.com/ai-serv/api/conversation?access_token={self.secrets['AccessToken']}"

    def run(self, taskid, version, metadata, question):
        try:
            system = ""
            for element in metadata:
                system += f"{element['table']}，包含字段：" + "、".join(element['columns']) + "。\n"

            inputs = {
                "sessionid": taskid,
                "v": version,
                "corpus": {
                    "system": system,
                    "message": [],
                },
                "prompt": f"写个SQL查询语句，只要SQL语句不要解释，{question}",
            }

            response = httpx.post(url=self.url, json=inputs, timeout=180).json()
            print(f"{response['status']} <{response['errno']}>: {response['errmsg']}")
            print(f"{response['data']['completion']}")

            if not response['status']:
                raise RuntimeError(f"FosunGPT 请求错误。\n<{response['errno']}>: {response['errmsg']}")

            outputs = {
                "schema": "output/results",
                "code": 200,
                "taskid": taskid,
                "version": version,
                "content": response['data']['completion'],
                "information": "FosunGPT 请求成功。结果已生成。",
            }
        except Exception as error:
            outputs = {
                "schema": "output/results",
                "code": 250,
                "taskid": taskid,
                "version": version,
                "content": "",
                "information": f"{error}\n{traceback.format_exc()}",
            }
        return outputs


if __name__ == '__main__':
    test_version = 4
    test_metadata = [
        {
            "table": "领取核销表CouponVerificationTable",
            "columns": [
                "用户编号CustomerId",
                "优惠券类型CouponType",
            ]
        }
    ]
    test_question = "帮我查一下7天内领过黄金优惠券，但没有核销的用户。"

    nlsql = NLSQL()
    nlsql.run(
        taskid=shortuuid.random(24),
        version=test_version,
        metadata=test_metadata,
        question=test_question
    )
