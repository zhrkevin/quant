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


class GenerateContent:

    # FosunGPT API Documents: https://www.yuque.com/liulin-7hzdz/luu3aw/rog74agsgc4bbior

    def __init__(self):
        with open(Config['Paths']['DataPath'] / 'model' / 'fosun-gpt-secrets.key', "r") as file:
            self.secrets = json.load(file)
        self.url = f"https://tc.fosun.com/ai-serv/api/conversation?access_token={self.secrets['AccessToken']}"

    def run(self, taskid, version, system, message, prompt):
        try:
            inputs = {
                "sessionid": taskid,
                "v": version,
                "corpus": {
                    "system": system,
                    "message": message,
                },
                "prompt": prompt,
            }

            response = httpx.post(url=self.url, json=inputs, timeout=180).json()

            print(f"{response['status']} <{response['errno']}>: {response['errmsg']}")
            if not response['status']:
                raise RuntimeError(f"FosunGPT 请求错误。\n<{response['errno']}>: {response['errmsg']}")

            print(response['data']['completion'])
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
    test_version = 3.5
    test_system = "你的角色为业务数据分析专家。"
    test_message = [
        {
            "user": "目前松鹤楼面馆的月平均销量同比增长3%。月平均销售额同比增长5%。",
            "assistant": "这是一个积极的趋势，显示出松鹤楼面馆的产品受到了更多顾客的欢迎和认可。这显示出他们的产品在市场上受到了更多消费者的认可和喜爱。"
        },
        {
            "user": "目前松鹤楼面馆的主力菜品月平均销量同比下降2%。月平均销售额同比下降4%。",
            "assistant": "主力菜品可能面临市场竞争的挑战。其他竞争对手可能提供了更具吸引力的主力菜品或更具竞争力的价格策略，导致顾客转向其他选项。"
        }
    ]
    test_prompt = "请结合前面的背景，分析一下销量情况。并给出未来展望。"

    generate_content = GenerateContent()
    test_output = generate_content.run(
        taskid=shortuuid.random(24),
        version=test_version,
        system=test_system,
        message=test_message,
        prompt=test_prompt,
    )
