#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------
# Copyright 2023 for Fosun. All Rights Reserved.
# -----------------------------------------------

import json
from datetime import datetime

from project.configuration import Config
from algorithms.middlewares.minios import MinIO


class Logger:

    def __new__(cls, code, taskid, information, mode='a+', **kwargs):
        message = {
            'code': code,
            'taskid': taskid,
            'information': information,
            'timestamp': datetime.now().strftime('%F %T.%f'),
            **kwargs,
        }
        line = json.dumps(message, ensure_ascii=False)

        with open(Config['Paths']['DataPath'] / 'system' / f'logs-{taskid}.log', mode=mode) as file:
            file.write(line + '\n')
            print(line)

        MinIO.upload(filename=f'system/logs-{taskid}.log')
        return message


if __name__ == '__main__':
    Logger(code=200, taskid='SystemLogs', information='Testing 测试。')
