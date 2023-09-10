#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------
# Copyright 2023 for Fosun. All Rights Reserved.
# -----------------------------------------------

from datetime import datetime
from zoneinfo import ZoneInfo

from algorithms.middlewares.minios import MinIO


class Logger:

    def __new__(cls, code, taskid, information=None, **kwargs):
        timestamp = datetime.now(ZoneInfo('Asia/Shanghai')).strftime('%F %T.%f %z %Z')
        logline = f"[{timestamp}] TaskID @ {taskid} <{code}>: {information}"
        MinIO.write(data=logline, filename=f"system/logs-{taskid}.log", mode='a+')
        print(logline)

        message = {
            'code': code,
            'taskid': taskid,
            'information': information,
            'timestamp': timestamp,
            **kwargs,
        }
        return message


if __name__ == '__main__':
    Logger(code=200, taskid='HaHaHa', information="TestHaHa")
