#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------------------
# Copyright 2024 for Jingzhi & Level. All Rights Reserved.
# ---------------------------------------------------------

import json
import traceback
from minio import Minio
from datetime import datetime

from project.configuration import Config


class Logger:

    mode = Config['Information']['Mode']
    client = Minio(
        endpoint=Config['MinIO']['Endpoint'],
        access_key=Config['MinIO']['AccessKey'],
        secret_key=Config['MinIO']['SecretKey'],
    )
    bucket = Config['MinIO']['Bucket']

    if mode not in ['development'] and not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    def __new__(cls, code, taskid, information, mode='a+', **kwargs):
        try:
            message = {
                'timestamp': datetime.now().strftime('%F %T.%f'),
                'code': code,
                'taskid': taskid,
                'information': information,
                **kwargs,
            }
            with open(Config['Paths']['DataPath'] / 'system' / f'logs-{taskid}.log', mode=mode) as file:
                file.write(json.dumps(message, ensure_ascii=False) + '\n')

            if cls.mode not in ['development']:
                cls.client.fput_object(
                    bucket_name=cls.bucket,
                    object_name=f"system/logs-{taskid}.log",
                    file_path=Config['Paths']['DataPath'] / 'system' / f'logs-{taskid}.log',
                    content_type='text/plain',
                )
        except Exception as error:
            message = {
                'timestamp': datetime.now().strftime('%F %T.%f'),
                'code': 500,
                'taskid': taskid,
                'information': f"错误信息: {error}\n{traceback.format_exc()}",
            }
        print(message['information'])
        return message


if __name__ == '__main__':
    Logger(code=200, taskid='Test', information='Testing 测试。')
