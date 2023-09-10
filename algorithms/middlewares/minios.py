#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------
# Copyright 2023 for Fosun. All Rights Reserved.
# -----------------------------------------------

import json
import traceback
from minio import Minio

from project.configuration import Config


class MinIO:

    client = Minio(
        endpoint=Config['MinIO']['Endpoint'],
        # endpoint="47.101.176.221:9089",
        access_key=Config['MinIO']['AccessKey'],
        secret_key=Config['MinIO']['SecretKey'],
        secure=False
    )
    bucket = Config['MinIO']['Bucket']
    mode = Config['Information']['Mode']

    def __new__(cls, *args, **kwargs):
        cls.write(data="\t", filename=f"system/logs-SystemLogs.log", mode='w')
        if cls.mode not in ['development']:
            if not cls.client.bucket_exists(cls.bucket):
                cls.client.make_bucket(cls.bucket)

    @classmethod
    def write(cls, data, filename, mode='w'):
        try:
            with open(Config['Paths']['DataPath'] / filename, mode=mode) as file:
                if 'json' in filename:
                    json.dump(data, file, indent=4, ensure_ascii=False)
                else:
                    file.write(data + '\n')
            if cls.mode not in ['development']:
                cls.client.fput_object(
                    bucket_name=cls.bucket,
                    object_name=filename,
                    file_path=Config['Paths']['DataPath'] / filename,
                )
            return data
        except Exception as error:
            print(f"{error}\n{traceback.format_exc()}\n")

    @classmethod
    def read(cls, filename, mode='r'):
        try:
            if cls.mode not in ['development']:
                cls.client.fget_object(
                    bucket_name=cls.bucket,
                    object_name=filename,
                    file_path=Config['Paths']['DataPath'] / filename,
                )
            with open(Config['Paths']['DataPath'] / filename, mode=mode) as file:
                if 'json' in filename:
                    data = json.load(file)
                else:
                    data = file.read() + '\n'
            return data
        except Exception as error:
            print(f"{error}\n{traceback.format_exc()}\n")

    @classmethod
    def check(cls, filename):
        try:
            if cls.mode not in ['development']:
                cls.client.fget_object(
                    bucket_name=cls.bucket,
                    object_name=filename,
                    file_path=Config['Paths']['DataPath'] / filename
                )
            return True
        except Exception:
            return False
