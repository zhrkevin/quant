#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------
# Copyright 2023 for Fosun. All Rights Reserved.
# -----------------------------------------------

import os
import json
import traceback
from minio import Minio

from project.configuration import Config


class MinIO:

    mode = Config['Information']['Mode']
    client = Minio(
        endpoint=Config['MinIO']['Endpoint'],
        access_key=Config['MinIO']['AccessKey'],
        secret_key=Config['MinIO']['SecretKey'],
    )
    bucket = Config['MinIO']['Bucket']

    def __new__(cls, *args, **kwargs):
        if cls.mode not in ['development'] and not cls.client.bucket_exists(cls.bucket):
            cls.client.make_bucket(cls.bucket)

    @classmethod
    def write(cls, data, filename):
        with open(Config['Paths']['DataPath'] / filename, mode='w') as file:
            json.dump(data, file, indent=4, ensure_ascii=False)

    @classmethod
    def read(cls, filename):
        with open(Config['Paths']['DataPath'] / filename, mode='r') as file:
            data = json.load(file)
        return data

    @classmethod
    def upload(cls, filename):
        try:
            if cls.mode in ['development']:
                return os.path.exists(Config['Paths']['DataPath'] / filename)
            else:
                cls.client.fput_object(
                    bucket_name=cls.bucket,
                    object_name=filename,
                    file_path=Config['Paths']['DataPath'] / filename,
                    content_type='text/plain',
                )
                return True
        except Exception as error:
            print(f"{error}\n{traceback.format_exc()}\n")
            return False

    @classmethod
    def download(cls, filename):
        try:
            if cls.mode in ['development']:
                return os.path.exists(Config['Paths']['DataPath'] / filename)
            else:
                cls.client.fget_object(
                    bucket_name=cls.bucket,
                    object_name=filename,
                    file_path=Config['Paths']['DataPath'] / filename,
                )
                return True
        except Exception as error:
            print(f"{error}\n{traceback.format_exc()}\n")
            return False

    @classmethod
    def exist(cls, filename):
        return os.path.exists(Config['Paths']['DataPath'] / filename)


if __name__ == '__main__':
    MinIO()
