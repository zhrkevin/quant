#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------
# Copyright 2023 for Fosun. All Rights Reserved.
# -----------------------------------------------

import json
import sanic
import traceback
from functools import wraps
from datetime import datetime
from base64 import b64encode, b64decode
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.serialization import load_pem_private_key, load_pem_public_key
from cryptography.hazmat.primitives.serialization import Encoding, PrivateFormat, PublicFormat, NoEncryption

from project.configuration import Config
from algorithms.middlewares import Logger


class Registration:

    PemEncoding = Encoding.PEM
    FormatPrivate = PrivateFormat.PKCS8
    FormatPublic = PublicFormat.SubjectPublicKeyInfo

    def __new__(cls):
        privatekey = ec.generate_private_key(curve=ec.SECP384R1())
        publickey = privatekey.public_key()
        private = privatekey.private_bytes(encoding=cls.PemEncoding, format=cls.FormatPrivate, encryption_algorithm=NoEncryption())
        public = publickey.public_bytes(encoding=cls.PemEncoding, format=cls.FormatPublic)

        keys = {
            'timestamp': datetime.now().strftime("%F %T.%f"),
            'private': private.decode('utf-8'),
            'public': public.decode('utf-8'),
        }
        with open(Config['Paths']['DataPath'] / 'authentication.key', 'w') as file:
            json.dump(keys, file, indent=4, ensure_ascii=False)
        message = Logger(code=200, taskid="SystemLogs", information=f"API 授权已注册成功，可申请签名或进行验证。")
        return message


class Authorization:

    Cipher = 'AKFkgnm6srb9PANEzvc99UdBXKAWHVPNr8FaSQfQp49T2C6MvpKpLEsHMpg2gNVb'.encode('utf-8')
    SignatureAlgorithm = ec.ECDSA(hashes.SHA512())
    Keys, Private, Public = None, None, None

    @classmethod
    def load(cls):
        with open(Config['Paths']['DataPath'] / 'authentication.key', 'r') as file:
            cls.Keys = json.load(file)
        cls.Private = load_pem_private_key(cls.Keys['private'].encode('utf-8'), password=None)
        cls.Public = load_pem_public_key(cls.Keys['public'].encode('utf-8'))

    @classmethod
    def sign(cls):
        try:
            cls.load()
            if not Config['Information']['Security']:
                message = Logger(code=200, taskid="SystemLogs", information=f"API 授权签名申请关闭。")
            else:
                b64signature = cls.Private.sign(cls.Cipher, cls.SignatureAlgorithm)
                signature = b64encode(b64signature).decode('utf-8')
                message = Logger(code=200, taskid="SystemLogs", information=f"API 授权签名申请成功。", signature=signature)
        except Exception as error:
            print(f"{error}\n{traceback.format_exc()}")
            message = Logger(code=400, taskid="SystemLogs", information=f"API 授权签名申请失败。", signature=None)
        return message

    @classmethod
    def verify(cls, signature):
        try:
            cls.load()
            if not Config['Information']['Security']:
                message = Logger(code=200, taskid="SystemLogs", information=f"API 授权签名验证关闭。")
            else:
                b64signature = b64decode(signature) if signature else 'null'.encode('utf-8')
                cls.Public.verify(b64signature, cls.Cipher, cls.SignatureAlgorithm)
                message = Logger(code=200, taskid="SystemLogs", information=f"API 授权签名验证通过。", verification=True)
        except Exception as error:
            print(f"{error}\n{traceback.format_exc()}")
            message = Logger(code=404, taskid="SystemLogs", information=f"API 授权签名验证失败。", verification=False)
        return message


def protect():
    def decorator(function):
        @wraps(function)
        async def wrapper(request, *args, **kwargs):
            message, flag = Authorization.verify(request.token)
            if flag:
                response = await function(request, *args, **kwargs)
                return response
            else:
                message = Logger(code=404, taskid="SystemLogs", information="抱歉，您未被授权使用 API 接口，请联系管理员。")
                return sanic.response.json(message)
        return wrapper
    return decorator
