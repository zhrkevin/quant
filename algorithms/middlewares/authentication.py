#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import json
import sanic
import traceback
from functools import wraps
from sanic.log import logger
from datetime import datetime
from base64 import b64encode, b64decode
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.serialization import load_pem_private_key, load_pem_public_key
from cryptography.hazmat.primitives.serialization import Encoding, PrivateFormat, PublicFormat, NoEncryption

from project.configuration import Config


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
            'private': private.decode(),
            'public': public.decode(),
        }
        with open(Config['Paths']['DataPath'] / 'authentication.key', 'w') as file:
            json.dump(keys, file, indent=4, ensure_ascii=False)

        logger.info(f"算法 API Authorization 授权，注册成功，可申请签名或进行验证。")


class Authorization:

    Cipher = 'AKFkgnm6srb9PANEzvc99UdBXKAWHVPNr8FaSQfQp49T2C6MvpKpLEsHMpg2gNVb'.encode()
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
            if Config['Information']['Security']:
                b64signature = cls.Private.sign(cls.Cipher, cls.SignatureAlgorithm)
                signature = b64encode(b64signature).decode()
            else:
                signature = True
            code, flag, message = 200, signature, f"算法 API Authorization 授权，签名通过。"
        except Exception as error:
            code, flag, message = 400, None, f"算法 API Authorization 授权，签名失败。\n{error}\n{traceback.format_exc()}"
        logger.info(message)
        return code, flag

    @classmethod
    def verify(cls, signature):
        try:
            cls.load()
            if Config['Information']['Security']:
                b64signature = b64decode(signature) if signature else 'null'.encode()
                cls.Public.verify(b64signature, cls.Cipher, cls.SignatureAlgorithm)
            else:
                pass
            code, flag, message = 200, True, f"算法 API Authorization 授权，验证通过。"
        except Exception as error:
            code, flag, message = 200, False, f"算法 API Authorization 授权，验证失败。\n{error}\n{traceback.format_exc()}"
        logger.info(message)
        return code, flag


def protect():
    def decorator(function):
        @wraps(function)
        async def wrapper(request, *args, **kwargs):
            code, flag = Authorization.verify(request.token)
            if flag:
                response = await function(request, *args, **kwargs)
                return response
            else:
                message = f"抱歉，您未被授权使用算法 API Authorization 授权，请联系管理员。"
                logger.error(message)
                return sanic.response.json(message)
        return wrapper
    return decorator
