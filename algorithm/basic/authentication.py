#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import json
import uuid
import sanic
import traceback
from functools import wraps
from datetime import datetime
from base64 import b64encode, b64decode
from cryptography.hazmat.primitives.hashes import SHA512
from cryptography.hazmat.primitives.asymmetric.ec import generate_private_key, ECDSA, BrainpoolP512R1
from cryptography.hazmat.primitives.serialization import Encoding, PrivateFormat, PublicFormat, NoEncryption, load_pem_private_key, load_pem_public_key

from project import Config


FormatPrivate = PrivateFormat.PKCS8
FormatPublic = PublicFormat.SubjectPublicKeyInfo
Cipher = uuid.uuid8().hex.encode()
SignatureAlgorithm = ECDSA(SHA512())
Curve = BrainpoolP512R1()


class Registration:

    def __new__(cls):
        privatekey = generate_private_key(curve=Curve)
        publickey = privatekey.public_key()
        cls.run(privatekey, publickey)

    @classmethod
    def run(cls, privatekey, publickey):
        private = privatekey.private_bytes(encoding=Encoding.PEM, format=FormatPrivate, encryption_algorithm=NoEncryption())
        public = publickey.public_bytes(encoding=Encoding.PEM, format=FormatPublic)

        keys = {
            'timestamp': datetime.now().strftime("%F %T.%f"),
            'private': private.decode(),
            'public': public.decode(),
        }
        with open(Config['Paths']['DataPath'] / 'system' / 'authentication.key', 'w') as file:
            json.dump(keys, file, indent=4, ensure_ascii=False)


class Authorization:

    operation, signature = None, None
    check, information = None, None
    private, public = None, None

    def __new__(cls, body):
        with open(Config['Paths']['DataPath'] / 'system' / 'authentication.key', 'r') as file:
            keys = json.load(file)
        cls.private = load_pem_private_key(keys['private'].encode(), password=None)
        cls.public = load_pem_public_key(keys['public'].encode())

        cls.operation = body['operation']
        cls.signature = body['signature']

        if cls.operation == 'sign':
            cls.sign()
        elif cls.operation == 'verify':
            cls.verify()
        else:
            cls.check, cls.information = None, f"算法授权，操作错误，请检查 {cls.operation}。"

        print(f"{cls.information} 授权信息: {cls.signature} ")

        message = {'check': cls.check, 'signature': cls.signature, 'information': cls.information}
        return message

    @classmethod
    def sign(cls):
        try:
            if Config['Information']['Security']:
                b64signature = cls.private.sign(Cipher, SignatureAlgorithm)
                cls.signature = b64encode(b64signature).decode()

            cls.check, cls.information = True, f"算法授权，签名通过。"
        except Exception as error:
            cls.check, cls.information = False, f"算法授权，签名失败。\n{error}\n{traceback.format_exc()}"

    @classmethod
    def verify(cls):
        try:
            if Config['Information']['Security']:
                b64signature = b64decode(cls.signature) if cls.signature else 'null'.encode()
                cls.public.verify(b64signature, Cipher, SignatureAlgorithm)

            cls.check, cls.information = True, f"算法授权，验证通过。"
        except Exception as error:
            cls.check, cls.information = False, f"算法授权，验证失败。\n{error}\n{traceback.format_exc()}"


def protect():
    def decorator(function):
        @wraps(function)
        async def wrapper(request, *args, **kwargs):
            body = {'operation': 'verify', 'signature': request.token}
            message = Authorization(body)
            if message['check']:
                response = await function(request, *args, **kwargs)
                return response
            else:
                return sanic.response.json(message)
        return wrapper
    return decorator


if __name__ == '__main__':
    Registration()
    Authorization(body={'operation': 'sign', 'signature': None})
