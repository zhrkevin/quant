#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------------------
# Copyright 2024 for Jingzhi & Level. All Rights Reserved.
# ---------------------------------------------------------

import httpx
import traceback
from sanic.log import logger
from kombu import Connection, Exchange, Queue

from project import Config


class MessageQueue:

    mode = Config['Information']['Mode']
    endpoint = Config['RabbitMQ']['Endpoint']
    username = Config['RabbitMQ']['Username']
    password = Config['RabbitMQ']['Password']
    url = f'amqp://{username}:{password}@{endpoint}'

    @classmethod
    def produce(cls, queue, body):
        try:
            if cls.mode not in ['test']:
                queues = Queue(
                    name=queue,
                    exchange=Exchange(name=queue, type="topic", durable=True)
                )
                with Connection(hostname=cls.url) as connection:
                    with connection.Producer(exchange=queues.exchange) as producer:
                        producer.publish(body=body, declare=[queues])
        except Exception as error:
            logger.error(f"错误信息: {error}\n{traceback.format_exc()}")

    @classmethod
    def consume(cls, queue, acknowledge):
        try:
            if cls.mode not in ['test']:
                queues = Queue(
                    name=queue,
                    exchange=Exchange(name=queue, type="topic", durable=True)
                )
                with Connection(hostname=cls.url) as connection:
                    with connection.Consumer(queues=[queues], prefetch_count=1) as consumer:
                        consumer.register_callback(acknowledge)
                        consumer.consume()
        except Exception as error:
            logger.error(f"错误信息: {error}\n{traceback.format_exc()}")


class Callback:

    # 本机 Test 模式调试时，注意关闭本机的代理 vpn

    def __init__(self, url, message):
        try:
            if Config['Information']['Mode'] in ['test']:
                response = httpx.post(url=url, json=message, timeout=4)
                assert response.is_success, response.json()
                logger.info(f"回调后端服务，请求成功 <Response {response.status_code}>: {response.json()}")
            else:
                MessageQueue.produce(
                    queue=Config['RabbitMQ']['CallbackQueue'],
                    body={'url': url, 'message': message, 'connection': 1}
                )
        except Exception as error:
            logger.error(f"回调后端服务，请求失败 <Response 500>: {error}")


if __name__ == '__main__':
    pass
