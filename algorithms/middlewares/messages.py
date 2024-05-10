#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ---------------------------------------------
# Copyright 2015 for Zen. All Rights Reserved.
# ---------------------------------------------

import httpx
import traceback
from kombu import Connection, Exchange, Queue

from project.configuration import Config


class MessageQueue:

    mode = Config['Information']['Mode']
    endpoint = Config['RabbitMQ']['Endpoint']
    username = Config['RabbitMQ']['Username']
    password = Config['RabbitMQ']['Password']
    url = f'amqp://{username}:{password}@{endpoint}'

    @classmethod
    def produce(cls, queue, body):
        try:
            if cls.mode not in ['development']:
                queues = Queue(
                    name=queue,
                    exchange=Exchange(name=queue, type="topic", durable=True)
                )
                with Connection(hostname=cls.url) as connection:
                    with connection.Producer(exchange=queues.exchange) as producer:
                        producer.publish(body=body, declare=[queues])
        except Exception as error:
            print(f"错误信息：{error}\n{traceback.format_exc()}")

    @classmethod
    def consume(cls, queue, acknowledge):
        try:
            if cls.mode not in ['development']:
                queues = Queue(
                    name=queue,
                    exchange=Exchange(name=queue, type="topic", durable=True)
                )
                with Connection(hostname=cls.url) as connection:
                    with connection.Consumer(queues=[queues], prefetch_count=1) as consumer:
                        consumer.register_callback(acknowledge)
                        consumer.consume()
        except Exception as error:
            print(f"错误信息：{error}\n{traceback.format_exc()}")


class Callback:

    # 本机 Development 模式调试时，注意关闭本机的代理 vpn

    def __new__(cls, url, message):
        try:
            response = httpx.post(url=url, json=message, timeout=4)
            assert response.is_success, f"<{response.status_code}>: {response.json()}"
            print(f"回调后端服务请求成功。<{response.status_code}>: {response.json()}")
        except Exception as error:
            print(f"回调后端服务请求失败。<500>: {error}")


if __name__ == '__main__':
    pass
