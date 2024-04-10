#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------
# Copyright 2023 for Fosun. All Rights Reserved.
# -----------------------------------------------

import httpx
import traceback
from datetime import datetime
from kombu import Connection, Exchange, Queue

from project.configuration import Config
from algorithms.middlewares.logger import Logger


class MessageQueue:

    mode = Config['Information']['Mode']
    endpoint = Config['RabbitMQ']['Endpoint']
    username = Config['RabbitMQ']['Username']
    password = Config['RabbitMQ']['Password']
    url = f'amqp://{username}:{password}@{endpoint}'

    @classmethod
    def produce(cls, queue, body):
        try:
            if cls.mode in ['development']:
                pass
            else:
                # print('produce')
                queues = Queue(
                    name=queue,
                    exchange=Exchange(name=queue, type="topic", durable=True)
                )
                with Connection(hostname=cls.url) as connection:
                    with connection.Producer(exchange=queues.exchange) as producer:
                        producer.publish(body=body, declare=[queues])
        except Exception as error:
            print(f"错误信息：{error}\n{traceback.format_exc()}")
        message = Logger(code=200, taskid='SystemLogs', information=f"算法任务消息已发出。")
        return message

    @classmethod
    def consume(cls, queue, acknowledge):
        try:
            if cls.mode in ['development']:
                pass
            else:
                # print('consume')
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

    def __init__(self, url, body):
        try:
            response = httpx.post(url=url, json=body)
            assert response.is_success, f"<{response.status_code}>: {response.text}"
            print(f"后端服务请求成功。\n{datetime.now().strftime('%F %T.%f')} <{response.status_code}>: {response.json()}")
        except Exception as error:
            print(f"后端服务请求失败。\n{datetime.now().strftime('%F %T.%f')} {error}\n{traceback.format_exc()}")


if __name__ == '__main__':
    pass
