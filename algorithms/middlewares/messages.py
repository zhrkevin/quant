#!/usr/bin/env python
# -*- coding: utf-8 -*-
# -----------------------------------------------
# Copyright 2023 for Fosun. All Rights Reserved.
# -----------------------------------------------

import traceback
from kombu import Connection, Exchange, Queue

from project.configuration import Config


class MessageQueue:

    url = Config['RabbitMQ']['URL']
    mode = Config['Information']['Mode']

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
            print(f"{error}\n{traceback.format_exc()}")

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
            print(f"{error}\n{traceback.format_exc()}")
