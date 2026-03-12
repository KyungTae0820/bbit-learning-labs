# Copyright 2024 Bloomberg Finance L.P.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import sys

import pika

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from consumer_interface import mqConsumerInterface


class mqConsumer(mqConsumerInterface):
    def __init__(
        self, binding_key: str, exchange_name: str, queue_name: str
    ) -> None:
        self.m_binding_key = binding_key
        self.m_queue_name = queue_name
        self.m_exchange_name = exchange_name
        self.setupRMQConnection()

    def setupRMQConnection(self) -> None:
        con_params = pika.URLParameters(os.environ["AMQP_URL"])
        self.m_connection = pika.BlockingConnection(parameters=con_params)
        self.m_channel = self.m_connection.channel()
        self.m_channel.queue_declare(queue=self.m_queue_name)
        self.m_channel.exchange_declare(
            exchange=self.m_exchange_name, exchange_type="topic"
        )
        self.m_channel.queue_bind(
            queue=self.m_queue_name,
            routing_key=self.m_binding_key,
            exchange=self.m_exchange_name,
        )
        self.m_channel.basic_consume(
            queue=self.m_queue_name,
            on_message_callback=self.on_message_callback,
            auto_ack=False,
        )

    def on_message_callback(
        self, channel, method_frame, header_frame, body
    ) -> None:
        channel.basic_ack(delivery_tag=method_frame.delivery_tag)
        if isinstance(body, bytes):
            body = body.decode("utf-8")
        print(f" [x] Received Message: {body}")

    def startConsuming(self) -> None:
        print(" [*] Waiting for messages. To exit press CTRL+C")
        self.m_channel.start_consuming()

    def __del__(self) -> None:
        print("Closing RMQ connection on destruction")
        if hasattr(self, "m_channel") and self.m_channel and self.m_channel.is_open:
            self.m_channel.close()
        if hasattr(self, "m_connection") and self.m_connection and self.m_connection.is_open:
            self.m_connection.close()
