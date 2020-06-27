import pika
from abc import ABCMeta, abstractmethod

QUEUE_SERVER_HOST = "localhost"
HEARTBEAT_TIMEOUT = 0
TASKS_QUEUE_NAME = "indexer_tasks"


class TasksQueueWrapper(metaclass=ABCMeta):
    @abstractmethod
    def register_consumer_callback(self, func):
        pass

    @abstractmethod
    def publish_task(self, task_body):
        pass

    @abstractmethod
    def start_consumer(self):
        pass

    @abstractmethod
    def stop_consumer(self):
        pass

    @abstractmethod
    def get_queue_size(self):
        pass


class RabbitMQWrapper(TasksQueueWrapper):

    def __init__(self):
        self._conn = pika.BlockingConnection(pika.ConnectionParameters(QUEUE_SERVER_HOST, heartbeat=HEARTBEAT_TIMEOUT))
        self._channel: pika.adapters.blocking_connection.BlockingChannel = self._conn.channel()
        self._declare_queue()
        self._channel.basic_qos(prefetch_count=1)
        self._consume_callback = None

    def _declare_queue(self):
        return self._channel.queue_declare(queue=TASKS_QUEUE_NAME, durable=True)

    def _callback_wrapper(self, channel, method, properties, body):
        self._consume_callback(body)
        channel.basic_ack(delivery_tag=method.delivery_tag)

    def register_consumer_callback(self, func):
        self._consume_callback = func
        self._channel.basic_consume(TASKS_QUEUE_NAME, self._callback_wrapper)

    def publish_task(self, task_body):
        self._channel.basic_publish(exchange="", routing_key=TASKS_QUEUE_NAME, body=task_body,
                                    properties=pika.BasicProperties(delivery_mode=2))

    def start_consumer(self):
        self._channel.start_consuming()

    def stop_consumer(self):
        self._channel.stop_consuming()

    def get_queue_size(self):
        return self._declare_queue().method.message_count

    def __del__(self):
        self.stop_consumer()
        self._conn.close()
