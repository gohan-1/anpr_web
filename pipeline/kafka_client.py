"""
One way for the whole project to talk to Kafka.

    producer = make_producer()                  send messages
    send(producer, topic, key, data)
    consumer = make_consumer(group, [topics])   receive messages
    msg, data = receive(consumer)
    consumer.commit(msg)                        "I'm done with this one" (only after saving!)
    ensure_topics([...])                        create topics that don't exist yet

Where Kafka is comes from KAFKA_BOOTSTRAP_SERVERS:
    localhost:9094  on your laptop (envs/.env.local)
    kafka:9092      inside Docker (envs/.env.docker)
"""
from __future__ import annotations

import json
import os
import socket

from confluent_kafka import Consumer, KafkaException, Message, Producer
from confluent_kafka.admin import AdminClient, NewTopic


def servers() -> str:
    return os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")


# ---------------------------------------------------------------- sending
def make_producer() -> Producer:
    return Producer({
        "bootstrap.servers": servers(),
        "client.id": socket.gethostname(),
        "acks": "all",                  # Kafka confirms only once the message is safely stored
        "enable.idempotence": True,     # a retried send is never stored twice
    })


def send(producer: Producer, topic: str, key: str, data: dict) -> None:
    """Queues one message. The key decides the partition: same key, same partition, same bot."""
    producer.produce(topic, key=str(key).encode(), value=json.dumps(data).encode())
    producer.poll(0)                    # lets the library report finished sends in the background


def flush(producer: Producer, timeout: float = 10) -> None:
    """Waits until every queued message is confirmed. Raises if some weren't."""
    left = producer.flush(timeout)
    if left:
        raise KafkaException(f"{left} message(s) not confirmed by Kafka at {servers()} within {timeout} s")


# ---------------------------------------------------------------- receiving
def make_consumer(group: str, topics: list[str]) -> Consumer:
    """A consumer in `group`. Every consumer with the same group shares the topics' partitions."""
    consumer = Consumer({
        "bootstrap.servers": servers(),
        "group.id": group,
        "client.id": socket.gethostname(),
        "enable.auto.commit": False,     # WE say when a message is done: only after saving it
        "auto.offset.reset": "earliest", # a brand-new group starts from the oldest message
        "max.poll.interval.ms": 600_000, # a bot may spend up to 10 min on one batch before Kafka
                                         # decides it's dead and gives its partitions to another
    })
    consumer.subscribe(topics)
    return consumer


def receive(consumer: Consumer, timeout: float = 1.0) -> tuple[Message, dict] | None:
    """The next message and its data, or None if nothing arrived within `timeout` seconds."""
    msg = consumer.poll(timeout)
    if msg is None:
        return None
    if msg.error():
        raise KafkaException(msg.error())
    return msg, json.loads(msg.value())


# ---------------------------------------------------------------- topics
def ensure_topics(names: list[str], partitions: int = 6) -> list[str]:
    """Creates the topics that don't exist yet. Returns the names it created."""
    admin = AdminClient({"bootstrap.servers": servers()})
    existing = set(admin.list_topics(timeout=10).topics)
    missing = [n for n in names if n not in existing]
    if missing:
        futures = admin.create_topics([NewTopic(n, num_partitions=partitions, replication_factor=1)
                                       for n in missing])
        for name, future in futures.items():
            future.result()             # raises if Kafka refused to create it
    return missing