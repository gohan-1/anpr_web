"""Sends one message to Kafka and reads it back, from your laptop."""
import json
import os
import time

from confluent_kafka import Consumer, Producer

SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
TOPIC = "anpr.folders"

# --- send -------------------------------------------------------------------
producer = Producer({"bootstrap.servers": SERVERS})
message = {"folder": f"laptop_test_{int(time.time())}", "accuracy": "low"}
producer.produce(TOPIC, key=message["folder"], value=json.dumps(message))
producer.flush(10)                       # wait until Kafka confirms it has the message
print("sent:", message)

# --- receive ----------------------------------------------------------------
consumer = Consumer({
    "bootstrap.servers": SERVERS,
    "group.id": "laptop-check",          # a consumer group just for this test
    "auto.offset.reset": "earliest",     # a new group starts from the oldest message
})
consumer.subscribe([TOPIC])
deadline = time.time() + 15
while time.time() < deadline:
    msg = consumer.poll(1.0)             # wait up to 1 s for a message
    if msg is None or msg.error():
        continue
    data = json.loads(msg.value())
    print(f"received from partition {msg.partition()}: {data}")
    if data == message:
        print("OK: your laptop can send to and receive from Kafka")
        break
consumer.close()