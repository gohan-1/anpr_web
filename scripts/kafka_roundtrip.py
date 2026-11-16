"""
Checks that this machine can talk to Kafka:  make kafka-check
(or: python scripts/kafka_roundtrip.py)

Sends one message to a TEST topic (anpr.selftest, never the real folders topic, so no
reader bot ever sees it) and reads it back with pipeline/kafka_client.py, the same code
the reader bots use. Also creates the real topics if they don't exist yet.

Where Kafka is: KAFKA_BOOTSTRAP_SERVERS (default localhost:9094, the port docker-compose
publishes on this machine).
"""
import sys
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))   # so "pipeline" can be imported

from pipeline import kafka_client as kc        # noqa: E402
from pipeline.config import load_config        # noqa: E402

TEST_TOPIC = "anpr.selftest"

cfg = load_config()["kafka"]
print("Kafka at", kc.servers())

created = kc.ensure_topics(list(cfg["topics"].values()) + [TEST_TOPIC], cfg["partitions"])
print("Topics created:", ", ".join(created) if created else "none (all existed)")

token = uuid.uuid4().hex
producer = kc.make_producer()
kc.send(producer, TEST_TOPIC, key=token, data={"check": token})
kc.flush(producer)
print("Sent a test message to", TEST_TOPIC)

consumer = kc.make_consumer(f"selftest-{token}", [TEST_TOPIC])   # a new group: reads from the start
deadline = time.time() + 20
try:
    while time.time() < deadline:
        got = kc.receive(consumer)
        if got and got[1].get("check") == token:
            consumer.commit(got[0])
            print("OK: this machine can send to and receive from Kafka")
            break
    else:
        sys.exit("NOT OK: the test message did not come back within 20 s")
finally:
    consumer.close()
