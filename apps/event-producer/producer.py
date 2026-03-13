import argparse
import os
import random
import time
import uuid
from datetime import datetime, timezone

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.serializing_producer import SerializingProducer
from faker import Faker


def load_schema() -> str:
    schema_path = os.path.join(os.path.dirname(__file__), "schemas", "user_behavior_event.avsc")
    with open(schema_path, "r", encoding="utf-8") as handle:
        return handle.read()


def build_event(fake: Faker) -> dict:
    event_type = random.choice(
        ["CLICK", "SEARCH", "ADD_TO_CART", "REMOVE_FROM_CART", "CHECKOUT_START"]
    )
    user_id = random.randint(1000, 1015)
    product_id = random.randint(1, 50) if event_type != "SEARCH" else None
    keyword = fake.word() if event_type == "SEARCH" else None
    quantity = random.randint(1, 3) if event_type in {"ADD_TO_CART", "REMOVE_FROM_CART"} else None
    price = round(random.uniform(9.9, 250.0), 2) if product_id else None

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "session_id": fake.uuid4(),
        "event_type": event_type,
        "page": random.choice(["home", "search", "product", "checkout"]),
        "product_id": product_id,
        "keyword": keyword,
        "cart_id": fake.uuid4() if quantity else None,
        "order_id": random.randint(1, 20) if event_type == "CHECKOUT_START" else None,
        "quantity": quantity,
        "price": price,
        "device_type": random.choice(["mobile", "desktop", "tablet"]),
        "source": "web",
        "event_time": int(datetime.now(tz=timezone.utc).timestamp() * 1000),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Produce demo user behavior events.")
    parser.add_argument("--count", type=int, default=int(os.getenv("EVENT_COUNT", "50")))
    parser.add_argument(
        "--interval-ms",
        type=int,
        default=int(os.getenv("EVENT_INTERVAL_MS", "500")),
    )
    args = parser.parse_args()

    fake = Faker()
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "localhost:29092")
    topic = os.getenv("TOPIC", "raw.event.commerce.user_behavior_v3")

    schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})
    avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry_client,
        schema_str=load_schema(),
        to_dict=lambda obj, ctx: obj,
    )

    producer = SerializingProducer(
        {
            "bootstrap.servers": bootstrap_servers,
            "key.serializer": StringSerializer("utf_8"),
            "value.serializer": avro_serializer,
        }
    )

    for index in range(args.count):
        event = build_event(fake)
        producer.produce(topic=topic, key=str(event["user_id"]), value=event)
        if index % 20 == 0:
            producer.poll(0)
        time.sleep(args.interval_ms / 1000.0)

    producer.flush()


if __name__ == "__main__":
    main()
