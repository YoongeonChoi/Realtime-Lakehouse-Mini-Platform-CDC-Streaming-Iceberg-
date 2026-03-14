import argparse
import os
import random
import time
import uuid
from datetime import datetime, timezone
from typing import Callable

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.serializing_producer import SerializingProducer
from faker import Faker


SCHEMA_BY_MODE = {
    "user_behavior": "user_behavior_event.avsc",
    "crypto_tick": "crypto_tick_event.avsc",
}

DEFAULT_TOPIC_BY_MODE = {
    "user_behavior": "raw.event.commerce.user_behavior_v3",
    "crypto_tick": "raw.event.market.crypto_ticks_v1",
}


def load_schema(mode: str) -> str:
    schema_file = SCHEMA_BY_MODE[mode]
    schema_path = os.path.join(os.path.dirname(__file__), "schemas", schema_file)
    with open(schema_path, "r", encoding="utf-8") as handle:
        return handle.read()


def build_user_behavior_event(fake: Faker, event_time_ms: int | None = None) -> dict:
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
        "event_time": event_time_ms or int(datetime.now(tz=timezone.utc).timestamp() * 1000),
    }


def build_crypto_tick_event(price_state: dict[str, float], event_time_ms: int | None = None) -> dict:
    symbol = random.choice(["BTC-USD", "ETH-USD", "SOL-USD", "XRP-USD"])
    reference_price = price_state.get(
        symbol,
        {
            "BTC-USD": 68000.0,
            "ETH-USD": 3400.0,
            "SOL-USD": 145.0,
            "XRP-USD": 0.62,
        }[symbol],
    )
    price_delta = random.uniform(-0.004, 0.004) * reference_price
    next_price = round(max(reference_price + price_delta, 0.0001), 4)
    price_state[symbol] = next_price

    volume = round(random.uniform(0.05, 4.5), 6)

    return {
        "tick_id": str(uuid.uuid4()),
        "symbol": symbol,
        "exchange": random.choice(["binance", "coinbase", "bybit"]),
        "price": next_price,
        "volume": volume,
        "side": random.choice(["BUY", "SELL"]),
        "trade_id": f"{symbol.replace('-', '')}-{uuid.uuid4().hex[:12]}",
        "ingest_source": "simulated_market_feed",
        "event_time": event_time_ms or int(datetime.now(tz=timezone.utc).timestamp() * 1000),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Produce demo streaming events.")
    parser.add_argument(
        "--mode",
        choices=sorted(SCHEMA_BY_MODE.keys()),
        default=os.getenv("PRODUCER_MODE", "user_behavior"),
    )
    parser.add_argument("--count", type=int, default=int(os.getenv("EVENT_COUNT", "50")))
    parser.add_argument(
        "--interval-ms",
        type=int,
        default=int(os.getenv("EVENT_INTERVAL_MS", "500")),
    )
    parser.add_argument(
        "--event-time-step-ms",
        type=int,
        default=int(os.getenv("EVENT_TIME_STEP_MS", "0")),
        help="Advance event_time by a fixed amount per record to simulate a faster event clock.",
    )
    args = parser.parse_args()

    fake = Faker()
    price_state: dict[str, float] = {}
    base_event_time_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    schema_registry_url = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
    bootstrap_servers = os.getenv("BOOTSTRAP_SERVERS", "localhost:29092")
    topic = os.getenv("TOPIC", DEFAULT_TOPIC_BY_MODE[args.mode])
    key_builder: Callable[[dict], str]
    event_builder: Callable[[], dict]

    if args.mode == "user_behavior":
        key_builder = lambda event: str(event["user_id"])
        event_builder = lambda event_time_ms: build_user_behavior_event(fake, event_time_ms)
    else:
        key_builder = lambda event: event["symbol"]
        event_builder = lambda event_time_ms: build_crypto_tick_event(price_state, event_time_ms)

    schema_registry_client = SchemaRegistryClient({"url": schema_registry_url})
    avro_serializer = AvroSerializer(
        schema_registry_client=schema_registry_client,
        schema_str=load_schema(args.mode),
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
        event_time_ms = None
        if args.event_time_step_ms > 0:
            event_time_ms = base_event_time_ms + (index * args.event_time_step_ms)

        event = event_builder(event_time_ms)
        producer.produce(topic=topic, key=key_builder(event), value=event)
        if index % 20 == 0:
            producer.poll(0)
        time.sleep(args.interval_ms / 1000.0)

    producer.flush()


if __name__ == "__main__":
    main()
