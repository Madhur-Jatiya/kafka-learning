
import json
from confluent_kafka import Consumer

from app.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    ORDERS_TOPIC,
    ORDER_CONSUMER_GROUP
)


config = {
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "group.id": ORDER_CONSUMER_GROUP,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False
}


def process_order(order):

    print("\n--- Inventory Service ---")

    print("Order ID:", order["order_id"])
    print("Customer ID:", order["customer_id"])
    print("Amount:", order["amount"])

    print("Inventory processing completed")


def start_consumer():

    consumer = Consumer(config)

    consumer.subscribe([ORDERS_TOPIC])

    print("Order consumer started...")

    try:
        while True:

            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                print("Kafka error:", message.error())
                continue

            try:
                order = json.loads(
                    message.value().decode("utf-8")
                )

                process_order(order)

                # Commit only after successful processing.
                consumer.commit(
                    message=message,
                    asynchronous=False
                )

                print(
                    f"Committed offset: {message.offset()}"
                )

            except Exception as error:
                print("Processing failed:", error)

    except KeyboardInterrupt:
        print("Order consumer stopping...")

    finally:
        consumer.close()


if __name__ == "__main__":
    start_consumer()
