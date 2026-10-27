
from confluent_kafka import Producer

from app.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    ORDERS_TOPIC
)

from app.models.order import Order


producer = Producer({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "client.id": "order-producer",
    "acks": "all"
})


def delivery_report(error, message):

    if error:
        print(f"Delivery failed: {error}")

    else:
        print(
            f"Message delivered successfully | "
            f"Topic: {message.topic()} | "
            f"Partition: {message.partition()} | "
            f"Offset: {message.offset()}"
        )


def publish_order(order: Order):

    producer.produce(
        topic=ORDERS_TOPIC,
        key=str(order.customer_id),
        value=order.to_kafka_message(),
        callback=delivery_report
    )

    # Trigger delivery callbacks and serve events.
    producer.poll(0)


def close_producer():
    remaining = producer.flush(10)

    if remaining > 0:
        print(f"Warning: {remaining} messages were not delivered")


if __name__ == "__main__":

    order = Order(
        order_id=101,
        customer_id=501,
        amount=2500
    )

    try:
        publish_order(order)
    finally:
        close_producer()
