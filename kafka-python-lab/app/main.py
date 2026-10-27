
from app.models.order import Order

from app.producer.order_producer import (
    publish_order,
    close_producer
)


def main():

    orders = [
        Order(
            order_id=101,
            customer_id=501,
            amount=2500
        ),

        Order(
            order_id=102,
            customer_id=502,
            amount=1500
        ),

        Order(
            order_id=103,
            customer_id=501,
            amount=3500
        ),

        Order(
            order_id=104,
            customer_id=503,
            amount=4500
        ),

        Order(
            order_id=105,
            customer_id=502,
            amount=1200
        )
    ]

    try:

        for order in orders:

            publish_order(order)

            print(
                f"Publishing order: {order.order_id}"
            )

    finally:
        close_producer()

    print("\nAll orders submitted!")


if __name__ == "__main__":
    main()
