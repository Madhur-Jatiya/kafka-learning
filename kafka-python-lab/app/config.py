import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

ORDERS_TOPIC = os.getenv("ORDERS_TOPIC", "orders")

ORDER_CONSUMER_GROUP = os.getenv("ORDER_CONSUMER_GROUP", "order-service")

ANALYTICS_CONSUMER_GROUP = os.getenv("ANALYTICS_CONSUMER_GROUP", "analytics-service")

AUTO_OFFSET_RESET = "earliest"
