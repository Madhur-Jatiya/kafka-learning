
from confluent_kafka.admin import AdminClient, NewTopic

from app.config import KAFKA_BOOTSTRAP_SERVERS


admin = AdminClient({
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
})


def create_topics():

    topics = [
        NewTopic(
            topic="orders",
            num_partitions=3,
            replication_factor=1
        )
    ]

    futures = admin.create_topics(topics)

    for topic, future in futures.items():

        try:
            future.result()
            print(f"Topic created: {topic}")

        except Exception as error:
            if "TOPIC_ALREADY_EXISTS" in str(error):
                print(f"Topic already exists: {topic}")
            else:
                print(f"Failed to create {topic}: {error}")
                raise


if __name__ == "__main__":
    create_topics()
