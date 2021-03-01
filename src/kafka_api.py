import json
import logging

from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic

logger = logging.getLogger(__name__)


class KafkaApi:
    """
    Provides interface to kafka server
    """

    WEBSITE_METRICS_TOPIC = "website_metrics_topic"

    def __init__(self, **settings):
        """
        Stores settings for creating kafka consumers, producers and admin client
        :param settings: dict of settings
        """
        self.settings = settings

    def create_producer(self) -> KafkaProducer:
        """
        Creates a kafka producer with json serializer. UTF-8 encoding is used.
        :return: KafkaProducer
        """
        return KafkaProducer(
            value_serializer=lambda value: json.dumps(value).encode("utf-8"),
            **self.settings,
        )

    def create_consumer(self) -> KafkaConsumer:
        """
        Creates a kafka consumer with json deserializer.
        :return: KafkaConsumer
        """
        return KafkaConsumer(
            **self.settings,
            value_deserializer=lambda value: json.loads(value),
        )

    def send_website_metrics(self, producer: KafkaProducer, website_metrics: dict):
        """
        Sends website metrics to kafka server
        :param producer: KafkaProducer
        :param website_metrics: dict of website metrics. The dict should contain following keys:
        "status_code": HTTP response status code
        "latency_seconds": Latency in seconds
        "url": URL of the checked website
        "event_timestamp": UTC POSIX timestamp of the event
        :return:
        """
        logger.debug(f"sending website metrics {website_metrics} to kafka")
        producer.send(topic=self.WEBSITE_METRICS_TOPIC, value=website_metrics)

    def create_topic(self, topic_name: str, num_partitions: int = 1, replication_factor: int = 1):
        """
        Creates a new topic if not already present
        :param topic_name: New topic name
        :param num_partitions: Number of partitions. Default value is 1.
        :param replication_factor: Replication factor. Default value is 1.
        """
        logger.debug(f"Creating a topic called {topic_name}")
        admin = KafkaAdminClient(
            **self.settings
        )

        existing_topics = admin.list_topics()
        if topic_name in existing_topics:
            admin.close()
            return

        topic = [
            NewTopic(topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
        ]
        admin.create_topics(topic)
        admin.close()
        logger.debug(f"Topic {topic_name} created")
