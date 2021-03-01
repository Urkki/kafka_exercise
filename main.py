import argparse
import asyncio
import logging
import os
import sys
from typing import List

from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord

from src.db_writer_service import DbWriterService
from src.kafka_api import KafkaApi
from src.website_poller_service import WebsitePollerService

logger = logging.getLogger(__name__)


def get_commandline_args():
    """
    Parse and return command line arguments
    """
    argparser = argparse.ArgumentParser()
    argparser.add_argument(
        "--urls",
        nargs="*",
        type=str,
        default=["http://google.com"],
        help="list of urls for checking their status. Default is http://google.com"
    )
    return argparser.parse_args()


async def consume_kafka_messages(consumer: KafkaConsumer, db_writer: DbWriterService):
    """
    Starts to consume kafka messages every 2 seconds and writes kafka messages to database.
    """
    logger.info("Consumer is starting to poll kafka api")

    record: ConsumerRecord

    while True:
        records_dict = consumer.poll()

        for key, records in records_dict.items():
            logger.info("Got %d records. Writing them into database", len(records))
            for record in records:
                await db_writer.insert_website_status_data(**record.value)

        await asyncio.sleep(2)


async def start_consumer(kafka_api: KafkaApi):
    """
    Initializes kafka consumer and database writer.
    """
    logger.debug("Initializing kafka consumer")

    consumer = kafka_api.create_consumer()

    logger.info(f"Consumer subscribing to {kafka_api.WEBSITE_METRICS_TOPIC}")
    consumer.subscribe([kafka_api.WEBSITE_METRICS_TOPIC])

    db_writer = DbWriterService()
    await db_writer.connect(
        db_host=os.environ["DB_HOST"],
        port=int(os.environ["DB_PORT"]),
        db_name=os.environ["DB_NAME"],
        user=os.environ["DB_USER"],
        password=os.environ["DB_PASSWORD"],
    )

    await consume_kafka_messages(consumer, db_writer)


async def start_producer(kafka_api: KafkaApi, urls: List[str]):
    """
    Starts kafka producer for given urls. It polls website every 10 seconds and sends status to kafka.
    """
    logger.debug("Initializing kafka producer")
    poller = WebsitePollerService(urls=urls, kafka_api=kafka_api)
    await poller.start_periodic_polling()


async def main():
    """
    Starts kafka consumer and producer tasks.
    """
    init_logger()
    args = get_commandline_args()

    host_url = f"{os.environ['KAFKA_HOST']}:{os.environ['KAFKA_HOST_PORT']}"
    kafka_api = KafkaApi(
        bootstrap_servers=[host_url],
        security_protocol="SSL",
        ssl_cafile=os.environ["KAFKA_SSL_CAFILE_PATH"],
        ssl_certfile=os.environ["KAFKA_SSL_CERTFILE_PATH"],
        ssl_keyfile=os.environ["KAFKA_SSL_KEYFILE_PATH"],
    )

    kafka_api.create_topic(kafka_api.WEBSITE_METRICS_TOPIC)

    await asyncio.gather(
        start_consumer(kafka_api=kafka_api),
        start_producer(kafka_api=kafka_api, urls=args.urls),
    )


def init_logger():
    """
    Initializes root logger for console output.
    """
    # noinspection PyArgumentList
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(stream=sys.stdout)
        ],
    )


if __name__ == "__main__":
    asyncio.run(main())
