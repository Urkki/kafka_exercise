import asyncio
import datetime
import logging
from datetime import timedelta
from typing import List, Dict

import requests

from src.kafka_api import KafkaApi

logger = logging.getLogger(__name__)


class WebsitePollerService:
    """
    Handles the logic of polling websites.
    """

    def __init__(self, urls: List[str], kafka_api: KafkaApi, poll_interval: timedelta = timedelta(seconds=10)):
        """
        Initializes website poller service.

        :param urls: List of url which are checked periodically
        :param kafka_api: Interface to kafka
        :param poll_interval: Website poll interval in seconds
        """
        self.urls = urls
        self.poll_interval = poll_interval
        self.kafka_api = kafka_api
        self.kafka_producer = kafka_api.create_producer()

    @staticmethod
    async def check_website_metrics(url: str) -> Dict:
        """
        Checks website status for given url. It sends GET request to url via requests library.
        HTTP status code, latency, url and event timestamp are stored.

        :param url: url which is going to be checked
        :return: dict which contains website status
        """
        loop = asyncio.get_event_loop()

        logger.debug(f"checking website metrics for {url}")
        event_timestamp = int(datetime.datetime.utcnow().timestamp())

        try:
            response = await loop.run_in_executor(None, requests.get, url)
            data = {
                "status_code": response.status_code,
                "latency_seconds": response.elapsed.total_seconds(),
                "url": response.url,
                "event_timestamp": event_timestamp,
            }
        except Exception as e:
            logger.error("Got exception when checking %s website status. Exception: %s", url, str(e))
            data = {
                "status_code": None,
                "latency_seconds": None,
                "url": url,
                "event_timestamp": event_timestamp,
            }

        logger.debug(f"got metrics: {data}")
        return data

    async def process_url(self, url: str):
        """
        Process given url by checking its status and sending metrics to kafka.

        :param url: URL to be checked
        """
        website_metrics = await self.check_website_metrics(url)
        self.kafka_api.send_website_metrics(
            producer=self.kafka_producer,
            website_metrics=website_metrics,
        )

    async def do_poll(self):
        """
        Polls urls asynchronously once.
        """

        tasks = [self.process_url(url) for url in self.urls]

        logger.info("Checking %d website statuses", len(tasks))
        await asyncio.gather(*tasks)

    async def start_periodic_polling(self):
        """
        Start periodic polling for urls.
        """
        poll_delay = self.poll_interval.total_seconds()

        logger.info(f"Starting to poll {len(self.urls)} websites every {poll_delay} seconds")

        while True:
            await asyncio.sleep(poll_delay)
            await self.do_poll()
