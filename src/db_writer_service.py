import datetime
import logging
from numbers import Number

import asyncpg
from asyncpg import Connection

logger = logging.getLogger(__name__)


class DbWriterService:
    """
    Handles writing of website status data into database
    """
    connection: Connection

    async def connect(self, db_host: str, user: str, password: str, db_name: str, port: int):
        """
        Connect to the existing database
        :param db_host: Database host name
        :param user: Database user name
        :param password: User's password
        :param db_name: Database name
        :param port: Port of the database
        :return:
        """
        self.connection = await asyncpg.connect(
            user=user,
            port=port,
            password=password,
            database=db_name,
            host=db_host
        )

    async def insert_website_status_data(self,
                                         url: str,
                                         event_timestamp: int,
                                         latency_seconds: Number = None,
                                         status_code: int = None,
                                         ):
        """
        Saves website data into database.
        :param url: Url of the checked website
        :param event_timestamp: UTC POSIX timestamp of the event
        :param latency_seconds: website latency in seconds
        :param status_code: HTTP status code
        """
        logger.debug(f"Sending data to db: {url} {latency_seconds} {status_code}")
        await self.connection.execute(
            "INSERT INTO website_statuses (url, latency_seconds, status_code, event_timestamp) VALUES ($1, $2, $3, $4);",
            url,
            latency_seconds,
            status_code,
            datetime.datetime.fromtimestamp(event_timestamp),
        )
