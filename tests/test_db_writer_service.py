import datetime
import unittest
from unittest import mock

from src.db_writer_service import DbWriterService


class DbWriterServiceTest(unittest.IsolatedAsyncioTestCase):

    async def test_insert_website_status_data_sends_correct_sql_query(self):
        mock_connection = mock.AsyncMock()
        expected_timestamp = 12
        expected_latency = 1.5
        expected_status_code = 404

        db_service = DbWriterService()
        db_service.connection = mock_connection

        await db_service.insert_website_status_data(
            url="checked_test_url",
            event_timestamp=expected_timestamp,
            latency_seconds=expected_latency,
            status_code=expected_status_code,
        )

        db_service.connection.execute.assert_called_once_with(
            "INSERT INTO website_statuses (url, latency_seconds, status_code, event_timestamp) VALUES ($1, $2, $3, $4);",
            "checked_test_url",
            expected_latency,
            expected_status_code,
            datetime.datetime(1970, 1, 1, 2, 0, 12),
        )


if __name__ == '__main__':
    unittest.main()
