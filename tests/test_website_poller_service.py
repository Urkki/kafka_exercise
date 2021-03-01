import unittest
from unittest import mock

from freezegun import freeze_time

from src.website_poller_service import WebsitePollerService


class WebsitePollerServiceTest(unittest.IsolatedAsyncioTestCase):
    expected_url = "localhost"

    @freeze_time("1970-01-01 00:01:01")
    @mock.patch("src.website_poller_service.requests.get", autospec=True)
    async def test_check_website_metrics_return_correct_data(self, mock_get):
        expected_status_code = 200
        expected_latency = 1.2
        expected_timestamp = 61

        mock_response = mock.MagicMock()
        mock_response.status_code = expected_status_code
        mock_response.elapsed.total_seconds.return_value = expected_latency
        mock_response.url = self.expected_url

        mock_get.return_value = mock_response

        expected_data = {
            "status_code": expected_status_code,
            "latency_seconds": expected_latency,
            "url": self.expected_url,
            "event_timestamp": expected_timestamp,
        }

        actual_data = await WebsitePollerService.check_website_metrics(self.expected_url)

        self.assertEqual(expected_data, actual_data)
        mock_get.assert_called_once_with(self.expected_url)

    @freeze_time("1970-01-01 00:01:01")
    @mock.patch("src.website_poller_service.requests.get", side_effect=TimeoutError('timeout error'), autospec=True)
    async def test_check_website_metrics_return_correct_data_when_exception_occurs(self, mock_get):
        expected_timestamp = 61

        expected_data = {
            "status_code": None,
            "latency_seconds": None,
            "url": self.expected_url,
            "event_timestamp": expected_timestamp,
        }

        actual_data = await WebsitePollerService.check_website_metrics(self.expected_url)

        self.assertEqual(expected_data, actual_data)
        mock_get.assert_called_once_with(self.expected_url)

    async def test_process_url_calls_checks_and_sends_metrics(self):
        mock_kafka_api = mock.MagicMock()
        mock_producer = mock.MagicMock()
        mock_check_website_metrics = mock.AsyncMock()
        mock_website_data = mock.MagicMock()
        mock_kafka_api.create_producer.return_value = mock_producer
        mock_check_website_metrics.return_value = mock_website_data

        service = WebsitePollerService(urls=[], kafka_api=mock_kafka_api)
        service.check_website_metrics = mock_check_website_metrics

        await service.process_url(self.expected_url)

        mock_check_website_metrics.assert_called_once_with(self.expected_url)
        mock_kafka_api.send_website_metrics.assert_called_once_with(
            producer=mock_producer,
            website_metrics=mock_website_data,
        )

    async def test_do_poll_polls_every_url(self):
        expected_url2 = "localhost2"
        mock_kafka_api = mock.MagicMock()
        mock_process_url = mock.AsyncMock()

        service = WebsitePollerService(urls=[self.expected_url, expected_url2], kafka_api=mock_kafka_api)
        service.process_url = mock_process_url

        await service.do_poll()

        self.assertEqual(2, mock_process_url.call_count)
        mock_process_url.assert_any_call(self.expected_url)
        mock_process_url.assert_any_call(expected_url2)


if __name__ == '__main__':
    unittest.main()
