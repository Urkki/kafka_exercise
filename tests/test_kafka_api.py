import unittest
from unittest import mock

from kafka.admin import NewTopic

from src.kafka_api import KafkaApi


class KafkaApiTest(unittest.TestCase):
    def test_send_website_metrics_to_correct_topic(self):
        mock_producer = mock.MagicMock()
        metrics = {
            "status_code": 404,
            "latency_seconds": 12,
            "url": "localhost",
            "event_timestamp": 123,
        }

        kafka_api = KafkaApi()
        kafka_api.send_website_metrics(mock_producer, metrics)

        mock_producer.send.assert_called_once_with(
            topic="website_metrics_topic",
            value=metrics,
        )

    @mock.patch("src.kafka_api.KafkaAdminClient", autospec=True)
    def test_create_new_topic_if_not_exist(self, mock_admin):
        mock_admin_instance = mock_admin.return_value

        mock_admin_instance.list_topics.return_value = []
        kafka_api = KafkaApi()
        kafka_api.create_topic("test_topic_name", 2, 3)

        expected_topic = NewTopic("test_topic_name", num_partitions=2, replication_factor=3)
        mock_admin_instance.create_topics.assert_called_once()
        actual_topic = mock_admin_instance.create_topics.call_args.args[0][0]
        self.assertEqual(expected_topic.name, actual_topic.name)
        self.assertEqual(expected_topic.num_partitions, actual_topic.num_partitions)
        self.assertEqual(expected_topic.replication_factor, actual_topic.replication_factor)

        mock_admin_instance.close.assert_called_once()

    @mock.patch("src.kafka_api.KafkaAdminClient", autospec=True)
    def test_dont_create_topic_if_exist(self, mock_admin):
        mock_admin_instance = mock_admin.return_value

        mock_admin_instance.list_topics.return_value = ["test_topic_name"]
        kafka_api = KafkaApi()
        kafka_api.create_topic("test_topic_name", 2, 3)

        mock_admin_instance.create_topics.assert_not_called()
        mock_admin_instance.close.assert_called_once()


if __name__ == '__main__':
    unittest.main()
