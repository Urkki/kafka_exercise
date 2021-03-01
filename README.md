# Kafka exercise project

This project demonstrates how to use Kafka producer and Kafka consumer with Python for monitoring website status.
Website status for given urls are monitored every 10 seconds and Kafka producer sends them to Kafka server. Next, the Kafka 
consumer will receive a message from Kafka server and messages are written into postgres database. The Kafka consumer polls 
the server every 2 seconds.

The project is developed with Python 3.9.2 

### Set up
Following environment variables are needed for running the demo:

- KAFKA_HOST - Kafka server URL
- KAFKA_HOST_PORT - Kafka server port
- KAFKA_SSL_CAFILE_PATH - Folder path to SSL CA file
- KAFKA_SSL_CERTFILE_PATH - Folder path to SSL CERT file
- KAFKA_SSL_KEYFILE_PATH - Folder path to SSL key file
- DB_HOST - Database host url
- DB_PORT - Database host port
- DB_NAME - Database name
- DB_USER - Database user name
- DB_PASSWORD - Database user password

and database should have table called `website_statuses` and following columns:

- `url` -> text
- `latency_seconds` -> real
- `status_code` -> smallint
- `event_timestamp` -> timestamp without timezone 

Once these environment variables are set, install python packages and use 
`python main.py --urls <url_1> <url_2> ...... <url_N>` to run program.
Example command could be `python main.py --urls http://google.com http://google.fi`
