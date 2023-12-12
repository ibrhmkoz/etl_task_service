import subprocess
from testcontainers.kafka import KafkaContainer
from confluent_kafka import Consumer

from app.source.kafka_source import KafkaSource
from app.kit.lib import time_block


def test_kafka_source():
    with KafkaContainer() as kafka:
        with time_block("Bootstrapping Kafka Source"):
            bootstrap_server = kafka.get_bootstrap_server()
            topic = "test_topic"
            num_records = 10

            # Start the KafkaSource consumer
            consumer_config = {'bootstrap.servers': bootstrap_server,
                               'group.id': 'testgroup',
                               'auto.offset.reset': 'earliest'}
            consumer = Consumer(consumer_config)
            consumer.subscribe([topic])
            kafka_source = KafkaSource(consumer, timeout=1.0, batch_size=num_records)

        # Measure the time taken to publish data to Kafka
        with time_block("Publishing data to Kafka"):
            subprocess.run(["./publish_data.sh", bootstrap_server, topic, str(num_records)], check=True)

        # Measure the time taken to extract data using KafkaSource
        with time_block("Extracting data with KafkaSource"):
            data = kafka_source.extract()

        # Perform assertions here
        assert len(data) == num_records

        # Clean up
        kafka_source.close()
