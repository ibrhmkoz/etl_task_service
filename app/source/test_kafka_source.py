import subprocess
from testcontainers.kafka import KafkaContainer

from app.source.kafka_source import KafkaSource
from app.kit.lib import time_block


def test_kafka_source():
    with KafkaContainer() as kafka:
        with time_block("Bootstrapping Kafka Source"):
            bootstrap_server = kafka.get_bootstrap_server()
            topic = "test_topic"
            num_records = 10

            consumer_config = {'bootstrap.servers': bootstrap_server,
                               'group.id': 'testgroup',
                               'auto.offset.reset': 'earliest'}
            kafka_source = KafkaSource.from_consumer_config(
                consumer_config=consumer_config,
                topic=topic,
                timeout=1.0,
                batch_size=num_records
            )

        with time_block("Publishing data to Kafka"):
            subprocess.run(["./publish_data.sh", bootstrap_server, topic, str(num_records)], check=True)

        with time_block("Extracting data with KafkaSource"):
            data = kafka_source.extract()

        assert len(data) == num_records

        kafka_source.close()
