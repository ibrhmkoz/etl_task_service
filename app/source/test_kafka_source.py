import subprocess
from testcontainers.kafka import KafkaContainer

from app.source.kafka_source import KafkaSource


def test_kafka_source():
    with KafkaContainer() as kafka:
        # Given
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

        subprocess.run(["./publish_data.sh", bootstrap_server, topic, str(num_records)], check=True)

        # When
        data = kafka_source.extract()

        # Then
        assert len(data) == num_records

        # Cleanup
        kafka_source.close()
