import subprocess
from testcontainers.kafka import KafkaContainer
from confluent_kafka import Consumer

from app.kafka_source import KafkaSource


def test_kafka_source():
    with KafkaContainer() as kafka:
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

        # Publish data to Kafka using the script
        subprocess.run(["./publish_data.sh", bootstrap_server, topic, str(num_records)], check=True)

        # Extract data using KafkaSource
        data = kafka_source.extract()

        # Perform assertions here
        assert len(data) == num_records

        # Clean up
        kafka_source.close()
