from testcontainers.kafka import KafkaContainer
from confluent_kafka import Producer, Consumer

from app.sink.kafka_sink import KafkaSink


def test_kafka_sink():
    with KafkaContainer() as kafka:
        bootstrap_server = kafka.get_bootstrap_server()
        topic = "test_sink_topic"
        test_messages = [b'1', b'2', b'3']

        # Create Kafka producer
        producer_config = {'bootstrap.servers': bootstrap_server}
        producer = Producer(producer_config)
        kafka_sink = KafkaSink(producer, topic)

        # Load messages using KafkaSink
        kafka_sink.load(test_messages)

        # Create Kafka consumer to read messages back
        consumer_config = {
            'bootstrap.servers': bootstrap_server,
            'group.id': 'testgroup',
            'auto.offset.reset': 'earliest'
        }
        consumer = Consumer(consumer_config)
        consumer.subscribe([topic])

        # Consume messages
        consumed_messages = []
        for _ in range(len(test_messages)):
            msg = consumer.poll(timeout=2.0)
            if msg is not None and not msg.error():
                consumed_messages.append(msg.value())

        # Perform assertions
        assert consumed_messages == test_messages

        # Clean up
        kafka_sink.close()
        consumer.close()
