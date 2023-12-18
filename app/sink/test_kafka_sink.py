from testcontainers.kafka import KafkaContainer
from confluent_kafka import Consumer
from app.sink.kafka_sink import KafkaSink


def test_kafka_sink():
    with KafkaContainer() as kafka:
        # Given
        bootstrap_server = kafka.get_bootstrap_server()
        test_messages = [b'1', b'2', b'3']
        topic = "test_sink_topic"
        producer_config = {'bootstrap.servers': bootstrap_server}
        kafka_sink = KafkaSink.from_producer_config(producer_config, topic)
        consumer_config = {
            'bootstrap.servers': bootstrap_server,
            'group.id': 'testgroup',
            'auto.offset.reset': 'earliest'
        }
        consumer = Consumer(consumer_config)
        consumer.subscribe([topic])

        # When
        kafka_sink.load(test_messages)
        consumed_messages = []
        for _ in range(len(test_messages)):
            msg = consumer.poll(timeout=2.0)
            if msg is not None and not msg.error():
                consumed_messages.append(msg.value())

        # Then
        assert consumed_messages == test_messages

        # Cleanup
        kafka_sink.close()
        consumer.close()
