from confluent_kafka import KafkaError


class KafkaSource:
    def __init__(self, kafka_consumer, timeout, batch_size):
        self.kafka_consumer = kafka_consumer
        self.timeout = timeout
        self.batch_size = batch_size

    def extract(self):
        batch = []
        for _ in range(self.batch_size):
            msg = self.kafka_consumer.poll(timeout=self.timeout)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"Consumer error: {msg.error()}")
                    continue

            batch.append(msg.value())

        return batch

    def close(self):
        self.kafka_consumer.close()
