from confluent_kafka import Producer


class KafkaSink:
    def __init__(self, producer, sink_topic):
        self.producer = producer
        self.sink_topic = sink_topic

    def load(self, messages):
        for message in messages:
            self.producer.produce(self.sink_topic, value=message)
        self.producer.flush()

    def close(self):
        self.producer.flush()

    @staticmethod
    def from_producer_config(producer_config, sink_topic):
        producer = Producer(producer_config)
        return KafkaSink(producer, sink_topic)
