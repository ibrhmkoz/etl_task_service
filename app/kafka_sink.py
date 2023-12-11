from confluent_kafka import Producer


class KafkaSink:
    def __init__(self, sink_config, sink_topic):
        self.producer = Producer(sink_config)
        self.sink_topic = sink_topic

    def load(self, records):
        for record in records.to_pandas().itertuples(index=False, name=None):
            message = ','.join(map(str, record))
            self.producer.produce(self.sink_topic, value=message)
        self.producer.flush()

    def close(self):
        self.producer.flush()
