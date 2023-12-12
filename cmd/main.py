import datafusion
from confluent_kafka import Consumer, Producer

from app.kit.callback_looper import CallbackLooper
from app.transformer.datafusion_transformer import DataFusionTransformer
from app.etl_task_iteration import ETLTaskIteration
from app.sink.kafka_sink import KafkaSink
from app.source.kafka_source import KafkaSource

if __name__ == "__main__":
    bootstrap_server = 'localhost:9092'
    topic = "test_topic"
    batch_size = 10

    consumer_config = {'bootstrap.servers': bootstrap_server,
                       'group.id': 'testgroup',
                       'auto.offset.reset': 'earliest'}
    consumer = Consumer(consumer_config)
    consumer.subscribe([topic])
    source = KafkaSource(kafka_consumer=consumer, timeout=1.0, batch_size=batch_size)

    ctx = datafusion.SessionContext()
    source_schema = [
        {
            "column_0": "int32"
        },
        {
            "column_1": "int32"
        },
        {
            "column_2": "int32"
        },
        {
            "column_3": "int32"
        }
    ]
    sql_query = "SELECT column_0+column_1+column_2+column_3 FROM temp_table"
    transformer = DataFusionTransformer(context=ctx, sql_query=sql_query, source_schema=source_schema)

    producer_config = {'bootstrap.servers': bootstrap_server}
    producer = Producer(producer_config)
    sink_topic = "test_sink_topic"
    sink = KafkaSink(producer=producer, sink_topic=sink_topic)

    etl_task_iteration = ETLTaskIteration(source=source, transformer=transformer, sink=sink)

    so_long_as_condition = [3, 2, 1]


    def so_long_as():
        return bool(so_long_as_condition.pop() if so_long_as_condition else False)


    callback_looper = CallbackLooper(callback=etl_task_iteration, so_long_as=so_long_as)
