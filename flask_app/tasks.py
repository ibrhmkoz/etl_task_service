from celery import shared_task
from celery.contrib.abortable import AbortableTask

from app.etl_task_iteration import ETLTaskIteration
from app.kit.callback_looper import CallbackLooper
from app.sink.kafka_sink import KafkaSink
from app.source.kafka_source import KafkaSource
from app.transformer.datafusion_transformer import DataFusionTransformer

KAFKA_SOURCE_TIMEOUT = 1.0
KAFKA_BATCH_SIZE = 10


@shared_task(bind=True, base=AbortableTask)
def start_etl_task(self, source_config, sink_config, sink_table, source_table, sql):
    source = KafkaSource.from_consumer_config(
        consumer_config=source_config,
        topic=source_table["topic"],
        timeout=KAFKA_SOURCE_TIMEOUT,
        batch_size=KAFKA_BATCH_SIZE
    )

    transformer = DataFusionTransformer.create_data_fusion_transformer(
        sql_query=sql,
        source_schema=source_table["schema"]
    )

    sink = KafkaSink.from_producer_config(
        producer_config=sink_config["bootstrap.servers"],
        sink_topic=sink_table["topic"]
    )

    etl_task_iteration = ETLTaskIteration(source=source, transformer=transformer, sink=sink)

    def is_not_aborted():
        return not self.is_aborted()

    callback_looper = CallbackLooper(callback=etl_task_iteration, so_long_as=is_not_aborted)
    callback_looper.start_loop()
