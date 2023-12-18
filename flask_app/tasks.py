from celery import shared_task
from celery.contrib.abortable import AbortableTask

from app.etl_task_iteration import ETLTaskIteration
from app.kit.callback_looper import CallbackLooper
from app.kit.lib import retry
from app.sink.kafka_sink import KafkaSink
from app.source.kafka_source import KafkaSource
from app.transformer.datafusion_transformer import DataFusionTransformer

from celery.utils.log import get_task_logger

KAFKA_SOURCE_TIMEOUT = 1.0
KAFKA_BATCH_SIZE = 10
logger = get_task_logger(__name__)


def abort_task(abortable_task):
    task_id = abortable_task.request.id
    result = abortable_task.AsyncResult(task_id)
    result.abort()


@shared_task(bind=True, base=AbortableTask)
def start_etl_task(self, etl_task):
    logger.info("Starting ETL task with ID: %s", self.request.id)
    source_config = etl_task["source_config"]
    sink_config = etl_task["sink_config"]
    sink_table = etl_task["sink_table"]
    source_table = etl_task["source_table"]
    sql = etl_task["sql"]

    source = KafkaSource.from_consumer_config(
        consumer_config=source_config,
        topic=source_table["topic"],
        timeout=KAFKA_SOURCE_TIMEOUT,
        batch_size=KAFKA_BATCH_SIZE
    )

    transformer = DataFusionTransformer.create_data_fusion_transformer(
        sql_query=sql,
        source_schema=source_table["schema"],
        table_name=source_table["table_name"]
    )

    sink = KafkaSink.from_producer_config(
        producer_config=sink_config,
        sink_topic=sink_table["topic"]
    )

    etl_task_iteration = ETLTaskIteration(source=source, transformer=transformer, sink=sink)
    retryable_etl_task_iteration = retry(callback=etl_task_iteration, times=3)

    is_failed = False

    def try_retryable_etl_task_iteration():
        nonlocal is_failed
        try:
            retryable_etl_task_iteration()
        except Exception as e:
            is_failed = True
            logger.error("Error during ETL task iteration: %s", e)
            source.close()
            sink.close()
            abort_task(self)

    callback_looper = CallbackLooper(callback=try_retryable_etl_task_iteration,
                                     so_long_as=lambda: (not self.is_aborted()) and (not is_failed))
    logger.info("Starting ETL task iteration loop")
    callback_looper.start_loop()
