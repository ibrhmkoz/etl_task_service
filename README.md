# ETL Task Processor

ETL Task Processor is a versatile, extendable ETL (Extract, Transform, Load) task processing system, designed to
interact with Kafka and perform complex data transformations using Apache Arrow and DataFusion. It features a RESTful
API, plugin-based architecture for user-defined functions, and robust integration testing using Testcontainers.

## Features

- **RESTful API with HATEOAS**: Provides an intuitive API to manage ETL tasks, featuring HATEOAS (Hypermedia as the
  Engine of Application State) principles for enhanced API navigability.
- **Dynamic UDF (User Defined Function) Loading**: Offers a plugin system to dynamically load and register new UDFs for
  data transformation.
- **Integration Testing with Testcontainers**: Leverages Testcontainers for Kafka to ensure thorough integration
  testing.
- **Kafka Source and Sink Support**: Integrates seamlessly with Kafka for data extraction and loading.
- **Apache Arrow and DataFusion Powered**: Utilizes Apache Arrow for data representation and DataFusion for SQL query
  execution, enabling efficient data processing.

## Quick Start

### Prerequisites

- Docker and Docker Compose
- `kcat` for Kafka interaction

### Running the Project

To start the project, use Docker Compose:

```bash
docker-compose up
```

### Interacting with the API

Send a POST request to `http://127.0.0.1:8080/api/v1/etl_task` with the following payload to start an ETL task:

```json
{
  "source_config": {
    "bootstrap.servers": "kafka-service:29092",
    "group.id": "initial_group_id",
    "auto.offset.reset": "earliest"
  },
  "sink_config": {
    "bootstrap.servers": "kafka-service:29092"
  },
  "sink_table": {
    "topic": "test_topic2"
  },
  "source_table": {
    "topic": "test_topic",
    "table_name": "test_table",
    "schema": [
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
  },
  "sql": "SELECT IS_EVEN(column_0 + column_1) as sum from test_table"
}
```

Response:

```json
{
  "_links": [
    {
      "href": "/api/v1/etl_task/01b9d90a-f45d-4843-b473-a3b2e9c3273f",
      "rel": "status",
      "type": "GET"
    },
    {
      "href": "/api/v1/etl_task/01b9d90a-f45d-4843-b473-a3b2e9c3273f",
      "rel": "abort",
      "type": "DELETE"
    },
    {
      "href": "/api/v1/etl_task",
      "rel": "self",
      "type": "POST"
    }
  ],
  "content": "ETL task started successfully"
}
```

### Publishing Data to Kafka Topic

Use the provided script to publish data to a Kafka topic:

```bash
./publish_data.sh localhost:9092 test_topic 10000
```

### Integration Testing

Example test using Testcontainers for Kafka:

```python
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
```

### Adding New User Defined Functions

UDFs can be defined and registered as follows:

```python
def is_even(array: pa.Array) -> pa.Array:
    return pc.equal(pc.bit_wise_and(array, 1), 0).fill_null(False)


is_even_arr = datafusion.udf(
    is_even,
    [pa.int32()],
    pa.bool_(),
    "stable",
    name="is_even",
)


def register(ctx):
    ctx.register_udf(is_even_arr)
```

UDFs are loaded using the following mechanism in `DataFusionTransformer`:

```python
@staticmethod
def create_data_fusion_transformer(sql_query, source_schema, table_name):
    ctx = datafusion.SessionContext()

    udf_directory = Path(__file__).parent / "user_defined_functions"
    for filepath in udf_directory.glob("*.py"):
        module_name = filepath.stem
        if module_name != "__init__":
            spec = importlib.util.spec_from_file_location(module_name, filepath)
            udf_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(udf_module)
            udf_module.register(ctx)

    return DataFusionTransformer(ctx, sql_query, source_schema, table_name)
```

---

For more details on API usage, UDF creation, and system configuration, refer to the detailed documentation and example
codebase.

---

This README provides a high-level overview of your project, including how to get started, use the API, and add new UDFs.
Adjust the content as needed to fit the specifics of your project and repository structure.