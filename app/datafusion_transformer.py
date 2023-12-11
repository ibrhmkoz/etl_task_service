import pyarrow as pa


def get_pyarrow_type(column_type):
    type_mapping = {
        'int16': pa.int16(),
        'int32': pa.int32(),
        'int64': pa.int64(),
        'float32': pa.float32(),
        'float64': pa.float64(),
        'utf8': pa.string()
    }
    return type_mapping.get(column_type, pa.null())


def convert_to_pyarrow_schema(source_schema):
    return pa.schema(
        [(list(column.keys())[0], get_pyarrow_type(list(column.values())[0])) for column in
         source_schema]
    )


class DatafusionTransformer:
    def __init__(self, context, sql_query, source_schema):
        self.sql_query = sql_query
        self.context = context
        self.source_schema = convert_to_pyarrow_schema(source_schema)

    def transform(self, kafka_messages):
        # Convert messages to a format suitable for Datafusion
        rows = [[int(value) for value in msg.decode('utf-8').split(',')] for msg in kafka_messages]
        columns = [pa.array(col) for col in zip(*rows)]  # Create PyArrow arrays for each column
        batch = pa.RecordBatch.from_arrays(columns, schema=self.source_schema)

        # Register the batch as a table in the Datafusion context
        self.context.register_record_batches("temp_table", [[batch]])

        # Execute the SQL query against the registered table
        result = self.context.sql(self.sql_query).collect()

        return result
