import pyarrow as pa


class DatafusionTransformer:
    def __init__(self, sql_query, context, source_schema):
        self.sql_query = sql_query
        self.context = context
        self.source_schema = source_schema

    def transform(self, kafka_messages):
        # Convert messages to a format suitable for Datafusion
        rows = [[int(value) for value in msg.decode('utf-8').split(',')] for msg in kafka_messages]
        columns = list(map(list, zip(*rows)))
        batch = pa.RecordBatch.from_arrays(columns, schema=self.source_schema)

        self.context.register_record_batches("temp_table", [batch])

        return self.context.sql(self.sql_query).collect()
