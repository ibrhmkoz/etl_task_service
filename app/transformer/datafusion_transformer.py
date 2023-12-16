import uuid

import datafusion
import pyarrow as pa
import pyarrow.compute as pc


def is_even(array: pa.Array) -> pa.Array:
    return pc.equal(pc.bit_wise_and(array, 1), 0).fill_null(False)


is_even_arr = datafusion.udf(
    is_even,
    [pa.int32()],
    pa.bool_(),
    "stable",
    name="is_even",
)


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


def convert_back_to_kafka_messages(result):
    temp = result[0].to_pydict()
    values = temp.values()
    rows = zip(*values)
    encoded_rows = [",".join([str(r) for r in row]).encode('utf-8') for row in list(rows)]
    return encoded_rows


class DataFusionTransformer:
    def __init__(self, context, sql_query, source_schema, table_name):
        self.sql_query = sql_query
        self.context = context
        self.source_schema = convert_to_pyarrow_schema(source_schema)
        self.table_name = table_name

    def transform(self, kafka_messages):
        if len(kafka_messages) == 0:
            return []

        batch = self.convert_to_record_batch(kafka_messages)

        unique_table_name = f"{self.table_name}_{uuid.uuid4().hex}"

        self.context.register_record_batches(unique_table_name, [[batch]])
        modified_sql_query = self.sql_query.replace(self.table_name, unique_table_name)
        result = self.context.sql(modified_sql_query).collect()

        return convert_back_to_kafka_messages(result)

    def convert_to_record_batch(self, kafka_messages):
        rows = [[int(value) for value in msg.decode('utf-8').split(',')] for msg in kafka_messages]
        columns = [pa.array(col) for col in zip(*rows)]
        batch = pa.RecordBatch.from_arrays(columns, schema=self.source_schema)
        return batch

    @staticmethod
    def create_data_fusion_transformer(sql_query, source_schema, table_name):
        ctx = datafusion.SessionContext()
        return DataFusionTransformer(ctx, sql_query, source_schema, table_name)
