import datafusion

from app.transformer.datafusion_transformer import DataFusionTransformer


def test_datafusion_transformer():
    kafka_messages = [
        b'10,20,40,5',
        b'2,3,7,22',
    ]

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

    transformer = DataFusionTransformer.create_data_fusion_transformer(sql_query, source_schema, "temp_table")

    transformed_data = transformer.transform(kafka_messages)

    assert transformed_data == [b'75', b'34']
