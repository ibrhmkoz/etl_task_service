import datafusion

from app.transformer.datafusion_transformer import DataFusionTransformer


def test_datafusion_transformer():
    # Sample data similar to what KafkaSource returns
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

    # Define an SQL query for transformation
    sql_query = "SELECT column_0+column_1+column_2+column_3 FROM temp_table"

    # Create DatafusionTransformer instance
    transformer = DataFusionTransformer.create_data_fusion_transformer(sql_query, source_schema)

    # Transform data
    transformed_data = transformer.transform(kafka_messages)

    # Assertions
    assert transformed_data == [b'75', b'34']
