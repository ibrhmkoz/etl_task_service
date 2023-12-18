from app.transformer.datafusion_transformer import DataFusionTransformer


def test_datafusion_transformer():
    # Given
    kafka_messages = [
        b'10,20,40,5',
        b'2,3,7,22',
    ]
    source_schema = [
        {"column_0": "int32"},
        {"column_1": "int32"},
        {"column_2": "int32"},
        {"column_3": "int32"}
    ]
    sql_query = "SELECT column_0+column_1+column_2+column_3 FROM temp_table"
    transformer = DataFusionTransformer.create_data_fusion_transformer(
        sql_query, source_schema, "temp_table"
    )

    # When
    transformed_data = transformer.transform(kafka_messages)

    # Then
    assert transformed_data == [b'75', b'34']


def test_datafusion_transformer_with_dynamically_loaded_user_defined_function():
    # Given
    kafka_messages = [
        b'10,20,40,5',
        b'2,3,7,22',
    ]
    source_schema = [
        {"column_0": "int32"},
        {"column_1": "int32"},
        {"column_2": "int32"},
        {"column_3": "int32"}
    ]
    sql_query = "SELECT IS_EVEN(column_0 - column_1) FROM temp_table"
    transformer = DataFusionTransformer.create_data_fusion_transformer(
        sql_query, source_schema, "temp_table"
    )

    # When
    transformed_data = transformer.transform(kafka_messages)

    # Then
    assert transformed_data == [b'True', b'False']
