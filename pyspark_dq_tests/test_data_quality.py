from pyspark.sql.functions import col


def test_schema_validation(sample_spark_dataframe, expected_schema):
    """
    Test to validate that the DataFrame schema matches the expected schema.
    """
    actual_schema = sample_spark_dataframe.schema
    assert actual_schema == expected_schema, f"Schema mismatch! Expected: {expected_schema}, Got: {actual_schema}"



def test_no_null_values_in_age(sample_spark_dataframe):
    """
    Test to check for null values in the 'age' column.
    """
    null_count = sample_spark_dataframe.filter(col("age").isNull()).count()
    assert null_count == 1, f"Expected 1 null value in 'age' column, but found {null_count}."

