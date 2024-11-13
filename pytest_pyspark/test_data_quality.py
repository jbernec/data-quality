from pyspark.sql.functions import col
import pytest

# parameterize the test with different source paths
#@pytest.mark.parametrize("sample_source_dataframe", ["abfss://experiments@adls04.dfs.core.windows.net/pytest"], indirect=True)

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


def test_unique_id_column(sample_spark_dataframe):
    """
    Test to ensure that the 'id' column has unique values.
    """
    total_count = sample_spark_dataframe.count()
    distinct_id_count = sample_spark_dataframe.select("id").distinct().count()
    assert total_count == distinct_id_count, "ID column contains duplicate values!"


def test_unique_id_column_source(sample_source_dataframe):
    """
    Test to ensure that the 'id' column has unique values.
    """
    total_count = sample_source_dataframe.count()
    distinct_id_count = sample_source_dataframe.select("id").distinct().count()
    assert total_count == distinct_id_count, "ID column contains duplicate values!"


