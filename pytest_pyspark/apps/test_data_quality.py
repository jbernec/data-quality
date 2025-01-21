from pyspark.sql.functions import col
import pytest
# import findspark
# findspark.init()

# parameterize the test with different source paths
#@pytest.mark.parametrize("sample_source_dataframe", ["abfss://experiments@adls04.dfs.core.windows.net/pytest"], indirect=True)

def test_schema_validation(actual_schema, expected_schema):
    """
    Test to validate that the DataFrame schema matches the expected schema.
    """
    assert actual_schema == expected_schema, f"Schema mismatch! Expected: {expected_schema}, Got: {actual_schema}"



def test_no_null_values_in_col(actual_spark_dataframe):
    """
    Test to check for null values in the 'age' column.
    """
    null_count = actual_spark_dataframe.filter(col("age").isNull()).count()
    assert null_count == 0, f"Expected 0 null value in 'age' column, but found {null_count}."


def test_unique_id_column(actual_spark_dataframe):
    """
    Test to ensure that the 'id' column has unique values.
    """
    total_count = actual_spark_dataframe.count()
    distinct_id_count = actual_spark_dataframe.select("id").distinct().count()
    assert total_count == distinct_id_count, "ID column contains duplicate values!"


def test_unique_id_column_source(data_source_dataframe):
    """
    Test to ensure that the 'id' column has unique values.
    """
    total_count = data_source_dataframe.count()
    distinct_id_count = data_source_dataframe.select("id").distinct().count()
    assert total_count == distinct_id_count, "ID column contains duplicate values!"


# Define the invalid characters (e.g., special characters)
INVALID_CHARACTERS = "[^a-zA-Z0-9_ ]" #Character Class Exclusion (Negated Set)

# Test function to check for invalid characters in specified columns
@pytest.mark.parametrize("columns", [["name", "comment"]])
def test_no_invalid_characters(invalid_xter_dataframe, columns):
    for column in columns:
        # Filter rows where the column contains invalid characters
        invalid_rows = invalid_xter_dataframe.filter(col(column).rlike(INVALID_CHARACTERS))
        invalid_count = invalid_rows.count()

        # Assert that no invalid characters are found
        assert invalid_count == 0, f"Invalid characters found in column '{column}'"

        # Optional: Print the invalid rows for debugging
        if invalid_count > 0:
            invalid_rows.show()


