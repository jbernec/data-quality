import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

@pytest.fixture
def spark():
  pass
  spark = SparkSession.builder \
          .appName("Pytest-PySpark-Testing") \
          .getOrCreate()
  return spark

@pytest.fixture(params=["dbfs:/files/data_sample.csv"])
def read_data_testing(spark, request):
  pass
  #df_lake = spark.read.format("delta").load("abfss://experiments@adls04.dfs.core.windows.net/pytest")
  source_file = request.param
  df = spark.read.format("csv").option("header", "true").load(source_file)
  return df


@pytest.fixture
def sample_spark_dataframe(spark):
    """
    Pytest fixture for creating a sample PySpark DataFrame with 6 rows and 4 columns.
    """
    data = [
        (1, "Alice", 25, "Engineer"),
        (2, "Bob", 30, "Doctor"),
        (3, "Charlie", None, "Artist"),   # Intentional null age
        (4, "David", 45, "Chef"),
        (5, "Eve", 28, "Data Scientist"),
        (6, "Frank", 33, "Lawyer")
    ]

    schema = StructType([
        StructField("id", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("age", IntegerType(), nullable=True),
        StructField("profession", StringType(), nullable=False)
    ])

    return spark.createDataFrame(data, schema)
  
@pytest.fixture
def expected_schema():
    """
    Pytest fixture for the expected PySpark DataFrame schema.
    """
    return StructType([
        StructField("id", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("age", IntegerType(), nullable=True),
        StructField("profession", StringType(), nullable=False)
    ])

def test_unique_id_column(read_data_testing):
    """
    Test to ensure that the 'id' column has unique values.
    """
    total_count = read_data_testing.count()
    distinct_id_count = read_data_testing.select("id").distinct().count()
    assert total_count == distinct_id_count, "ID column contains duplicate values!"
    print(distinct_id_count)

def test_schema_validation(sample_spark_dataframe, expected_schema):
    """
    Test to validate that the DataFrame schema matches the expected schema.
    """
    actual_schema = sample_spark_dataframe.schema
    assert actual_schema == expected_schema, f"Schema mismatch! Expected: {expected_schema}, Got: {actual_schema}"



@pytest.fixture
def invalid_test_data(spark):
    schema = StructType([
        StructField("id", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("age", IntegerType(), nullable=True),
        StructField("profession", StringType(), nullable=False)
    ])

    # Data with some invalid entries
    data = [
        (1, "John Doe", 30, "Engineer"),       # Valid row
        (2, "Jane Doe", "thirty", "Doctor"),   # Invalid age (string)
        ("3", "Alex", 25, "Lawyer"),           # Invalid id (string)
        (4, None, 40, "Scientist"),            # Invalid name (None)
        (5, "Mike", 29, None),                 # Invalid profession (None)
    ]

    return spark.createDataFrame(data, schema=schema)


def test_type_check(invalid_test_data):
    for row in invalid_test_data.collect():
        assert isinstance(row["id"], int), f"Invalid type for id: {row['id']}"
        assert isinstance(row["name"], str), f"Invalid type for name: {row['name']}"
        assert row["age"] is None or isinstance(row["age"], int), f"Invalid type for age: {row['age']}"
        assert isinstance(row["profession"], str), f"Invalid type for profession: {row['profession']}"

