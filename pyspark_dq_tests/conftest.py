import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

@pytest.fixture(scope="session")
def spark():
    """
    Pytest fixture for creating a Spark session.
    """
    spark_session = SparkSession.builder \
        .appName("Pytest-PySpark-Testing") \
        .master("local[*]") \
        .getOrCreate()
    yield spark_session
    spark_session.stop()

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
