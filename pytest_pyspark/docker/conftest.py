import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import findspark
findspark.init()

# setup spark session

@pytest.fixture(scope="session")
def spark():
    """
    Pytest fixture for creating a Spark session.
    """
    spark = SparkSession.builder \
        .appName("Pytest-PySpark-Testing") \
        .getOrCreate()
    return spark

@pytest.fixture
def actual_spark_dataframe(spark):
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
def actual_schema(spark):
    """
    Pytest fixture for creating a sample PySpark DataFrame with 6 rows and 4 columns.
    """
    data = [
        (1, "Alice", 25, "Engineer"),
        (2, "Bob", 30, "Doctor"),
        (3, "Charlie", 65, "Artist"),
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

    df = spark.createDataFrame(data, schema)
    return df.schema


@pytest.fixture(params=["data/data.csv"])
def data_source_dataframe(spark, request):
    """
    Pytest fixture for creating a sample PySpark DataFrame from a file source.
    """
    # access the file path passed as a parameter using request.param
    source_path = request.param

    # define the schema of the DataFrame
    schema = StructType([
        StructField("id", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("age", IntegerType(), nullable=True),
        StructField("profession", StringType(), nullable=False)
    ])

    # read the source file into a DataFrame using the specified schema
    df = spark.read.format("csv").schema(schema).load(source_path)
    return df

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


# Sample DataFrame fixture
@pytest.fixture
def invalid_xter_dataframe(spark):
    data = [
        (1, "Alice", "Good123"),
        (2, "Bob", "Valid_text"),
        (3, "Charlie", "Invalid#Text"),
        (4, "David", "No@Specials")
    ]
    columns = ["id", "name", "comment"]
    return spark.createDataFrame(data, columns)
