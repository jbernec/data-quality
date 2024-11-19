import pytest
from pyspark.sql import SparkSession
import logging
import re
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import col

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

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





@pytest.fixture
def ivalid_pattern_test_data(spark):
    schema = StructType([
        StructField("id", IntegerType(), nullable=False),
        StructField("name", StringType(), nullable=False),
        StructField("age", IntegerType(), nullable=True),
        StructField("profession", StringType(), nullable=False)
    ])

    # Sample data including invalid characters
    data = [
        (1, "John Doe", 30, "Engineer"),           # Valid row
        (2, "Jane,Doe", 28, "Doctor"),             # Invalid character (comma) in name
        (3, "Alex/Smith", 25, "Lawyer"),           # Invalid character (slash) in name
        (4, "Alice", 40, "Scientist?"),            # Invalid character (question mark) in profession
        (5, "Mike O'Neil", 29, "Teacher"),         # Valid row
        (6, "Chris Brown", 32, "Data/Analyst"),    # Invalid character (slash) in profession
    ]

    return spark.createDataFrame(data, schema=schema)


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


# check for invalid pattern characters
# Define a regex pattern for invalid characters
INVALID_CHAR_PATTERN = r"[,/?:]" #Explicit Invalid Characters List

def contains_invalid_chars(value):
    """
    Checks if the given string contains any invalid characters based on the regex pattern.
    """
    if value is None:
        return False
    return re.search(INVALID_CHAR_PATTERN, value) is not None

def test_invalid_characters(invalid_xter_dataframe):
    """
    Test to check for invalid characters in 'name' and 'profession' columns.
    Logs any rows that contain invalid characters.
    """
    invalid_rows = []

    for idx, row in enumerate(invalid_xter_dataframe.collect()):
        name, profession = row["name"], row["profession"]

        if contains_invalid_chars(name):
            logger.error(f"Invalid character in 'name' at row {idx}: {name}")
            invalid_rows.append(row)

        if contains_invalid_chars(profession):
            logger.error(f"Invalid character in 'profession' at row {idx}: {profession}")
            invalid_rows.append(row)

    # Optionally, assert that there are no invalid rows
    assert not invalid_rows, "Data contains rows with invalid characters."



# quarantine invalid pattern data
def quarantine_invalid_rows(spark_session, invalid_rows, quarantine_path="quarantine_data/invalid_rows.parquet"):
    if invalid_rows:
        logger.info(f"Quarantining {len(invalid_rows)} invalid rows.")
        invalid_df = spark_session.createDataFrame(invalid_rows, schema=invalid_xter_dataframe.schema)
        invalid_df.write.mode("overwrite").parquet(quarantine_path)

# quarantine_invalid_rows(test_data.sparkSession, invalid_rows)


# Test function to check for null values in specified columns
@pytest.mark.parametrize("columns", [["name", "age"]])
def test_no_null_values_in_columns(sample_dataframe, columns):
    for column in columns:
        null_count = sample_dataframe.filter(col(column).isNull()).count()
        assert null_count == 0, f"Column '{column}' contains null values"


