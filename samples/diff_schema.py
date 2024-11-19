import difflib

def test_schema_validation(sample_spark_dataframe, expected_schema):
    """
    Test to validate that the DataFrame schema matches the expected schema.
    """
    actual_schema = sample_spark_dataframe.schema

    # Check if the schemas are equal
    if actual_schema != expected_schema:
        # Convert the schemas to strings for comparison
        actual_schema_str = str(actual_schema)
        expected_schema_str = str(expected_schema)

        # Generate a diff to show the mismatch
        diff = "\n".join(
            difflib.unified_diff(
                expected_schema_str.splitlines(),
                actual_schema_str.splitlines(),
                lineterm="",
                fromfile="Expected Schema",
                tofile="Actual Schema"
            )
        )

        # Fail the test with the diff output
        pytest.fail(f"Schema mismatch detected:\n{diff}")

    # Assert to ensure the test still fails if schemas do not match
    assert actual_schema == expected_schema
