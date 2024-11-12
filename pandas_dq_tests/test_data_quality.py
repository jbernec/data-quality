import pandas as pd
import numpy as np
import pytest

@pytest.fixture
def sample_data():
    return pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 34, np.nan] # Missing age value for Charlie
    })

def test_no_missing_values(sample_data):
    assert sample_data.isnull().sum().sum() == 0, "Data contains missing values!"


# Test for unique ID column
def test_unique_id_column(sample_data):
    assert sample_data['id'].is_unique, "ID column has duplicate values!"

# Test for specific value ranges (e.g., age column)
def test_age_column_range(sample_data):
    assert sample_data['age'].between(0, 120).all(), "Age values are out of expected range!"

# Test for valid data types
def test_valid_data_types(sample_data):
    assert pd.api.types.is_integer_dtype(sample_data['age']), "Age column is not of integer type!"
    assert pd.api.types.is_string_dtype(sample_data['name']), "Name column is not of string type!"
