import pytest
from pyspark.sql import functions as f

from ..src.process_data import create_spark_session, transform
from ..utils.utils import convert_parquet, convert_spark_df


@pytest.fixture
def get_data():
    spark = create_spark_session()
    convert_parquet("./../csv_data/employees.csv", "./../test_data/employees.parquet")
    convert_parquet(
        "./../csv_data/departments.csv", "./../test_data/departments.parquet"
    )
    emp_df = spark.read.parquet("./../test_data/employees.parquet")
    dept_df = spark.read.parquet("./../test_data/departments.parquet")
    return emp_df, dept_df


def test_count(get_data):
    emp_df, dept_df = get_data
    out_df = transform(emp_df, dept_df)
    assert out_df.count() == 1


def test_aggregation(get_data):
    emp_df, dept_df = get_data
    out_df = transform(emp_df, dept_df)
    out_df = out_df.select("aggre").agg(f.sum(f.col("aggre")))
    data = convert_spark_df(out_df)
    assert data == [(58000.00,)]

# Data type check
# Duplicate check
# Count check
# Data - transformation check
