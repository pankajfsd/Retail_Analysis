import pytest
from lib.Utils import get_spark_session

# Setup should be done as part of fixture and should not be going in a test case.

@pytest.fixture
def spark():
    "Create spark session"
    spark_session=get_spark_session(env="LOCAL")
    yield spark_session

    spark_session.stop()

@pytest.fixture
def expected_results(spark):
    "Gives the expected results"

    results_schema="state string, count int"
    return spark.read \
    .format("csv") \
    .schema(results_schema) \
    .load("data/test_result/state_aggregate.csv")