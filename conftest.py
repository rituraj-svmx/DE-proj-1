import pytest

from lib.Utils import get_or_create_session

@pytest.fixture
def spark():
    spark_session = get_or_create_session("LOCAL")
    yield spark_session
    spark_session.stop()

@pytest.fixture
def expected_count(spark):
    schema = "state string, count int"
    return spark.read \
        .format("csv") \
        .schema(schema) \
        .load("data/test_result/expected_state_count.csv")

