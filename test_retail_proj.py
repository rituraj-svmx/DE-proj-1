from pyspark.sql.functions import *
from lib.Datareader import read_orders,read_customers
from lib.Utils import get_or_create_session
from lib.Datamanipulation import filter_close_orders,count_state,filter_dataframe_based_on_status
from lib.ConfigReader import get_app_config
import pytest

def test_count_orders(spark):
    orders_count = read_orders(spark, "LOCAL").count()
    assert orders_count == 68884

@pytest.mark.transformation()
def test_count_customers(spark):
    customers_count = read_customers(spark, "LOCAL").count()
    assert customers_count == 12435

@pytest.mark.transformation()
def test_filter_order_count(spark):
    orders_df = read_orders(spark, "LOCAL")
    closed_order_count = filter_close_orders(orders_df).count()
    assert closed_order_count == 7556

def test_app_config():
    config = get_app_config("LOCAL")
    assert config["orders.file.path"] == "data/orders.csv"

@pytest.mark.skip("WIP")
def test_verify_result(spark, expected_count):
    customers_df = read_customers(spark, "LOCAL")
    actual_count_df = count_state(customers_df)
    assert actual_count_df.collect() == expected_count.collect()

@pytest.mark.skip()
def test_count(spark):
    orders_df = read_orders(spark, "LOCAL")
    actual_count = filter_dataframe_based_on_status(orders_df, "CLOSED").count()
    assert actual_count == 7556

@pytest.mark.skip()
def test_count(spark):
    orders_df = read_orders(spark, "LOCAL")
    actual_count = filter_dataframe_based_on_status(orders_df, "PENDING_PAYMENT").count()
    assert actual_count == 15030

@pytest.mark.skip()
def test_count(spark):
    orders_df = read_orders(spark, "LOCAL")
    actual_count = filter_dataframe_based_on_status(orders_df, "COMPLETE").count()
    assert actual_count == 22900

# Parameterize the function so that we dont have to write same function 
@pytest.mark.latest()
@pytest.mark.parametrize("status,expected_count",
[("CLOSED",7556),
("PENDING_PAYMENT", 15030),
("COMPLETE", 2290)])
def test_check_status_count(spark, status, expected_count):
    orders_df = read_orders(spark, "LOCAL")
    actual_count = filter_dataframe_based_on_status(orders_df, status).count()
    assert actual_count == expected_count








