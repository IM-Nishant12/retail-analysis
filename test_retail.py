#this code is for pytest, to test the df counts

import pytest
from lib.Utils import get_spark_session
from lib.DataReader import read_customers, read_orders
from lib.DataManipulation import filter_closed_orders, count_orders_state
from lib.ConfigReader import get_app_config

#if you want to run a particular test cases, just use its marker in this command:
#python -m pytest -'markername', but before running, just mention that markername inside pytest.ini

@pytest.mark.transformation()
def test_read_customers_df(spark):
    customers_count = read_customers(spark, "LOCAL").count()
    assert customers_count == 12435


@pytest.mark.transformation()
def test_read_orders_df(spark):
    orders_count = read_orders(spark, "LOCAL").count()
    assert orders_count == 68884

#the below parametrize one is to test more than 1 test cases at once which are generic, mentioned in def test_check_count_df
@pytest.mark.parametrize(
"entry1,count",
[("CLOSED", 7556),
("PENDING_PAYMENT", 15030),
("COMPLETE", 22899)])


@pytest.mark.skip("work in progress")
def test_check_count_df(spark,entry1,count):
    orders_df = read_orders(spark, "LOCAL")
    filtered_count = filter_orders_generic(orders_df,entry1).count()
    assert filtered_count == count


@pytest.mark.transformation()
def test_filter_closed_orders(spark):
    orders_df = read_orders(spark, "LOCAL")
    filtered_count = filter_closed_orders(orders_df).count()
    assert filtered_count == 7556


@pytest.mark.slow()
def test_read_app_config():
    config = get_app_config("LOCAL")
    assert config["orders.file.path"] == "data/orders.csv"


@pytest.mark.skip("work in process")  #skip is an inbuilt function inside markers
def test_count_orders_state(spark,expected_results):
    customers_df = read_customers(spark, "LOCAL")
    actual_results = count_orders_state(customers_df)
    assert actual_results.collect() == expected_results.collect()