from lib.DataReader import read_customers, read_orders
from lib.ConfigReader import get_app_config


def test_read_customers_df(spark):
  customers_count = read_customers(spark, "LOCAL").count()
  assert customers_count == 12435


def test_read_orders_df(spark):
 orders_count = read_orders(spark, "TEST").count()
 assert orders_count == 68884

def test_read_app_config():
 config = get_app_config("TEST")
 assert config["orders.file.path"] == "data/orders.csv"
