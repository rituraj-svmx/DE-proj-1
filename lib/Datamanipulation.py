from lib import Datareader
from pyspark.sql.functions import *

def filter_close_orders(orders_df):
    return orders_df.filter("order_status == 'CLOSED'")


def join_orders_customers(orders_df, customers_df):
    return orders_df.join(customers_df, "customer_id")

def count_state(joined_df):
    return joined_df.groupby("state").count()

def filter_dataframe_based_on_status(orders_df, status):
    return orders_df.filter("order_status == '{}'".format(status))



    
