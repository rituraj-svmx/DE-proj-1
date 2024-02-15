import sys
from lib import Datareader, Datamanipulation, Utils
from pyspark.sql.functions import *
from lib.logger import Log4j

if __name__== '__main__':
    
    # if len(sys.argv) < 2:
    #     print("Please specify the environment")
    #     sys.exit(-1)

    # job_run_env = sys.argv[2]

    # print("creating spark session")

    
    #spark = Utils.get_or_create_session(job_run_env)

    print("Creating Spark session")

    spark = Utils.get_or_create_session("LOCAL")

    logger = Log4j(spark)

    logger.info("Spark Session created succesfully!!")

    logger.info("creating dataframes")

    customers_df = Datareader.read_customers(spark, "LOCAL")

    orders_df = Datareader.read_orders(spark, "LOCAL")

    logger.info("filtering orders dataframe")

    orders_filtered_df = Datamanipulation.filter_close_orders(orders_df)

    logger.info("joining orders and customers dataframes")

    joined_df = Datamanipulation.join_orders_customers(orders_filtered_df, customers_df)

    logger.info("counting the state")

    Datamanipulation.count_state(joined_df).show()

    logger.info("end of main")



