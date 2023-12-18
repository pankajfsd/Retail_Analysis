import sys
from lib import DataManipulation, DataReader, Utils
from pyspark.sql.functions import *
from lib.logger import Log4j

if __name__== '__main__':
    
    if len(sys.argv) < 2:
        print("Please specify the environment")
        sys.exit(-1)
        
    job_run_env = sys.argv[1]
    
    # print("creating spark session")
    
    spark=Utils.get_spark_session(job_run_env)

    logger = Log4j(spark)
    logger.warn("spark session created")

    
    # print("spark session created")
    
    orders_df = DataReader.read_orders(spark=spark, env=job_run_env)
    
    orders_filtered = DataManipulation.filter_closed_orders(orders_df=orders_df)
    
    customers_df = DataReader.read_customers(spark=spark, env=job_run_env)
        
    joined_df = DataManipulation.join_orders_customers(orders_df=orders_filtered, customers_df=customers_df)
    
    aggregated_results = DataManipulation.count_orders_state(joined_df=joined_df)
    
    aggregated_results.show()

    # # Cout number of states and group by states

    # count_customers_state=DataManipulation.count_customers_state(customers_df=customers_df)

    # count_customers_state.show(50)
    
    # print("end of main")
    logger.warn("end of main")