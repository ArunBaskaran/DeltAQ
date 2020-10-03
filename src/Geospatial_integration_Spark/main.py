import os
from pyspark.sql import *
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from aux_funcs import *
from configs import *
import pandas as pd

if __name__ == "__main__":
    
    os.environ['PYSPARK_SUBMIT_ARGS'] = PYSPARK_CONFIGS

    spark = SparkSession.builder.appName(SESSION_NAME).config('spark.driver.extraClassPath', DRIVER_PATH).getOrCreate()

    df_aqraw, df_zip = read_from_tables()  #convert table to dataframe
    
    df_aqraw = filter_df_aqraw(df_aqraw)       #filter the rows needed to perform geospatial integration
    
    df_integrated = crossjoin(df_aqraw, df_zip)   #Perform crossjoin
    
    write_to_tables(df_integrated)   #Write integrated dataframe to database table
    
    spark.stop()
