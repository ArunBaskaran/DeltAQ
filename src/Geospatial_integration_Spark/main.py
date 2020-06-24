import os
from pyspark.sql import *
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from aux_funcs import *
from configs import *

if __name__ == "__main__":
    
    os.environ['PYSPARK_SUBMIT_ARGS'] = PYSPARK_CONFIGS

    spark = SparkSession.builder.appName(SESSION_NAME).config('spark.driver.extraClassPath', DRIVER_PATH).getOrCreate()

    df_aqraw, df_zip = read_from_tables()
    
    df_aqraw = filter_df_aqraw(df_aqraw)
    
    df_integrated = crossjoin(df_aqraw, df_zip)
    
    spark.stop()
