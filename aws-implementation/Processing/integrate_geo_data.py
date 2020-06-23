import os
from pyspark.sql import *
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.functions import *

def read_from_tables():
    url = XXX
    properties = YYY 
    df_aqraw = spark.read.jdbc(url=url,table='usa',properties=properties)
    df_zip = spark.read.jdbc(url=url,table='zip_code',properties=properties)
    return df_aqraw, df_zip
    
def filter_df_aqraw(df):
    df = df.select(df['country'], df['time'], df['latitude'], df['longitude'])
    df = df.filter(f.col('country') == "US")
    df = df.select(df['time'], df['latitude'], df['longitude'])
    df = df.filter(f.col('time') > ('2020-06-06 00:00:00'))
    df = df.dropDuplicates(['latitude', 'longitude'])
    return df
    
def crossjoin(df1, df2):
    df = df1.crossJoin(df2)
    df = df.withColumn("dis_diff", (abs(df.latitude - df.lat) + abs(df.longitude - df.lng)))
    partition_list = ["latitude", "longitude"]
    df =  df.withColumn( "rank", dense_rank().over(Window.partitionBy([col(c) for c in partition_list]).orderBy(asc("dis_diff"))))
    df = df.filter(f.col('rank') == 1)
    df = df.select(df['latitude'], df['longitude'], df['zip'])
    return df

def write_to_tables(df):
    url = XXX
    properties = YYY 
    df.write.jdbc(url = url, table = 'db_name_3', mode = 'append', properties=properties)
    

if __name__ == "__main__":
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--driver-class-path /tmp/jars/postgresql-42.2.13.jar --jars /tmp/jars/postgresql-42.2.13.jar pyspark-shell'

    spark = SparkSession\
            .builder\
            .appName("Downloading from local files")\
            .config('spark.driver.extraClassPath', '/usr/local/spark/jars/postgresql-42.2.13.jar')\
            .getOrCreate()
        

    df_aqraw, df_zip = read_from_tables()
    df_aqraw = filter_df_aqraw(df_aqraw)
    df_integrated = crossjoin(df_aqraw, df_zip)
    spark.stop()
