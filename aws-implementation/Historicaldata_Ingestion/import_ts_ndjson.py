import sys
from pyspark.sql import *
from pyspark.sql.functions import col, weekofyear, dayofyear
import pyspark.sql.functions as f
from pyspark.sql import *
from pyspark.sql.types import *

def read_from_json(filename):
    df = spark.read.json(filename)
    return df 
    
def schema_transformation(df):
    df = df.drop('attribution')
    df = df.withColumn("time", col('date.local').cast(TimestampType()))
    df = df.drop('date')
    df = df.withColumn("latitude", col('coordinates.latitude'))
    df = df.withColumn("longitude", col('coordinates.longitude'))
    df = df.drop('coordinates')
    df = df.withColumn("ap_units", col('averagingPeriod.unit'))
    df = df.withColumn("ap_value", col('averagingPeriod.value'))
    df = df.drop('averagingPeriod')
    df = df.withColumn("week", (weekofyear(col('time').cast(DateType())).cast(IntegerType())))
    df = df.withColumn("day", (dayofyear(col('time').cast(DateType())).cast(IntegerType())))
    return df
    
def write_to_tables(df):
    df = df.filter(df['country'] == "US")
    urlzip = XXXX
    properties = YYYY
    df1 = spark.read.jdbc(url=urlzip,table='db_name1',properties=properties)
    df = df.join(df1, (df.latitude == df1.latitude) & (df.longitude == df1.longitude))
    df2 = df.filter(df['zip'] == 85006)
    df2 = df2.select(df2['time'], df2['week'], df2['day'], df2['zip'], df2['parameter'], df2['unit'], df2['value'])
    url = XXXX
    properties = YYYY
    df2.write.jdbc(url = url, table = 'db_name2', mode = 'append', properties=properties)   
    df3 = df.select(df['time'], df['week'], df['day'], df['zip'], df['parameter'], df['unit'], df['value'])
    df3.write.jdbc(url = url, table = 'db_name3', mode = 'append', properties=properties)


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: import_ndjson <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("Import ndjson")\
        .getOrCreate()

    df = read_from_json(sys.argv[1])
    df = schema_transformation(df)
    write_to_tables(df)

    spark.stop()

