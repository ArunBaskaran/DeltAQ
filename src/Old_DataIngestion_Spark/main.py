import sys
from pyspark.sql import *
from pyspark.sql.functions import col, weekofyear, dayofyear
import pyspark.sql.functions as f
from pyspark.sql import *
from pyspark.sql.types import *
from configs import *
from aux_funcs import *


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: import_ndjson <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession.builder.appName(SESSION_NAME).config('spark.driver.extraClassPath', DRIVER_PATH).getOrCreate()

    df = read_from_json(sys.argv[1])  #convert json to dataframe
    df = schema_transformation(df) #Transform schema from old to new
    if(df == None):
            continue
    write_to_tables(df) #Write transformed dataframe to database

    spark.stop()

