from configs import *

#------------------------UDFs--------------------------#

def read_from_json(filename):
    df = spark.read.json(filename)
    return df 

def schemavalidation(df): #Schema Validation
    requiredSchema = StructType([StructField("attribution", 
                                        ArrayType(StructType([StructField("element", 
                                                        StructType([StructField("name", StringType(), True), StructField("url", StringType(), True)]), True]))), True), 
                     StructField("averagingPeriod", 
                                        StructType([StructField("unit", StringType(), True), StructField("value", LongType(), True)]), True) , 
                     StructField("city", StringType(), True), 
                     StructField("coordinates", 
                                        StructType([StructField("latitude", DoubleType(), True), StructField("longitude", DoubleType(), True)]), True), 
                     StructField("country", StringType(), True), 
                     StructField("date", 
                                        StructType([StructField("local", StringType(), True), StructField("utc", StringType(), True)]), True), 
                     StructField("location", StringType(), True), 
                     StructField("mobile", BooleanType(), True), 
                     StructField("parameter", StringType(), True), 
                     StructField("sourceName", StringType(), True), 
                     StructField("sourceType", StringType(), True), 
                     StructField("unit", StringType(), True), 
                     StructField("value", DoubleType(), True)])
    try:
        quinn.validate_schema(df, requiredSchema)
    except:
        return False
    return True
    
def schema_transformation(df):  #Transform raw data from a nested structure to a linear structure
    #Schema Validation
    if(schemavalidation(df)==False):
        return None
    #------Data Cleaning-----#
    df = df.na.drop(how='all')  
    df = df.fillna({'value':0.0})
    #------Schema Transformation----#
    df = df.drop('attribution')
    # optimize using pandas udf
    df = df.withColumn("time", col('date.local').cast(TimestampType()))
    df = df.drop('date')
    # optimize using pandas udf
    df = df.withColumn("latitude", col('coordinates.latitude'))
    # optimize using pandas udf
    df = df.withColumn("longitude", col('coordinates.longitude'))
    df = df.drop('coordinates')
    df = df.withColumn("ap_units", col('averagingPeriod.unit'))
    df = df.withColumn("ap_value", col('averagingPeriod.value'))
    df = df.drop('averagingPeriod')
    # optimize using pandas udf
    df = df.withColumn("week", (weekofyear(col('time').cast(DateType())).cast(IntegerType())))
    # optimize using pandas udf
    df = df.withColumn("day", (dayofyear(col('time').cast(DateType())).cast(IntegerType())))
    return df
    
def write_to_tables(df):    #Write schema-transformed dataframe to Timescale table
    df = df.filter(df['country'] == "US")
    df1 = spark.read.jdbc(url=USZIP_URL,table='db_name1',properties=PROPERTIES)
    df = df.join(df1, (df.latitude == df1.latitude) & (df.longitude == df1.longitude))
    df2 = df.filter(df['zip'] == 85006)
    df2 = df2.select(df2['time'], df2['week'], df2['day'], df2['zip'], df2['parameter'], df2['unit'], df2['value'])
    df2.write.jdbc(url = US_URL, table = 'db_name2', mode = 'append', properties=PROPERTIES)   
    df3 = df.select(df['time'], df['week'], df['day'], df['zip'], df['parameter'], df['unit'], df['value'])
    df3.write.jdbc(url = USZIP_URL, table = 'db_name3', mode = 'append', properties=PROPERTIES)
    

