from configs import *

#------------------------UDFs--------------------------#

def download_from_s3(response):     #Download file from s3 onto local ec2 instance
    data = json.loads(response['Messages'][0]['Body'])
    data1 = json.loads(data['Message'])
    file = data1['Records'][0]['s3']['object']['key']
    print("Downloading ", file)
    name = '/home/ubuntu/file' + str(flag) + '.ndjson'
    s3.download_file('openaq-fetches', file, name)
    return name
    
def read_from_json(filename):      #Load json files into pyspark dataframes
    df = spark.read.json(filename)
    return df 
    
def schemavalidation(df): #Schema Validation
    try:
        quinn.validate_schema(df, requiredSchema)
    except:
        return False
    return True

def schema_transformation(df):      #Transform raw data from a nested structure to a linear structure
    #Schema Validation
    if(schemavalidation(df)==False):
        return None
    #------Data Cleaning-----#
    df = df.na.drop(how='all')  
    df = df.fillna({'value':0.0})
    #------Schema Transformation----#
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
    
def write_to_tables(df):            #Write schema-transformed dataframe to Timescale table
    df = df.filter(df['country'] == "US")
    df1 = spark.read.jdbc(url=USZIP_URL,table='db_name1',properties=PROPERTIES)
    df = df.join(df1, (df.latitude == df1.latitude) & (df.longitude == df1.longitude))
    df2 = df.filter(df['zip'] == 85006)
    df2 = df2.select(df2['time'], df2['week'], df2['day'], df2['zip'], df2['parameter'], df2['unit'], df2['value'])
    df2.write.jdbc(url = PHOENIX_URL, table = 'db_name2', mode = 'append', properties=PROPERTIES)   
    df3 = df.select(df['time'], df['week'], df['day'], df['zip'], df['parameter'], df['unit'], df['value'])
    df3.write.jdbc(url = url, table = 'db_name3', mode = 'append', properties=properties)
    

