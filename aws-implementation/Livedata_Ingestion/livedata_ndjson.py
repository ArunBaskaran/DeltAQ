import boto3
import json
import os
from pyspark.sql import *
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.functions import *
    
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
    url = XXX
    properties = YYY 
    df = spark.read.json(filename)
    return df 
    
def schema_transformation(df):      #Transform raw data from a nested structure to a linear structure
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

#----------------------------main-----------------------------#

if __name__ == "__main__":
    s3 = boto3.client('s3', region_name='us-east-1')
    sqs = boto3.client('sqs', region_name='us-east-1')
    queue_url = XXXX 

    os.popen('sh /usr/local/spark/sbin/start-all.sh')

    spark = SparkSession\
            .builder\
            .appName("Ingestion of live data")\
            .config('spark.driver.extraClassPath', '/usr/local/spark/jars/postgresql-42.2.13.jar')\
            .getOrCreate()

    flag = 0
    while(1):
        flag += 1
        response = sqs.receive_message(QueueUrl = queue_url)
        if(len(response)<2):  #len(response) = 2 when an unread message is on the queue
            break
        name = download_from_s3(response)
        df = read_from_json(name)
        df = schema_transformation(df)
        write_to_tables(df)
        receipt_handle = response['Messages'][0]['ReceiptHandle']
        sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=receipt_handle) #Delete message after file data has been loaded onto table
        if os.path.exists(name):
            os.remove(name)

    spark.stop()
    os.popen('sh /usr/local/spark/sbin/stop-all.sh')
