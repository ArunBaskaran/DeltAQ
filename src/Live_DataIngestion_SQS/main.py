import boto3
import json
import os
from pyspark.sql import *
from pyspark.sql.types import *
import pyspark.sql.functions as f
from pyspark.sql.functions import *
from configs import *
from aux_funcs import *


if __name__ == "__main__":
    s3 = boto3.client('s3', region_name='us-east-1')
    sqs = boto3.client('sqs', region_name='us-east-1')
    os.popen(START_SPARK)
    
    spark = SparkSession.builder.appName(SESSION_NAME).config('spark.driver.extraClassPath', DRIVER_PATH).getOrCreate()

    flag = 0
    while(1):
        flag += 1
        response = sqs.receive_message(QueueUrl = QUEUE_URL)
        if(len(response)<2):  #len(response) = 2 when an unread message is on the queue
            break
        name = download_from_s3(response)   #get the name of new file that has been uploaded to s3
        df = read_from_json(name)   #convert json to dataframe
        df = schema_transformation(df)    #Transform schema from old to new
        if(df == None):
            continue
        write_to_tables(df)         #Write transformed dataframe to database
        receipt_handle = response['Messages'][0]['ReceiptHandle']
        sqs.delete_message(QueueUrl=QUEUE_URL, ReceiptHandle=receipt_handle) #Delete message after file data has been loaded onto table
        if os.path.exists(name):
            os.remove(name)

    spark.stop()
    os.popen(STOP_SPARK)
