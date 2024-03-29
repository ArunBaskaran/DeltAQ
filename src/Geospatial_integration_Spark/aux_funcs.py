#------------------------Native Python function--------------------------#

def read_from_tables():    #Load table into pyspark dataframes
    url = XXX
    properties = YYY 
    df_aqraw = spark.read.jdbc(url=url,table='usa',properties=properties)
    df_zip = spark.read.jdbc(url=url,table='zip_code',properties=properties)
    return df_aqraw, df_zip
    
def filter_df_aqraw(df):     #Clean dataframes - Drop irrelevant columns, filter column values
    df = df.select(df['country'], df['time'], df['latitude'], df['longitude'])
    df = df.filter(f.col('country') == "US")
    df = df.select(df['time'], df['latitude'], df['longitude'])
    df = df.filter(f.col('time') > ('2020-06-06 00:00:00'))
    df = df.dropDuplicates(['latitude', 'longitude'])
    return df
    
@pandas_udf('int', PandasUDFType.SCALAR)
def get_rank(v):
    return v.rank()
    
@pandas_udf('double', PandasUDFType.SCALAR)
def get_diff(v1, v2, v3, v4):
    latdiff =  v1.add(-v3).abs()
    longdiff =  v2.add(-v4).abs()
    return latdiff.sum(longdiff)
    
def crossjoin(df1, df2):       #Pyspark computation that integrates the data from the two tables
    df = df1.crossJoin(df2)
    #df = df.withColumn("dis_diff", get_diff(df.latitude, df.longitude, df.lat, df.lng)) #optimization alternative using pandas udf
    df = df.withColumn("dis_diff", (abs(df.latitude - df.lat) + abs(df.longitude - df.lng)))
    partition_list = ["latitude", "longitude"]
    w = Window.partitionBy([col(c) for c in partition_list]).orderBy(asc("dis_diff"))
    #df = df.withColumn("rank", get_rank(df['dis_diff']).over(w))   #optimization alternative using pandas udf
    df =  df.withColumn( "rank", dense_rank().over(Window.partitionBy([col(c) for c in partition_list]).orderBy(asc("dis_diff"))))  #Rank the distances between each lat-long pair in aq data and lat-long pair in zipcode data
    df = df.filter(f.col('rank') == 1)  #Select records that have rank 1, i.e., the best match between the two data
    df = df.select(df['latitude'], df['longitude'], df['zip'])
    return df

def write_to_tables(df):        #Write cross-joined dataframe to new table
    url = XXX
    properties = YYY 
    df.write.jdbc(url = url, table = 'db_name_3', mode = 'append', properties=properties)
    

