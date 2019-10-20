import configparser
from datetime import datetime
import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format
from pyspark.sql import functions as F
from pyspark.sql import types as T

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark):#, input_data, output_data):
    # get filepath to song data file
    song_data = 's3a://udacity-dend/song-data/*/*/*/*.json'
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(["song_id", "title", "artist_id", "year", "duration"])
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode('overwrite').partitionBy("year", "artist_id").parquet("data/song_data/song.parquet")

    # extract columns to create artists table
    artists_table = df.select(["artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude"])
    
    # write artists table to parquet files
    artists_table.write.mode('overwrite').parquet("data/song_data/artist.parquet")


def process_log_data(spark):#, input_data, output_data):
    # get filepath to log data file
    log_data = 's3a://udacity-dend/log-data/*.json'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select(["userId", "firstName", "lastName", "gender", "level"])
    
    # write users table to parquet files
    users_table.write.mode('overwrite').parquet("data/log_data/users.parquet")

    # create timestamp column from original timestamp column
    get_timestamp = F.udf(lambda ts: datetime.datetime.fromtimestamp((ts/1000.0)), T.TimestampType())
    df = df.withColumn("Timestamp", get_timestamp("ts"))
    
    # create datetime column from original timestamp column
    get_datetime = F.udf(lambda ts: datetime.datetime.fromtimestamp((ts/1000.0)), T.DateType())
    df = df.withColumn("Datetime", get_datetime("ts"))
    
    # extract columns to create time table
    time_table = df.select('Timestamp', hour('Timestamp').alias('hour'), dayofmonth('Timestamp').alias('day'), weekofyear('Timestamp').alias('week'), month('Timestamp').alias('month'), year('Timestamp').alias('year'), )
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy("year", "month").parquet("data/log_data/time.parquet")

    # read in song data to use for songplays table
    song_df = spark.read.json('s3a://udacity-dend/song_data/*/*/*/*.json')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = df.alias('a').join(song_df.alias('b'),col('b.title') == col('a.song')).select([col('a.Timestamp').alias('start_time'), col('a.userId'), col('a.level'), col('a.sessionId'),col('a.location'), col('a.userAgent')] + [col('b.song_id'),col('b.artist_id')])
    
    songplays_table.withColumn('songplay_id', F.monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.repartition(year("start_time"), month("start_time")).write.mode('overwrite').partitionBy("start_time").parquet("data/log_data/plays.parquet")


def main():
    spark = create_spark_session()
    #input_data = "s3a://udacity-dend/"
    #output_data = ""
    
   # process_song_data(spark, input_data, output_data)    
   # process_log_data(spark, input_data, output_data)
    process_song_data(spark)
    process_log_data(spark)

    
    
if __name__ == "__main__":
    main()
