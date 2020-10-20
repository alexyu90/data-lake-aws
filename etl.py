import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, dayofweek
from pyspark.sql.types import TimestampType


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config.get('AWS','AWS_ACCESS_KEY_ID')
os.environ['AWS_SECRET_ACCESS_KEY']=config.get('AWS','AWS_SECRET_ACCESS_KEY')

def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    '''
    This function loads song data stored in S3,
    and outputs 2 dimension tables: songs and artists.
    The output is stored in parquet files in S3.
    '''
    # get filepath to song data file
    song_data = input_data + 'song_data/*/*/*/*'

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id","title","artist_id","year","duration").drop_duplicates()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(output_data + "songs/", mode="overwrite", partitionBy=["year","artist_id"])

    # extract columns to create artists table
    artists_table = df.select("artist_id","artist_name","artist_location","artist_latitude","artist_longitude").drop_duplicates()
    
    # write artists table to parquet files
    artists_table.write.parquet(output_data + "artists/", mode="overwrite")


def process_log_data(spark, input_data, output_data):
    '''
    This function loads log and song data stored in S3,
    and outputs 2 dimension tables: users and time,
    and 1 fact table: songplays.
    The output is stored in parquet files in S3.
    '''
    # get filepath to log data file
    log_data = input_data + 'log-data/'

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    df = df.where(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select("userId","firstName","lastName","gender","level").drop_duplicates()
    
    # write users table to parquet files
    users_table.write.parquet(output_data + "users/", mode="overwrite")

    # create timestamp column from original timestamp column
    get_timestamp = udf(lambda x : datetime.utcfromtimestamp(int(x)/1000), TimestampType())
    df = df.withColumn("start_time", get_timestamp("ts"))
    
    # extract columns to create time table
    time_table = df.withColumn("hour",hour("start_time")) \
                   .withColumn("day",dayofmonth("start_time")) \
                   .withColumn("week",weekofyear("start_time")) \
                   .withColumn("month",month("start_time")) \
                   .withColumn("year",year("start_time")) \
                   .withColumn("weekday",dayofweek("start_time")) \
                   .select("ts","start_time","hour", "day", "week", "month", "year", "weekday").drop_duplicates()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(output_data + "time/", mode="overwrite", partitionBy=["year","month"])
    
    # read song data file
    song_data = input_data + 'song_data/*/*/*/*'
    df_stage = spark.read.json(song_data)
    # read in song data to use for songplays table
    song_df = df_stage.select("song_id","title","artist_id","year","duration").drop_duplicates()

    # creating two staging views to use sql query later to create songplays table
    df.createOrReplaceTempView("staging_events")
    song_df.createOrReplaceTempView("staging_songs")
    
    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
                                SELECT monotonically_increasing_id() as songplay_id,
                                       to_timestamp(e.ts/1000) as start_time, 
                                       year(to_timestamp(e.ts/1000)) as year,
                                       month(to_timestamp(e.ts/1000)) as month,
                                       e.userid as user_id, 
                                       e.level, 
                                       s.song_id, 
                                       s.artist_id, 
                                       e.sessionid as session_id, 
                                       e.location, 
                                       e.useragent as user_agent
                                FROM staging_songs s JOIN staging_events e
                                ON s.title = e.song
                                """)
    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(output_data + "songplays/", mode="overwrite", partitionBy=["year","month"])


def main():
    spark = create_spark_session()
    #input_data = 'data/'
    #output_data = 'output/'
    
    input_data = "s3://udacity-dend/"
    # reader please specify desired output bucket
    output_data = "s3://udacity-dend/test_output/" 
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
