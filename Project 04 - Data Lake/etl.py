import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_date
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, dayofweek, monotonically_increasing_id
from pyspark.sql.types import StructType as R, StructField as Fld, DoubleType as Dbl,StringType as Str, IntegerType as Int, DateType as Date, TimestampType as TS

config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    """ 
    Create and return a Spark Session 
    """
    
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    
    return spark


def process_song_data(spark, input_data, output_data):
    """
        Load the songs datasets in json format from s3, make some processing and extract
    the fields needs to populate the songs and artists dimensions tables
    """
    
    # get filepath to song data file
    song_data = f"{input_data}/song-data/*/*/*/*.json"
    
    # defining song_data schema
    songData_schema = R([
        Fld("num_songs", Int()),
        Fld("artist_id", Str()),
        Fld("artist_latitude", Dbl()),
        Fld("artist_longitude", Dbl()),
        Fld("artist_location", Str()),
        Fld("artist_name", Str()),
        Fld("song_id", Str()),
        Fld("title", Str()),
        Fld("duration", Dbl()),
        Fld("year", Int()),
    ])
    
    # read song data file
    df = spark.read.json(song_data, schema = songData_schema).dropDuplicates()

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").parquet(f"{output_data}/songs/")

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")
    
    # write artists table to parquet files
    artists_table.write.parquet(f"{output_data}/artists/")


def process_log_data(spark, input_data, output_data):
    """
        Load the logs datasets in json format from s3, make some processing and extract
    the fields needs to populate the songplays fact table and dimensions tables about users and time
    """
    # get filepath to log data file
    log_data = f"{input_data}/log-data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data).dropDuplicates()
    
    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level")
    
    # write users table to parquet files
    users_table.write.parquet(f"{output_data}/users/")

    # create start_time column from original timestamp column
    get_timestamp = udf(lambda x: datetime.fromtimestamp(x / 1000), TS())
    df = df.withColumn("start_time", get_timestamp(col("ts")))
   
    # extract columns to create time table
    df = df.withColumn("hour", hour("start_time"))
    df = df.withColumn("day", dayofmonth("start_time"))
    df = df.withColumn("month", month("start_time"))
    df = df.withColumn("year", year("start_time"))
    df = df.withColumn("week", weekofyear("start_time"))
    df = df.withColumn("weekday", dayofweek("start_time"))
    
    time_table = df.select("start_time", "hour", "day", "month", "year", "week", "weekday")
    
    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").parquet(f"{output_data}/time/")

    # read in song data to use for songplays table
    df_songs = spark.read.parquet(f"{output_data}/songs/")
    
    df_songs = df_songs.select('song_id', 'title', 'duration')

    df_artists = spark.read.parquet(f"{output_data}/artists/")

    songs_df = df.join(df_songs, ([df.song == df_songs.title,
                              df.length == df_songs.duration]), "inner")

    songs_df = songs_df.join(df_artists, (songs_df.artist == df_artists.artist_name), "inner")

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = songs_df.select("year", "month", "start_time", "userId", "level", 
                                      "song_id", "artist_id", "sessionId", "location", "userAgent")
    
    songplays_table = songplays_table.withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").parquet(f"{output_data}/songplays/")


def main():
    """
        Main function to execute the extract and load data in S3 files in parquet format using Spark
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend"
    output_data = "s3a://sparkify-data-lake-udacity"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
