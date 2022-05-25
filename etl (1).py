import configparser
from datetime import datetime
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS_SECRET_ACCESS_KEY']


def create_spark_session():
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(spark, input_data, output_data):
    # https://knowledge.udacity.com/questions/433205
    
    # get filepath to song data file
    song_data = os.path.join(input_data, 'song_data/*/*/*/*.json')
    
    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select(['song_id', 'title', 'artist_id', 'year', 'duration']).distinct()
    
    # write songs table to parquet files partitioned by year and artist
    songs_table.write.mode("overwrite").partitionBy('year', 'artist_id').parquet(path=output_data + 'songs')

    # extract columns to create artists table
    artists_table = df.select('artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude').distinct()
    
    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(path=output_data + 'artists')
    
    # create a temp view of the songs df table
    # https://knowledge.udacity.com/questions/697035
    df.createOrReplaceTempView("songs_df_table")

def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = os.path.join(input_data + 'log_data/*/*/*.json')

    # read log data file
    df = spark.read.json(log_data)
    
    # filter by actions for song plays
    # Reference: https://knowledge.udacity.com/questions/475642
    df = df.where(df['page'] == 'NextSong')

    # extract columns for users table    
    users_table = df.select('userId', 'firstName', 'lastName', 'gender', 'level').distinct()
    
    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(path=output_data + 'users')

    # create timestamp column from original timestamp column
    # https://knowledge.udacity.com/questions/67777     
    get_timestamp = udf(lambda x: x / 1000, TimestampType())
    df = df.withColumn("timestamp", get_timestamp(df.ts))
    
    # create datetime column from original timestamp column
    get_datetime = udf(lambda x: datetime.fromtimestamp(x), TimestampType())
    df = df.withColumn("start_time", get_datetime(df.timestamp))
    
    # extract columns to create time table
    # https://knowledge.udacity.com/questions/537253
    time_table = df.withColumn("hour", hour("start_time")) \
        .withColumn("day", dayofmonth("start_time")) \
        .withColumn("week", weekofyear("start_time")) \
        .withColumn("month", month("start_time")) \
        .withColumn("year", year("start_time")) \
        .withColumn("weekday", dayofweek("start_time"))
    
    time_table = df.select('start_time', 'hour', 'day', 'week', 'month', 'year', 'weekday').distinct()
    
    # write time table to parquet files partitioned by year and month
    time_table.write.mode('overwrite').partitionBy('year', 'month').parquet(path=output_data + 'time')

    # read in song data to use for songplays table
    songs_df = spark.sql("SELECT DISTINCT * FROM songs_df_table")
    

    # extract columns from joined song and log datasets to create songplays table
    # https://knowledge.udacity.com/questions/458957     
    songplays_table = df.join(songs_df, (df.artist == songs_df.artist_name, "inner").distinct().select('start_time', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location', 'userAgent', df['year'].alias('year'), df['month'].alias('month')).withColumn("songplay_id", monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    # https://knowledge.udacity.com/questions/481917
    songplays_table.write.mode("overwrite").partitionBy('year', 'month').parquet(path=output_data + 'songplays')


def main():
    
    """1. Creates a Spark session
       2. Runs the functions defined above"""
    
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    
    # Reference for output_data: https://knowledge.udacity.com/questions/201575
    output_data = "s3a://dend/analytics"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
