from pyspark.sql import SparkSession
from pyspark.sql.functions import to_timestamp, dayofweek, monotonically_increasing_id
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    ## only taking subset of files otherwise the spark job will take a very long time (it does a S3 API call for each of the ~14,000 song files on S3)
    song_data = f"{input_data}/song_data/A/A/*"

    # read song data file
    df = spark.read.json(song_data)

    # extract columns to create songs table
    songs_table = df.select("song_id", "title", "artist_id", "year", "duration")

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.parquet(f"{output_data}/songs/", partitionBy=['year', 'artist_id'], mode='overwrite')

    # extract columns to create artists table
    artists_table = df.select("artist_id", "artist_name", "artist_location", "artist_latitude", "artist_longitude")

    # write artists table to parquet files
    artists_table.write.parquet(f"{output_data}/artists", mode='overwrite')


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = f"{input_data}/log_data/*/*/*"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df.page == "NextSong")

    # extract columns for users table    
    users_table = df.select("userId", "firstName", "lastName", "gender", "level")

    # write users table to parquet files
    users_table.write.parquet(f"{output_data}/users", mode='overwrite')

    # create timestamp column from original timestamp column
    df = df.withColumn("start_time", to_timestamp(df.ts / 1000))

    # create datetime column from original timestamp column
    df = df.withColumn("hour", hour("start_time")) \
        .withColumn("day", dayofmonth("start_time")) \
        .withColumn("week", weekofyear("start_time")) \
        .withColumn("month", month("start_time")) \
        .withColumn("year", year("start_time")) \
        .withColumn("weekday", dayofweek("start_time"))

    # extract columns to create time table
    time_table = df.select("start_time", "hour", "day", "week", "month", "year", "weekday")

    # write time table to parquet files partitioned by year and month
    time_table.write.parquet(f"{output_data}/time", partitionBy=['year', 'month'], mode='overwrite')

    # read in song data to use for songplays table
    song_df = spark.read.parquet(f"{output_data}/songs/")

    joined_df = df.join(song_df, df.song == song_df.title)

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = joined_df.select('start_time', 'userId', 'level', 'song_id', 'artist_id', 'sessionId', 'location',
                                       'userAgent', df.year, 'month').withColumn("songplay_id",
                                                                                 monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.parquet(f"{output_data}/songplays", partitionBy=['year', 'month'], mode='overwrite')


def create_spark_session():
    spark = SparkSession \
        .builder \
        .getOrCreate()
    return spark


if __name__ == "__main__":
    # for running locally
    spark = create_spark_session()
    input_data = "./data"
    output_data = "./output"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)
