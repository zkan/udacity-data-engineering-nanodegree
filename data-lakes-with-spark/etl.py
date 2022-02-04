import configparser
import os

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    DoubleType,
    StringType,
    IntegerType,
)


config = configparser.ConfigParser()
config.read("dl.cfg")

os.environ["AWS_ACCESS_KEY_ID"] = config["AWS_ACCESS_KEY_ID"]
os.environ["AWS_SECRET_ACCESS_KEY"] = config["AWS_SECRET_ACCESS_KEY"]


def create_spark_session():
    spark = SparkSession.builder.config(
        "spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0"
    ).getOrCreate()

    return spark


def process_song_data(spark, input_data, output_data):
    # get filepath to song data file
    song_data = f"{input_data}/song_data/*/*/*/"

    # read song data file
    song_schema = StructType(
        [
            StructField("artist_id", StringType()),
            StructField("artist_latitude", DoubleType()),
            StructField("artist_location", StringType()),
            StructField("artist_longitude", DoubleType()),
            StructField("artist_name", StringType()),
            StructField("duration", DoubleType()),
            StructField("num_songs", IntegerType()),
            StructField("song_id", StringType()),
            StructField("title", StringType()),
            StructField("year", IntegerType()),
        ]
    )
    songs = spark.read.json(song_data, schema=song_schema)
    songs.createOrReplaceTempView("staging_songs")

    # extract columns to create songs table
    songs_table = spark.sql(
        """
        SELECT
          DISTINCT song_id AS song_id,
          title,
          artist_id,
          year,
          duration
        FROM
          staging_songs
        """
    )

    # write songs table to parquet files partitioned by year and artist
    songs_table.write.partitionBy("year", "artist_id").mode("overwrite").parquet(
        f"{output_data}/songs"
    )

    # extract columns to create artists table
    artists_table = spark.sql(
        """
        SELECT
          DISTINCT artist_id,
          artist_name,
          artist_location,
          artist_latitude,
          artist_longitude
        FROM
          staging_songs
        """
    )

    # write artists table to parquet files
    artists_table.write.mode("overwrite").parquet(f"{output_data}/artists")


def process_log_data(spark, input_data, output_data):
    # get filepath to log data file
    log_data = f"{input_data}/log_data/"

    # read log data file
    log_schema = StructType(
        [
            StructField("artist", StringType()),
            StructField("auth", StringType()),
            StructField("firstName", StringType()),
            StructField("gender", StringType()),
            StructField("itemInSession", IntegerType()),
            StructField("lastName", StringType()),
            StructField("length", DoubleType()),
            StructField("level", StringType()),
            StructField("location", StringType()),
            StructField("method", StringType()),
            StructField("page", StringType()),
            StructField("registration", DoubleType()),
            StructField("sessionId", IntegerType()),
            StructField("song", StringType()),
            StructField("status", IntegerType()),
            StructField("ts", DoubleType()),
            StructField("userAgent", StringType()),
            StructField("userId", IntegerType()),
        ]
    )
    logs = spark.read.json(log_data, schema=log_schema)
    logs = logs.withColumn("timestamp", F.to_timestamp(F.col("ts") / 1000))
    logs.createOrReplaceTempView("staging_events")

    # extract columns for users table
    users_table = spark.sql(
        """
        SELECT
          DISTINCT userid AS user_id,
          firstname AS first_name,
          lastname AS last_name,
          gender,
          level
        FROM
          staging_events
        WHERE
          page = 'NextSong'
        """
    )

    # write users table to parquet files
    users_table.write.mode("overwrite").parquet(f"{output_data}/users")

    # extract columns to create time table
    time_table = spark.sql(
        """
        SELECT
          DISTINCT timestamp AS start_time,
          EXTRACT(hour FROM timestamp) AS hour,
          EXTRACT(day FROM timestamp) AS day,
          EXTRACT(week FROM timestamp) AS week,
          EXTRACT(month FROM timestamp) AS month,
          EXTRACT(year FROM timestamp) AS year,
          EXTRACT(dayofweek FROM timestamp) AS weekday
        FROM
          staging_events
        """
    )

    # write time table to parquet files partitioned by year and month
    time_table.write.partitionBy("year", "month").mode("overwrite").parquet(
        f"{output_data}/time"
    )

    # read in song data to use for songplays table
    song_schema = StructType(
        [
            StructField("artist_id", StringType()),
            StructField("artist_latitude", DoubleType()),
            StructField("artist_location", StringType()),
            StructField("artist_longitude", DoubleType()),
            StructField("artist_name", StringType()),
            StructField("duration", DoubleType()),
            StructField("num_songs", IntegerType()),
            StructField("song_id", StringType()),
            StructField("title", StringType()),
            StructField("year", IntegerType()),
        ]
    )
    songs = spark.read.json(f"{input_data}/song_data/*/*/*/", schema=song_schema)
    songs.createOrReplaceTempView("staging_songs")

    # extract columns from joined song and log datasets to create songplays table
    songplays_table = spark.sql(
        """
        SELECT
          timestamp AS start_time,
          e.userid AS user_id,
          e.level AS level,
          s.song_id AS song_id,
          s.artist_id AS artist_id,
          e.sessionId AS session_id,
          e.location AS location,
          e.userAgent AS user_agent,
          EXTRACT(month FROM timestamp) AS month,
          EXTRACT(year FROM timestamp) AS year
        FROM
          staging_events e
        JOIN
          staging_songs s
        ON
          e.artist = s.artist_name
          AND e.song = s.title
        WHERE
          e.page = 'NextSong'
        """
    )

    # write songplays table to parquet files partitioned by year and month
    songplays_table.write.partitionBy("year", "month").mode("overwrite").parquet(
        f"{output_data}/songplays"
    )


def main():
    spark = create_spark_session()
    input_data = "s3a://udacity-dend"
    output_data = "s3a://kanouivirach-data-lake"

    process_song_data(spark, input_data, output_data)
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
