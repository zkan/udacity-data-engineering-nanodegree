import argparse

from pyspark.sql import SparkSession


def process_data(spark, input_loc, output_loc):
    """
    - Processes the input data in CSV
    - Writes the table to S3 in CSV
    """
    df = spark.read.option("header", True).csv(input_loc)

    df.createOrReplaceTempView("global_land_temperature_df")

    spark.sql("select * from global_land_temperature_df").write.mode(
        "overwrite"
    ).csv(output_loc)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input", type=str, help="HDFS input", default="/global_temperature_data"
    )
    parser.add_argument(
        "--output", type=str, help="HDFS output", default="/global_temperature_output"
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName(
        "Global Temperature Data Processor"
    ).getOrCreate()

    process_data(spark, input_loc=args.input, output_loc=args.output)
