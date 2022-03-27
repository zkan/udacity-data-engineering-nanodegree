import argparse

from pyspark.sql import SparkSession


def process_data(spark, input_loc, output_loc):
    """
    - Processes the input data in JSON
    - Writes the table to S3 in CSV
    """
    df = spark.read.json(input_loc)

    df.createOrReplaceTempView("worldbank_country_profile_df")

    spark.sql("select fields.* from worldbank_country_profile_df").write.mode(
        "overwrite"
    ).csv(output_loc)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input", type=str, help="HDFS input", default="/worldbank_data"
    )
    parser.add_argument(
        "--output", type=str, help="HDFS output", default="/worldbank_output"
    )
    args = parser.parse_args()

    spark = SparkSession.builder.appName("World Bank Data Processor").getOrCreate()

    process_data(spark, input_loc=args.input, output_loc=args.output)
