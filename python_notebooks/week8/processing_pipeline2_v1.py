#!/usr/bin/env python
# coding: utf-8

from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws
import pandas as pd

# Step 1: Initialize Spark Session with Hadoop-AWS connector
print("🚀 Initializing Spark session...")
spark = SparkSession.builder \
    .appName("ConnectToS3") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.1") \
    .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2") \
    .getOrCreate()

# Step 2: Configure Spark to use s3a and AWS credentials
print("🔐 Configuring Spark to use AWS credentials...")
hadoop_conf = spark._jsc.hadoopConfiguration()
hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
hadoop_conf.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.profile.ProfileCredentialsProvider")
hadoop_conf.set("fs.s3a.endpoint", "s3.us-west-2.amazonaws.com") 

# Input from user
channel_name = input("Please Enter the Channel Name: ")
chat_years_input = input("Please Enter the Chat Years (comma-separated): ")
# Convert input to list of years
chat_years = [year.strip() for year in chat_years_input.split(",") if year.strip().isdigit() and len(year.strip()) == 4]
print(chat_years)

# read vod file
vod = spark.read.format("csv")\
                .option("header", True)\
                .option("inferSchema", True)\
                .load("/Users/hari14/Desktop/PHD/twitch_emotes_analytics_project/data/videos_on_demand_urls/esl_dota2_vods.csv")

# Summary storage
summary_rows = []
# Step 4: Loop through each year
for chat_year in chat_years:
    print(f"\n📦 Processing year {chat_year} for channel '{channel_name}'...")

    # Read from S3 bucket
    parquet_files_path = f"s3a://twitch-emotes-analytics-project/data/processed_silver/{channel_name}/{chat_year}/*.parquet"
    try:
        df = spark.read.parquet(parquet_files_path)
        df = df.join(vod.select("id", "title"), df["vod_id"] == vod["id"], how="left")
        df = df.drop("id")
        df = df.withColumnRenamed("title", "match_title")
        record_count = df.count()
        missing_users = df.filter((df["i_user_id"].isNull()) | (df["i_user_name"].isNull())).count()
        missing_users_pct = round((missing_users/record_count)*100,2) if record_count > 0 else 0
        summary_rows.append({
            "Year" : chat_year,
            "Total Emote Records" : record_count,
            "Records Missing User_info" : missing_users,
            "Records Missing User_info Percentage" : missing_users_pct
        })
        print(f"✅ Loaded {record_count:,} records from {parquet_files_path}")
    except Exception as e:
        print(f"❌ Failed to read data for {chat_year}: {e}")
        summary_rows.append({
            "Year" : chat_year,
            "Total Emote Records" : 0,
            "Records Missing User_info" : 0,
            "Records Missing User_info Percentage" : 0
        })
        continue

    # Flatten array columns for CSV
    print("🛠 Flattening array columns for CSV compatibility...")
    df_csv = df.withColumn("i_badge_names", concat_ws(",", "i_badge_names")) \
               .withColumn("i_badge_titles", concat_ws(",", "i_badge_titles")) \
               .withColumn("i_badge_versions", concat_ws(",", "i_badge_versions"))

    # Define output paths
    output_s3_path_parquet = f"s3a://twitch-emotes-analytics-project/data/processed_gold/{channel_name}/{chat_year}/all_data_parquet"
    output_s3_path_csv = f"s3a://twitch-emotes-analytics-project/data/processed_gold/{channel_name}/{chat_year}/all_data_csv"

    # Write Parquet output
    print(f"📤 Writing full Parquet dataset to: {output_s3_path_parquet}")
    df.repartition(8).write.mode("overwrite").parquet(output_s3_path_parquet)

    # Write CSV output
    print(f"📤 Writing CSV-friendly dataset to: {output_s3_path_csv}")
    df_csv.repartition(8).write.mode("overwrite").option("header", True).csv(output_s3_path_csv)

    print(f"✅ Finished writing all data for {chat_year} 🎉")

print("\n🏁 All requested years processed. Data exported to S3 Gold layer.")

# saving summary file
summary_df = pd.DataFrame(summary_rows)
excel_path = f"/Users/hari14/Desktop/PHD/twitch_emotes_analytics_project/data/processed_gold/{channel_name}_emote_pipeline2_summary.xlsx"
summary_df.to_excel(excel_path, index=False)
print(f"\n📊 Emote summary report saved to: {excel_path}")

# stop spark session
spark.stop()
print("Spark Session stopped successfully!")




