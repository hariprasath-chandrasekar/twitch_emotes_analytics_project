#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jul  1 12:42:21 2025

@author: hari14
"""

# Import all the necessary modules/packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_timestamp, expr, lit 
import boto3
import os
import glob
import shutil 

# Start pySpark session
spark = SparkSession.builder \
                    .appName("twitch_emotes_analytics_project") \
                    .master("local[*]") \
                    .getOrCreate() 
                    
# Input from user
channel_name = input("Please Enter the Channel Name: ")
chat_years_input = input("Please Enter the Chat Years (comma-separated): ")
# Convert input to list of years
chat_years = [year.strip() for year in chat_years_input.split(",") if year.strip().isdigit() and len(year.strip()) == 4]

# S3 setup
bucket = "twitch-emotes-analytics-project"
s3 = boto3.client("s3") 

# Local paths
local_path = "/Users/hari14/Desktop/PHD/twitch_emotes_analytics_project/data/processed_silver/"
output_local_path = "/Users/hari14/Desktop/PHD/twitch_emotes_analytics_project/data/processed_silver/" 

# order raw s3 files
def order_files(json_files):
    try:
        return sorted(
            json_files,
            key=lambda x: int(os.path.basename(x).replace(".json", "").split("_")[-2])
        )
    except (IndexError, ValueError) as e:
        raise ValueError(f"Filename parsing failed: {e}") 
        
# Loop through years
for chat_year in chat_years:
    s3_key_prefix = f"data/raw_bronze/{channel_name}/{chat_year}/"
    response = s3.list_objects_v2(Bucket=bucket, Prefix=s3_key_prefix)
    contents = response.get("Contents", [])
    
    if not contents:
        print(f"🚫 No files found at S3 path: {s3_key_prefix}")
        continue

    json_files = [i["Key"] for i in contents]
    sorted_json_files = order_files(json_files)

    # 🔧 Initialize counters
    files_processed = 0
    files_failed = 0
    failed_files = []

    for i, key in enumerate(sorted_json_files):
        file_name = os.path.basename(key)
        local_file_path = os.path.join(local_path, file_name)
        temp_processed_path = os.path.join(output_local_path, "temp_processed")
        file_vod_id = file_name.replace(".json", "").split("_")[-1]

        try:
            # Download
            s3.download_file(Bucket=bucket, Key=key, Filename=local_file_path)
            print(f"📥 file {i+1}: {file_name} downloaded and being processed")

            # Read JSON
            df = spark.read.format("json").option("multiLine", True).load(local_file_path)
            df_filtered = df.filter(col("emotes").isNotNull())
            df_exploded = df_filtered.withColumn("emote", explode("emotes"))

            df_badges = df_exploded.withColumn("i_badge_names", expr("transform(author.badges, x -> x.name)")) \
                                   .withColumn("i_badge_titles", expr("transform(author.badges, x -> x.title)")) \
                                   .withColumn("i_badge_versions", expr("transform(author.badges, x -> x.version)"))

            df_panel = df_badges.select(
                col("author.id").cast("long").alias("i_user_id"),
                col("author.name").alias("i_username"),
                col("author.colour").alias("i_display_color"),
                col("i_badge_names"),
                col("i_badge_titles"),
                col("i_badge_versions").cast("array<string>"),
                lit(None).cast("string").alias("i_user_status"),
                lit(channel_name).alias("j_streamer"),
                col("emote.name").alias("k_emote_name"),
                to_timestamp((col("timestamp") / 1_000_000).cast("timestamp")).alias("t_timestamp"),
                col("time_text").alias("t_time_text"),
                col("time_in_seconds").cast("long").alias("t_seconds"),
                lit(file_vod_id).alias("vod_id")
            )

            # Write parquet
            df_panel.coalesce(1).write.mode("overwrite").parquet(temp_processed_path)
            parquet_files = glob.glob(os.path.join(temp_processed_path, "part-*.parquet"))
            if not parquet_files:
                raise FileNotFoundError("No parquet file found in temp_processed folder")

            # Upload to S3
            parquet_file_path = parquet_files[0]
            s3_output_key = f"data/processed_silver/{channel_name}/{chat_year}/{file_name.replace('.json', '.parquet')}"
            s3.upload_file(parquet_file_path, bucket, s3_output_key)
            print(f"✅ Uploaded: s3://{bucket}/{s3_output_key}")
            files_processed += 1  # 🔧 increment success

        except Exception as e:
            print(f"❌ Error processing file {file_name}: {e}")
            files_failed += 1  # 🔧 increment failure
            failed_files.append(file_name)

        finally:
            # Cleanup
            if os.path.exists(temp_processed_path):
                shutil.rmtree(temp_processed_path)
            if os.path.exists(local_file_path):
                os.remove(local_file_path)
            print(f"🧹 Cleaned up local files for {file_name} (file {i+1}/{len(sorted_json_files)})")

    # 🔧 Summary for the year
    print("\n" + "="*60)
    print(f"📊 Summary for {chat_year}:")
    print(f"✅ Files processed successfully: {files_processed}")
    print(f"❌ Files failed to process: {files_failed}")
    if failed_files:
        print(f"📁 Failed files:\n - " + "\n - ".join(failed_files))
    print("="*60 + "\n")
