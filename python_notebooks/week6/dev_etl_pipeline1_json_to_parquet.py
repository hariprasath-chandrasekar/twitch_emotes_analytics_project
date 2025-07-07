#!/usr/bin/env python
# coding: utf-8

# In[1]:


# start pyspark session for data processing
from pyspark.sql import SparkSession
spark = SparkSession.builder \
                     .appName("twitch_emotes_analytics_project") \
                     .master("local[*]") \
                     .getOrCreate()
from pyspark.sql.functions import col, explode, to_timestamp, expr, lit 
import glob
import shutil

# Get inputs from user
channel_name = input("Please Enter the Channel Name: ")
chat_year = input("Please Enter the Chat Year: ")

# set local file path for download
local_path = "/Users/hari14/Desktop/PHD_ESCAPE/twitch_emotes_analytics_project/data/processed_silver/"
output_local_path = "/Users/hari14/Desktop/PHD_ESCAPE/twitch_emotes_analytics_project/data/processed_silver/"
# set up s3 connection using boto3
import boto3
import os

# s3 config 
bucket = "twitch-emotes-analytics-project"
s3_key = f"data/raw/{channel_name}/{chat_year}/"
# print("s3 key:", s3_key)

# initialize boto3 client
s3 = boto3.client("s3")

# list all objects in prefix/s3_key
response = s3.list_objects_v2(Bucket = bucket, Prefix = s3_key)
# contents = response["Contents"]
contents = response.get("Contents", [])
if not contents:
    raise ValueError(f"No files found at S3 path: {s3_key}")
json_files = [i["Key"] for i in contents]
# print(json_files)
# print(len(json_files))
def order_files():
    try:
        return sorted(json_files, key=lambda x: int(x.split("_")[4].split(".")[0]))
    except (IndexError, ValueError) as e:
        raise ValueError(f"Filename parsing failed: {e}") 
sorted_json_files = order_files()
# sorted_json_files

for i, key in enumerate(sorted_json_files):
    file_name = os.path.basename(key)
    local_file_path = os.path.join(local_path, file_name)
    temp_processed_path = os.path.join(output_local_path, "temp_processed")
    try: 
        s3.download_file(Bucket = bucket, Key = key, Filename = local_file_path)
        print(f"file {i+1}:{file_name} downloaded and being processed")
        df = spark.read.format("json") \
                  .option("multiLine",True) \
                  .load(local_file_path)
        df_filtered = df.filter(col("emotes").isNotNull()) 
        df_exploded = df_filtered.withColumn("emote",explode("emotes"))
        df_badges = df_exploded.withColumn("i_badge_names", expr("transform(author.badges, x -> x.name)")) \
                               .withColumn("i_badge_titles", expr("transform(author.badges, x -> x.title)")) \
                               .withColumn("i_badge_versions", expr("transform(author.badges, x -> x.version)"))
        df_panel = df_badges.select(
            # User Info
            col("author.id").cast("long").alias("i_user_id"),
            col("author.name").alias("i_username"),
            col("author.colour").alias("i_display_color"),
            col("i_badge_names"),      # array<string>
            col("i_badge_titles"),     # array<string>
            col("i_badge_versions").cast("array<string>"),  # convert from array<long> to array<string>
            lit(None).cast("string").alias("i_user_status"),  # placeholder
    
            # Streamer Info
            lit(channel_name).alias("j_streamer"),
    
            # Emote Info
            col("emote.name").alias("k_emote_name"),
    
            # Time Info
            to_timestamp((col("timestamp") / 1_000_000).cast("timestamp")).alias("t_timestamp"),
            col("time_text").alias("t_time_text"),
            col("time_in_seconds").cast("long").alias("t_seconds"))
        # save csv file locally
        df_panel.coalesce(1).write.mode("overwrite").parquet(temp_processed_path)
        # define file paths to upload to s3
        parquet_files = glob.glob(os.path.join(temp_processed_path, "part-*.parquet"))
        if not parquet_files:
            raise FileNotFoundError("No parquet file found in temp_processed folder")
        parquet_file_path = parquet_files[0]
        s3_output_key = f"data/processed/{channel_name}/{chat_year}/{file_name.replace('.json', '.parquet')}"
        # upload to S3
        s3.upload_file(parquet_file_path, bucket, s3_output_key)
        print(f"✅ Uploaded: s3://{bucket}/{s3_output_key}")  
        print(f"✅ Local files removed. <<<<<<<< Processing file {i+1}/{len(sorted_json_files)} complete >>>>>>>>")
    except Exception as e:
        print(f"Error processing file {file_name} : {e}")
    finally:
        # remove the json file and parquet file folder
        if os.path.exists(temp_processed_path):
            shutil.rmtree(temp_processed_path)
        if os.path.exists(local_file_path):
            os.remove(local_file_path)
        print(f"✅ Local files removed for{file_name} <<<<<<<< Processing file {i+2}/{len(sorted_json_files)} complete >>>>>>>>")

