# Import all the necessary modules/packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, to_timestamp, expr, lit 
import boto3
import os
import glob
import shutil
import pandas as pd

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
print(chat_years)

# S3 setup
bucket = "twitch-emotes-analytics-project"
s3 = boto3.client("s3")

# Local paths
local_path = "/Users/hari14/Desktop/PHD/twitch_emotes_analytics_project/data/processed_silver/"

# order raw s3 files
def order_files(json_files):
    try:
        return sorted(
            json_files,
            key=lambda x: int(os.path.basename(x).replace(".json", "").split("_")[-2])
        )
    except (IndexError, ValueError) as e:
        raise ValueError(f"Filename parsing failed: {e}")

# Keep track of files being processed
total_files_per_year = {}
processed_files_per_year = {}
no_emote_files_per_year = {}
missing_users_files_per_year = {}
missing_users_count_per_year = {}

# processing loop
for chat_year in chat_years:
    processed_files =[]
    no_emote_files =[]
    missing_users_files = []
    s3_key_prefix = f"data/raw_bronze/{channel_name}/{chat_year}/"
    response = s3.list_objects_v2(Bucket=bucket, Prefix=s3_key_prefix)
    contents = response.get("Contents", [])
    
    if not contents:
        print(f"🚫 No files found at S3 path: {s3_key_prefix}")
        continue

    json_files = [i["Key"] for i in contents]
    sorted_json_files = order_files(json_files)
    total_files_per_year[chat_year] = len(sorted_json_files)

    for i, key in enumerate(sorted_json_files):
        file_name = os.path.basename(key)
        local_file_path = os.path.join(local_path, file_name)
        temp_processed_path = os.path.join(local_path, "temp_processed")

        # Extract VOD ID from filename
        file_vod_id = file_name.replace(".json", "").split("_")[-1]

        try:
            # Download
            s3.download_file(Bucket=bucket, Key=key, Filename=local_file_path)
            print(f"📥 file {i+1}: {file_name} downloaded and being processed")

            # Read JSON
            df = spark.read.format("json").option("multiLine", True).load(local_file_path)
            df_filtered = df.filter(col("emotes").isNotNull())
            df_exploded = df_filtered.withColumn("emote", explode("emotes"))

            # log missing user_name or user_id files
            
            missing_users_df = df_exploded.filter(col("author.id").isNull() | col("author.name").isNull())
            missing_users_count = missing_users_df.count() 
            if missing_users_count > 0:
                missing_users_files.append(file_vod_id) 
            
            # Add columns
            df_badges = df_exploded.withColumn("i_badge_names", expr("transform(author.badges, x -> x.name)")) \
                                   .withColumn("i_badge_titles", expr("transform(author.badges, x -> x.title)")) \
                                   .withColumn("i_badge_versions", expr("transform(author.badges, x -> x.version)"))

            # Final select with VOD ID added
            df_panel = df_badges.select(
                col("author.id").cast("long").alias("i_user_id"),
                col("author.name").alias("i_user_name"),
                col("author.colour").alias("i_display_color"),
                col("i_badge_names"),
                col("i_badge_titles"),
                col("i_badge_versions").cast("array<string>"),
                lit(None).cast("string").alias("i_user_status"),
                lit(channel_name).alias("j_streamer"),
                col("emote.name").alias("k_emote_name"),
                to_timestamp((col("timestamp") / 1000000).cast("double")).alias("t_timestamp"),
                col("time_text").alias("t_time_text"),
                col("time_in_seconds").cast("long").alias("t_seconds"),
                lit(file_vod_id).alias("vod_id"),
                col("message").alias("chat_message")
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
            processed_files.append(file_vod_id)

        except Exception as e:
            no_emote_files.append(file_vod_id)
            print(f"❌ Error processing file {file_name}: {e}")

        finally:
            # Cleanup
            if os.path.exists(temp_processed_path):
                shutil.rmtree(temp_processed_path)
            if os.path.exists(local_file_path):
                os.remove(local_file_path)
            print(f"🧹 Cleaned up local files for {file_name} (file {i+1}/{len(sorted_json_files)})")
    # Save year-level data
    processed_files_per_year[chat_year] = processed_files
    no_emote_files_per_year[chat_year] = no_emote_files
    missing_users_files_per_year[chat_year] = missing_users_files
    missing_users_count_per_year[chat_year] = len(missing_users_files)

# === Generate Excel Summary Report ===

summary_rows = []

for year in chat_years:
    summary_rows.append({
        "Year": year,
        "Total S3 Files": total_files_per_year.get(year, 0),
        "Processed Files": len(processed_files_per_year.get(year, [])),
        "Unprocessed Files": len(no_emote_files_per_year.get(year, [])),
        "Missing User Files": missing_users_count_per_year.get(year, 0)
    })

# Convert to DataFrame
summary_df = pd.DataFrame(summary_rows)

# Save to Excel
excel_path = f"/Users/hari14/Desktop/PHD/twitch_emotes_analytics_project/data/processed_silver/twitch_emote_processing_summary_{channel_name}.xlsx"
summary_df.to_excel(excel_path, index=False)

print(f"\n📊 Summary Excel report saved to: {excel_path}")