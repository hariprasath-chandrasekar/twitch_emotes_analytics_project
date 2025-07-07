# import all required modules/packages
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws

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


# In[ ]:


# Step 3: Get list of years from user
years_input = input("📅 Please enter years to process (comma-separated, e.g. 2021,2022,2023): ")
years = [year.strip() for year in years_input.split(",")]
channel_name = "esl_dota2"


# In[ ]:


vod = spark.read.format("csv")\
                .option("header", True)\
                .option("inferSchema", True)\
                .load("/Users/hari14/Desktop/PHD/twitch_emotes_analytics_project/data/videos_on_demand_urls/esl_dota2_vods.csv")


# In[ ]:


# Step 4: Loop through each year
for chat_year in years:
    print(f"\n📦 Processing year {chat_year} for channel '{channel_name}'...")

    # Read from S3 bucket
    parquet_files_path = f"s3a://twitch-emotes-analytics-project/data/processed_silver/{channel_name}/{chat_year}/*.parquet"
    try:
        df = spark.read.parquet(parquet_files_path)
        df = df.join(vod.select("id", "title"), df["vod_id"] == vod["id"], how="left")
        df = df.drop("id")
        df = df.withColumnRenamed("title", "match_title")
        record_count = df.count()
        print(f"✅ Loaded {record_count:,} records from {parquet_files_path}")
    except Exception as e:
        print(f"❌ Failed to read data for {chat_year}: {e}")
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
    df.write.mode("overwrite").parquet(output_s3_path_parquet)

    # Write CSV output
    print(f"📤 Writing CSV-friendly dataset to: {output_s3_path_csv}")
    df_csv.write.mode("overwrite").option("header", True).csv(output_s3_path_csv)

    print(f"✅ Finished writing all data for {chat_year} 🎉")

print("\n🏁 All requested years processed. Data exported to S3 Gold layer.")

