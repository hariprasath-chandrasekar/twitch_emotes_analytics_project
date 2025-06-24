# import required libraries/packages
import pandas as pd
import os
import boto3
import subprocess
import time
from datetime import datetime

# # set working directory
# set_working_directory = "/Users/hari14/Desktop/PHD_ESCAPE/twitch_emotes_analytics_project"
# os.chdir(set_working_directory)
# print(f"Current working directory: {os.getcwd()}")

# get inputs from user
channel_name = input("Enter Twitch Channel Name:")
chats_year = input("Enter the year of chat messages:")
print(channel_name)
print(chats_year)

# load video links csv for esl_dota2
vod_links = pd.read_csv(f"/Users/hari14/Desktop/PHD_ESCAPE/twitch_emotes_analytics_project/data/raw_bronze/week5/{channel_name}.csv")
vod_links["created_at"] = pd.to_datetime(vod_links["created_at"])
vods = list(vod_links[vod_links["created_at"].dt.year == int(chats_year)]["id"])
print(vods)
print(len(vods))

# AWS s3 set up 
s3 = boto3.client("s3")
bucket_name = 'twitch-emotes-analytics-project'
print(s3.list_objects_v2(Bucket = bucket_name))

# set up local download folder
download_path = "/Users/hari14/Desktop/PHD_ESCAPE/twitch_emotes_analytics_project/data/raw_bronze/week5/"
print(f"""
      ===========================================================
      Json files will be downloaded to this path: 
      {download_path}
      ===========================================================
      """)

# chat downloads
for file_number, vod in enumerate(vods):
    print(f"Starting download for file_number {file_number+1}/{len(vods)} with the vod {vod}")
    url = f"https://www.twitch.tv/videos/{vod}"
    
    #define file paths
    file_name = f"{channel_name}_{chats_year}_{file_number}.json"
    file_path = download_path + file_name 
    s3_key = f"data/raw/{channel_name}/{chats_year}/{file_name}"
    
    # download json file
    try:
        subprocess.run(["chat_downloader", url, "--output", file_path], check = True)
        print("Download: ====================== Successful ===================")
    except subprocess.CalledProcessError as e:
        print(f"Download >>>>>>>>>>>>>>>>> Failed {e} <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<")
        continue
    
    # upload to s3
    try:
        s3.upload_file(file_path, bucket_name, s3_key)
        print(f"file {file_number} : {vod} uploaded to s3")
    except:
        print(f"file {file_number} : {vod} upload to s3 failed")
        continue
    
    # delete the local file after upload
    try:
        os.remove(file_path)
        print ("<<<<<<<<<<<<<============== Local file removed=============>>>>>>>>>>>>>>>")
    except:
        print ("<<<<<<<<<<<<<============== Local file delete failed=============>>>>>>>>>>>>>>>")
    
    
    












# output_file_name = f"/Users/hari14/Desktop/PHD_ESCAPE/twitch_emotes_analytics_project/data/videos_on_demand_urls/{channel_name}_{chats_year}_urls.csv"
# print(output_file_name)
