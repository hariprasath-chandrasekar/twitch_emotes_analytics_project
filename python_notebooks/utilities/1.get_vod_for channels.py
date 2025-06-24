# import required libraries/packages
import pandas as pd
import os
import boto3
import subprocess
import time

# # set working directory
# set_working_directory = "/Users/hari14/Desktop/PHD_ESCAPE/twitch_emotes_analytics_project"
# os.chdir(set_working_directory)
# print(f"Current working directory: {os.getcwd()}")

channel_name = input("Enter Twitch Channel Name:")
output_file_name = f"/Users/hari14/Desktop/PHD_ESCAPE/twitch_emotes_analytics_project/data/videos_on_demand_urls/{channel_name}_urls.csv"
print(output_file_name)

