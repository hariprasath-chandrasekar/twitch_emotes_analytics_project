#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun 28 23:28:16 2025

@author: hari14
"""

# ================== Twitch Chat Downloader Script ==================
# Run with: caffeinate -i python download_chats.py
# ===================================================================

import pandas as pd
import os
import boto3
import subprocess
from datetime import datetime

# === Get user input ===
channel_name = input("Enter Twitch Channel Name: ").strip()
chats_year = input("Enter the year of chat messages: ").strip()
print(f"📺 Channel: {channel_name}, 📆 Year: {chats_year}")

# === Load and sort VOD dataset ===
vod_csv_path = f"/Users/hari14/Desktop/PHD/twitch_emotes_analytics_project/data/videos_on_demand_urls/{channel_name}_vods.csv"
vod_links = pd.read_csv(vod_csv_path)
vod_links["created_at"] = pd.to_datetime(vod_links["created_at"])
vod_links = vod_links.sort_values(by="created_at", ascending=True)

# === Filter for selected year and keep id + title ===
vod_links_filtered = vod_links[vod_links["created_at"].dt.year == int(chats_year)][["id", "title"]].copy()
print(f"🎥 Found {len(vod_links_filtered)} VODs for year {chats_year}")

# === AWS S3 setup ===
s3 = boto3.client("s3")
bucket_name = 'twitch-emotes-analytics-project'

# === Local path for JSONs ===
download_path = "/Users/hari14/Desktop/PHD/twitch_emotes_analytics_project/data/raw_bronze/week5/"
print(f"\n📂 Chat JSONs will be saved temporarily to:\n{download_path}\n")

# === Track failures ===
failed_downloads = []
failed_uploads = []

# === Start download loop ===
for file_number, row in enumerate(vod_links_filtered.itertuples()):
    vod = row.id
    title = row.title
    print(f"\n▶️ Downloading file {file_number+1}/{len(vod_links_filtered)} | VOD: {vod}")

    url = f"https://www.twitch.tv/videos/{vod}"

    # === Sanitize title for filename (alphanumeric + safe chars only) ===
    safe_title = "".join(c if c.isalnum() or c in "._-" else "_" for c in title)[:50]

    file_name = f"{channel_name}_{chats_year}_{file_number:03d}_{vod}.json"
    file_path = os.path.join(download_path, file_name)
    s3_key = f"data/raw_bronze/{channel_name}/{chats_year}/{file_name}"

    # === Download chat JSON ===
    try:
        subprocess.run(["chat_downloader", url, "--output", file_path], check=True)
        print("✅ Chat downloaded")
    except subprocess.CalledProcessError as e:
        print(f"❌ Download failed for {vod}: {e}")
        failed_downloads.append((vod, title))
        continue

    # === Upload to S3 ===
    try:
        s3.upload_file(file_path, bucket_name, s3_key)
        print(f"🪣 Uploaded to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"❌ Upload failed for {vod}: {e}")
        failed_uploads.append((vod, title))
        continue

    # === Delete local file ===
    try:
        os.remove(file_path)
        print("🗑️ Local file deleted")
    except Exception as e:
        print(f"⚠️ Could not delete local file: {e}")

# === Final summary ===
print("\n=========== SUMMARY ===========")
print(f"✅ Successful downloads: {len(vod_links_filtered) - len(failed_downloads)}")
print(f"❌ Failed downloads: {len(failed_downloads)}")
print(f"❌ Failed uploads: {len(failed_uploads)}")
if failed_downloads:
    print("\nFailed Downloads:")
    for vod, title in failed_downloads:
        print(f"  - VOD: {vod}, Title: {title}")
if failed_uploads:
    print("\nFailed Uploads:")
    for vod, title in failed_uploads:
        print(f"  - VOD: {vod}, Title: {title}")
print("================================\n")
