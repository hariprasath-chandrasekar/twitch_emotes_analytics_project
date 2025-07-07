# ========================= Twitch Chats Downloader Script =======================
# Run with : caffeinate -i python <filename.py>
# ================================================================================

# Import all the required packages
import pandas as pd
import os
import boto3
import subprocess
from datetime import datetime
import smtplib
from email.message import EmailMessage
import textwrap


# In[7]:


# === Get user input ===
channel_name = input("Enter Twitch Channel Name: ").strip()
chats_year_multiple = input("Enter the years of chat messages separated by a comma: ").strip()
chats_years_list = chats_year_multiple.split(",")
chats_years_list = [i.strip() for i in chats_years_list]
print(f"📺 Channel: {channel_name}, 📆 Year: {chats_years_list}")


# In[ ]:


# === Load and sort VOD dataset ===
vod_csv_path = f"/Users/hari14/Desktop/PHD/twitch_emotes_analytics_project/data/videos_on_demand_urls/{channel_name}_vods.csv"
vod_links = pd.read_csv(vod_csv_path)
vod_links["created_at"] = pd.to_datetime(vod_links["created_at"])
vod_links = vod_links.sort_values(by="created_at", ascending=True)


# In[ ]:


# === AWS S3 setup ===
s3 = boto3.client("s3")
bucket_name = 'twitch-emotes-analytics-project'


# In[6]:


# === Local path for JSONs ===
download_path = "/Users/hari14/Desktop/PHD/twitch_emotes_analytics_project/data/raw_bronze/"
print(f"\n📂 Chat JSONs will be saved temporarily to:\n{download_path}\n")


# In[ ]:


# set and define email configurations
EMAIL_ADDRESS = os.environ.get("EMAIL_USER")
EMAIL_PASSWORD = os.environ.get("EMAIL_PASS")    
def send_email(subject, body, to_email):
    msg = EmailMessage()
    msg.set_content(body)
    msg["Subject"] = subject
    msg["From"] = EMAIL_ADDRESS
    msg["To"] = to_email

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as smtp:
        smtp.login(EMAIL_ADDRESS, EMAIL_PASSWORD)
        smtp.send_message(msg)


# In[ ]:


for chats_year in chats_years_list:
    # === Filter for selected year and keep id + title ===
    vod_links_filtered = vod_links[vod_links["created_at"].dt.year == int(chats_year)][["id", "title"]].copy()
    print(f"🎥 Found {len(vod_links_filtered)} VODs for year {chats_year}")

    # === Track failures ===
    failed_downloads = []
    failed_uploads = []

    for file_number, row in enumerate(vod_links_filtered.itertuples()):
        vod = row.id
        title = row.title
        print(f"\n▶️ Downloading file {file_number+1}/{len(vod_links_filtered)} | VOD: {vod}")

        url = f"https://www.twitch.tv/videos/{vod}"

        # === File naming ===
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

    # === Per-Year Summary ===
    print(f"\n=========== SUMMARY for {chats_year} ===========")
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

    # === Send summary email ===
    failed_downloads_text = (
        chr(10).join(f"  - VOD: {vod}, Title: {title}" for vod, title in failed_downloads)
        if failed_downloads else "  None"
    )
    failed_uploads_text = (
        chr(10).join(f"  - VOD: {vod}, Title: {title}" for vod, title in failed_uploads)
        if failed_uploads else "  None"
    )

    summary = textwrap.dedent(f"""
        =========== SUMMARY for {chats_year} ===========
        ✅ Successful downloads: {len(vod_links_filtered) - len(failed_downloads)}
        ❌ Failed downloads: {len(failed_downloads)}
        ❌ Failed uploads: {len(failed_uploads)}

        Failed Downloads:
        {failed_downloads_text}

        Failed Uploads:
        {failed_uploads_text}
        ================================
    """)

    try:
        send_email(
            subject=f"📦 Twitch Chat Download Summary – {channel_name} {chats_year}",
            body=summary,
            to_email="hariprasath.cp@gmail.com"
        )
        print(f"📧 Email summary sent for {chats_year}")
    except Exception as e:
        print(f"❌ Failed to send summary email for {chats_year}: {e}")

