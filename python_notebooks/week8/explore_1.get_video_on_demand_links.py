#!/usr/bin/env python
# coding: utf-8

# In[17]:


import requests
import csv


# In[8]:


# Connection strings
CLIENT_ID = "3t6hp09w7jck16tyvkho4p2rgtd9f3"
CLIENT_SECRET = "i6vg2u4i4v631fo3nfdrui5wdxyvx7"


# In[9]:


# Getting OAuth token
def get_app_token():
    url = "https://id.twitch.tv/oauth2/token"
    params = {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "grant_type": "client_credentials"
             }
    response = requests.post(url, data=params)
    response.raise_for_status()
    return response.json()["access_token"]    
access_token = get_app_token()
print("Access token :", access_token)


# In[15]:


CHANNEL_LOGIN = "BLASTPremier"
OUTPUT_CSV = f"/Users/hari14/Desktop/PHD/twitch_emotes_analytics_project/data/videos_on_demand_urls/{CHANNEL_LOGIN}_vods.csv"
print(CHANNEL_LOGIN,OUTPUT_CSV)


# In[11]:


# --- Step 2: Get User ID ---
def get_user_id(access_token):
    headers = {
        "Client-ID": CLIENT_ID,
        "Authorization": f"Bearer {access_token}"
    }
    response = requests.get(
        "https://api.twitch.tv/helix/users",
        headers=headers,
        params={"login": CHANNEL_LOGIN}
    )
    response.raise_for_status()
    return response.json()["data"][0]["id"]


# In[12]:


# --- Step 3: Fetch All Archived VODs ---
def get_all_vods(user_id, access_token):
    vods = []
    url = "https://api.twitch.tv/helix/videos"
    headers = {
        "Client-ID": CLIENT_ID,
        "Authorization": f"Bearer {access_token}"
    }
    params = {
        "user_id": user_id,
        "type": "archive",
        "first": 100  # max per page
    }

    while True:
        response = requests.get(url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        vods.extend(data["data"])

        if "pagination" in data and "cursor" in data["pagination"]:
            params["after"] = data["pagination"]["cursor"]
        else:
            break

    return vods


# In[13]:


# --- Step 4: Save to CSV ---
def save_vods_to_csv(vods):
    with open(OUTPUT_CSV, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=["id", "title", "created_at", "duration", "url"])
        writer.writeheader()
        for v in vods:
            writer.writerow({
                "id": v["id"],
                "title": v["title"],
                "created_at": v["created_at"],
                "duration": v["duration"],
                "url": v["url"]
            })


# In[18]:


# --- MAIN EXECUTION ---
if __name__ == "__main__":
    print("🔐 Getting OAuth token...")
    token = get_app_token()

    print("🎯 Fetching user ID for channel:", CHANNEL_LOGIN)
    user_id = get_user_id(token)

    print("📺 Fetching all archived VODs...")
    vods = get_all_vods(user_id, token)
    print(f"✅ Found {len(vods)} VODs")

    print(f"💾 Saving to CSV: {OUTPUT_CSV}")
    save_vods_to_csv(vods)
    print("🎉 Done!")


# In[19]:


import pandas as pd


# In[20]:


df = pd.read_csv(OUTPUT_CSV)


# In[21]:


df.info()


# In[22]:


df


# In[24]:


df.info()


# In[ ]:




