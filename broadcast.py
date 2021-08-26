import os
import json
import asyncio

import aiohttp
from google.cloud import pubsub_v1

API_VER = os.getenv("API_VER")
ACCESS_TOKEN = os.getenv("ACCESS_TOKEN")
BASE_URL = f"https://graph.facebook.com/{API_VER}/"
BUSINESS_ID = "238246140238714"


async def get_running_ads_accounts():
    connector = aiohttp.TCPConnector(limit=10)
    async with aiohttp.ClientSession(connector=connector) as session:
        ads_account_ids = await get_ads_accounts(session)
        tasks = [
            asyncio.create_task(_get_running_ads_accounts(session, ads_account_id))
            for ads_account_id in ads_account_ids
        ]
        running_ads_accounts = await asyncio.gather(*tasks)
    running_ads_accounts = [i for i in running_ads_accounts if i is not None]
    return running_ads_accounts


async def get_ads_accounts(session):
    url = f"{BASE_URL}{BUSINESS_ID}/owned_ad_accounts"
    async with session.get(
        url,
        params={
            "access_token": ACCESS_TOKEN,
            "limit": 100,
        },
    ) as r:
        res = await r.json()
    ads_account_ids = [i["id"] for i in res["data"]]
    return ads_account_ids


async def _get_running_ads_accounts(session, ads_account_id):
    url = f"{BASE_URL}{ads_account_id}/ads_volume"
    async with session.get(
        url,
        params={
            "access_token": ACCESS_TOKEN,
        },
    ) as r:
        res = await r.json()
    data = res["data"][0]
    ads_running_or_in_review_count = data["ads_running_or_in_review_count"]
    if ads_running_or_in_review_count > 0:
        return ads_account_id
    else:
        return None


def broadcast(broadcast_data):
    """Broadcast jobs to PubSub

    Args:
        broadcast_mode (str): Broadcast mode

    Returns:
        int: Number of messages
    """

    running_ads_accounts = asyncio.run(get_running_ads_accounts())
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(os.getenv("PROJECT_ID"), os.getenv("TOPIC_ID"))
    test = []

    for ads_account_id in running_ads_accounts:
        data = {
            "ads_account_id": ads_account_id,
            "start": broadcast_data.get("start"),
            "end": broadcast_data.get("end"),
            "mode": broadcast_data["broadcast"],
        }
        test.append(data)
        message_json = json.dumps(data)
        message_bytes = message_json.encode("utf-8")
        publisher.publish(topic_path, data=message_bytes).result()

    return {
        "message_sent": len(running_ads_accounts),
    }
