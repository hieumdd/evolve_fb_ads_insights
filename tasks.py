import os
import json
import uuid

from google.cloud import tasks_v2

TABLES = [
    "AdsInsights",
    "AdsInsights_Hourly",
    "AdsInsights_AgeGenders",
    "AdsInsights_CountryRegion",
    "AdsInsights_Devices",
]

ACCOUNTS = [
    {"ad_account": "PeakGastro", "id": "act_989020241594322"},
    {"ad_account": "AFriendlyFace", "id": "act_3158907461002773"},
    {"ad_account": "AnswersNow", "id": "act_1196413044107581"},
    {"ad_account": "ChicagoENT_425021478118723", "id": "act_425021478118723"},
    {"ad_account": "ChicagoENT_1125645734548778", "id": "act_1125645734548778"},
    {"ad_account": "PainReliefInstitute", "id": "act_2806329806303956"},
    {"ad_account": "SpecialtyCareInstitute", "id": "act_505620200866621"},
    {"ad_account": "StrivePsychiatry", "id": "act_3158907461002773"},
    {"ad_account": "Merritt", "id": "act_914085879409577"},
]

TASKS_CLIENT = tasks_v2.CloudTasksClient()
CLOUD_TASKS_PATH = (
    os.getenv("PROJECT_ID"),
    "us-central1",
    "fb-ads-insights",
)
PARENT = TASKS_CLIENT.queue_path(*CLOUD_TASKS_PATH)


def create_tasks(tasks_data):
    """Create tasks and put into queue
    Args:
        tasks_data (dict): Task request
    Returns:
        dict: Job Response
    """

    payloads = [
        {
            "name": f"{account['ad_account']}-{uuid.uuid4()}",
            "payload": {
                "table": tasks_data["task"],
                "ads_account": account,
                "start": tasks_data.get("start"),
                "end": tasks_data.get("end"),
            },
        }
        for account in ACCOUNTS
    ]
    tasks = [
        {
            "name": TASKS_CLIENT.task_path(*CLOUD_TASKS_PATH, task=payload["name"]),
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": os.getenv("PUBLIC_URL"),
                "oidc_token": {
                    "service_account_email": os.getenv("GCP_SA"),
                },
                "headers": {
                    "Content-type": "application/json",
                },
                "body": json.dumps(payload["payload"]).encode(),
            },
        }
        for payload in payloads
    ]
    responses = [
        TASKS_CLIENT.create_task(
            request={
                "parent": PARENT,
                "task": task,
            }
        )
        for task in tasks
    ]
    return {
        "tasks": len(responses),
        "tasks_data": tasks_data,
    }
