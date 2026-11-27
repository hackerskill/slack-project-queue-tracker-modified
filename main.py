import os
import threading
import requests
import time
from datetime import datetime, timezone
import logging
import json
from dotenv import load_dotenv
from slack_bolt import App
from slack_bolt.adapter.socket_mode import SocketModeHandler

load_dotenv()

app_token=os.environ.get("SLACK_APP_TOKEN")
bot_token=os.environ.get("SLACK_BOT_TOKEN")
base_url=os.environ.get("BASE_URL")
endpoint=f"{base_url}/api/v1/projects"
channel_id=os.environ.get("SLACK_NOTIFICATION_CHANNEL_ID")

old_projects=None
new_project_notifications=True
queue_empty_notifications=True

app=App(token=bot_token)

CACHE_TTL=300  # cache for 5 minutes 
queue_cache={"data": None, "expires_at": 0}
cache_lock=threading.Lock()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("queue_tracker.log"),
        logging.StreamHandler()
    ]
)

def fetch_projects_by_status(status, reply_fn, logger):
    projects = []
    page = 1
    max_attempts = 3

    while page is not None:
        response = None
        for attempt in range(max_attempts):
            try:
                response = requests.get(
                    endpoint,
                    params={"status": status, "per_page": 100, "page": page},
                    timeout=10
                )
                if response.status_code == 429:
                    logger.warning(f"Rate limited (429) on {status} page {page}. Attempt {attempt + 1}/{max_attempts}.")
                    retry_after = int(response.headers.get("Retry-After", 1))
                    reply_fn(f"Rate limited by API, retrying in {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue
                else:
                    break
            except requests.exceptions.RequestException as e:
                logger.exception(f"Request failed on attempt {attempt + 1}: {e}.")

                if attempt < max_attempts - 1:
                    time.sleep(1)
                    continue
                else:
                    reply_fn(f"Request failed: {e}, try again later.")
                    return

        if response.status_code==200:
            data=response.json()
            if not data or "data" not in data:
                logger.error(f"Invalid JSON: {response.text[:200]}")
                raise RuntimeError("Server returned invalid JSON.")

        elif response.status_code==400:
            reply_fn(f"Bad request: {response.json()}")
        else:
            reply_fn(f"Unexpected error: {response.status_code}, {response.json()}")

        projects.extend(data["data"])
        page=data["pagination"]["next"]

    return projects

def fetch_unique_people(pending_projects=None):
    unique_people=set()
    for project in pending_projects:
        if "user" in project and "id" in project["user"]:
            unique_people.add(project["user"]["id"])
    return len(unique_people)

def fetch_oldest_days(pending_projects=None):
    current_date=datetime.now(timezone.utc)
    oldest_date=None

    for project in pending_projects:
        ts=project["updated_at"]

        date=datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if oldest_date is None or date < oldest_date:
            oldest_date=date

    return (current_date - oldest_date).days

def get_data(reply_fn, logger):
    with cache_lock:
        now=time.time()
        if now < queue_cache["expires_at"]:
            age=int(now - (queue_cache["expires_at"] - CACHE_TTL))//60
            if age==0:
                queue_cache["data"]["age"]="< 1 min ago"
            else:
                queue_cache["data"]["age"]=f"{age} mins ago"

            logger.info(f"Serving queue status from cache ({age}): {queue_cache['data']['pending']} pending projects")

            return queue_cache["data"]
        else:
            try:
                pending=fetch_projects_by_status("pending", reply_fn, logger)
                pending_pitches=fetch_projects_by_status("pitch_pending", reply_fn, logger)
                unique_people=fetch_unique_people(pending)
                oldest_days=fetch_oldest_days(pending)
                logger.info(f"Fetched queue status from API: {len(pending)} pending projects")
                queue_cache["data"]={
                    "pending": len(pending),
                    "unique_people": unique_people,
                    "pending_pitches": len(pending_pitches),
                    "oldest_days": oldest_days,
                    }
                queue_cache["data"]["age"]="Just now(Live)"
                queue_cache["expires_at"]=time.time() + CACHE_TTL
            except Exception as e:
                logger.exception(f"Failed to fetch queue status: {e}")
                reply_fn(f"Failed to fetch queue status: {e}")

            return queue_cache["data"]

def get_queue_message(reply_fn, logger):
    data=get_data(reply_fn, logger)

    if data is None:
        return

    message=(
        f"Projects in queue: *{data['pending']}* \n"
        f"Unique people waiting: *{data['unique_people']}*\n"
        f"Pending pitches: *{data['pending_pitches']}*\n"
        f"Oldest Waiting: *{data['oldest_days']} days*\n"
        f"Updated: *{data['age']}*\n"
    )
    reply_fn(message)
    return message

@app.command("/forge")
def handle_some_command(ack, respond, command, logger):
    ack()
    logger.info(f"Queue called by {command['user_name']}({command['user_id']}) in channel {command['channel_name']}({command['channel_id']}) using command {command['command']}")

    get_queue_message(respond, logger)

@app.event("app_mention")
def handle_bot_ping(event, say, logger):
    logger.info(f"Queue called by {event["user"]} in channel {event['channel']} using app mention")

    thread_timestamp = event.get("ts")
    def reply_in_thread(text):
        say(text, thread_ts=thread_timestamp)

    get_queue_message(reply_in_thread, logger)

if __name__ == "__main__":
    SocketModeHandler(app, app_token).start()
