import os
import threading
import requests
import time
from datetime import datetime
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

CACHE_TTL = 180  # cache for 3 minutes 
queue_cache = {"data": None, "expires_at": 0}
_cache_lock = threading.Lock()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
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

            projects.extend(data.get("data", []))
            page=data.get("pagination", {}).get("next")

        elif response.status_code==400:
            reply_fn(f"Bad request: {response.json()}")
        else:
            reply_fn(f"Unexpected error: {response.status_code}, {response.json()}")

        projects.extend(data.get("data", []))
        page=data.get("pagination", {}).get("next")
        print(f"Fetched {len(data.get('data', []))} projects from page {page}. Total so far: {len(projects)}.")
        print(f"Next page: {page}")
        print(len(projects))

    reply_fn(f"Fetched {len(projects)} projects with status '{status}'.")
    return projects

def get_queue_message(reply_fn, logger):
    pass # to be implemented

@app.command("/forge")
def handle_some_command(ack, respond, command, logger):
    ack()
    logger.info(f"Queue called by {command['user_name']}({command['user_id']}) in channel {command['channel_name']}({command['channel_id']}) using command {command['command']}")

    projects = fetch_projects_by_status("pending", respond, logger)
    respond(str(len(projects)))

@app.event("app_mention")
def handle_bot_ping(event, say, logger):
    logger.info(f"Queue called by {event["user"]} in channel {event['channel']} using app mention")

    thread_timestamp = event.get("ts")
    def reply_in_thread(text):
        say(text, thread_ts=thread_timestamp)

    projects = fetch_projects_by_status("pending", reply_in_thread, logger)
    say(str(len(projects)))

if __name__ == "__main__":
    SocketModeHandler(app, app_token).start()
