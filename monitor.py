import asyncio
import json
import logging
import os
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
import threading
import httpx

GH_TOKEN = os.getenv("GH_TOKEN")
GH_OWNER = os.getenv("GH_OWNER", "shiftcoin0")
GH_REPO = os.getenv("GH_REPO", "dlive-sports")
PORT = int(os.getenv("PORT", 10000))

BASE_URL = "https://streamcenter.st"
API_SPORTS = f"{BASE_URL}/api/sports"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0"
)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("Watcher")

class HealthServer(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type", "application/json")
        self.end_headers()
        response = {
            "status": "running",
            "service": "StreamCenter API Watcher",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
        self.wfile.write(json.dumps(response).encode("utf-8"))

def start_health_server():
    server = HTTPServer(("0.0.0.0", PORT), HealthServer)
    log.info(f"Health server running on port {PORT}")
    server.serve_forever()

def get_event_status(match_dt: datetime) -> str:
    now_utc = datetime.now(timezone.utc)
    start_utc = match_dt.astimezone(timezone.utc)
    live_start = start_utc - timedelta(minutes=15)
    end_time = start_utc + timedelta(hours=3)

    if live_start <= now_utc <= end_time:
        return "LIVE"
    elif now_utc < live_start:
        return "UPCOMING"
    return "ENDED"

async def trigger_github(client: httpx.AsyncClient):
    if not GH_TOKEN:
        log.error("GH_TOKEN is missing!")
        return False

    url = f"https://api.github.com/repos/{GH_OWNER}/{GH_REPO}/dispatches"
    headers = {
        "Authorization": f"Bearer {GH_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
        "User-Agent": "Render-Watcher-Bot",
        "Content-Type": "application/json"
    }
    payload = {
        "event_type": "source_updated",
        "client_payload": {
            "triggered_by": "StreamCenter Watcher",
            "timestamp": datetime.now(timezone.utc).isoformat()
        }
    }

    try:
        res = await client.post(url, headers=headers, json=payload, timeout=10.0)
        if res.status_code in [200, 204]:
            log.info(">>> GitHub Action successfully triggered! <<<")
            return True
        else:
            log.error(f"GitHub dispatch failed: {res.status_code} - {res.text}")
            return False
    except Exception as e:
        log.error(f"Error calling GitHub API: {e}")
        return False

async def watch_streamcenter():
    last_state = ""
    log.info("StreamCenter Watcher started. Polling every 5 minutes...")

    async with httpx.AsyncClient(headers={"User-Agent": UA, "Referer": f"{BASE_URL}/"}, timeout=12.0) as client:
        while True:
            try:
                sports_res = await client.get(API_SPORTS)
                if sports_res.status_code != 200:
                    log.warning(f"Failed to fetch sports: {sports_res.status_code}")
                    await asyncio.sleep(60)
                    continue

                sports_list = sports_res.json()
                current_events = []
                now_utc = datetime.now(timezone.utc)

                async def check_sport(sp):
                    sp_id = sp.get("id")
                    if not sp_id:
                        return
                    try:
                        m_res = await client.get(f"{BASE_URL}/api/matches/{sp_id}", timeout=6.0)
                        if m_res.status_code == 200:
                            for m in m_res.json():
                                title = m.get("title") or m.get("name")
                                date_ms = m.get("date")
                                sources = m.get("sources", [])
                                if title and date_ms and sources:
                                    dt = datetime.fromtimestamp(date_ms / 1000.0, tz=timezone.utc)
                                    if dt >= now_utc - timedelta(hours=3) and dt <= now_utc + timedelta(hours=18):
                                        st = get_event_status(dt)
                                        current_events.append(f"{title}|{st}|{len(sources)}")
                    except Exception:
                        pass

                await asyncio.gather(*(check_sport(sp) for sp in sports_list))

                new_state = "#".join(sorted(current_events))

                if last_state == "":
                    log.info(f"Initial run: {len(current_events)} active events. Syncing...")
                    if await trigger_github(client):
                        last_state = new_state
                elif new_state != last_state:
                    log.info("Change detected! (Event added/removed or status changed to LIVE)")
                    if await trigger_github(client):
                        last_state = new_state
                else:
                    log.info("No status change or source update. Skipping.")

            except Exception as e:
                log.error(f"Watcher loop exception: {e}")

            await asyncio.sleep(300)

def main():
    t = threading.Thread(target=start_health_server, daemon=True)
    t.start()
    asyncio.run(watch_streamcenter())

if __name__ == "__main__":
    main()
