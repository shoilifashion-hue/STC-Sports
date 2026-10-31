import asyncio
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import httpx

GITHUB_TOKEN = os.getenv("GH_PAT") or os.getenv("GITHUB_TOKEN") or os.getenv("PAT")
GITHUB_REPO = os.getenv("GH_REPO", "shoilifashion-hue/STC-Sports")
EVENT_TYPE = os.getenv("EVENT_TYPE", "source_updated")
GITHUB_BRANCH = os.getenv("GH_BRANCH", "main")

BASE_URL = "https://strmfree.st"
ALT_BASE = "https://streamfree.top"
STREAM_API = "https://strmfree.st/streams"

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/111.0.0.0 Safari/537.36"
)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("MONITOR")

@dataclass(slots=True)
class RawEvent:
    title: str
    start_dt: datetime

def determine_status(start_dt: datetime, early_minutes: int = 15) -> str:
    now_utc = datetime.now(timezone.utc)
    start_utc = start_dt.astimezone(timezone.utc)
    live_start = start_utc - timedelta(minutes=early_minutes)

    if now_utc < live_start:
        return "UPCOMING"
    return "LIVE"

async def trigger_github_workflow(client: httpx.AsyncClient, reason: str = "") -> bool:
    if not GITHUB_TOKEN or not GITHUB_REPO:
        log.error("GitHub Token or Repo not configured!")
        return False

    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": UA,
    }

    triggered = False

    # ১. repository_dispatch
    try:
        dispatch_url = f"https://api.github.com/repos/{GITHUB_REPO}/dispatches"
        payload = {
            "event_type": EVENT_TYPE,
            "client_payload": {
                "triggered_at": datetime.now(timezone.utc).isoformat(),
                "reason": reason or "State changed or match went live",
            },
        }
        resp = await client.post(dispatch_url, json=payload, headers=headers, timeout=12.0)
        if resp.status_code == 204:
            log.info(f">>> SUCCESS: Triggered via repository_dispatch! ({reason})")
            triggered = True
    except Exception as e:
        log.warning(f"repository_dispatch error: {e}")

    # ২. workflow_dispatch
    try:
        wf_list_url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows"
        wf_resp = await client.get(wf_list_url, headers=headers, timeout=12.0)
        if wf_resp.status_code == 200:
            for wf in wf_resp.json().get("workflows", []):
                if wf.get("state") == "active":
                    trigger_url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/workflows/{wf.get('id')}/dispatches"
                    run_resp = await client.post(trigger_url, json={"ref": GITHUB_BRANCH}, headers=headers, timeout=12.0)
                    if run_resp.status_code in (204, 200):
                        log.info(f">>> SUCCESS: Triggered workflow '{wf.get('name')}' via workflow_dispatch!")
                        triggered = True
    except Exception as e:
        log.warning(f"workflow_dispatch error: {e}")

    return triggered

async def get_raw_events(client: httpx.AsyncClient) -> list[RawEvent]:
    events = []
    headers = {
        "User-Agent": UA,
        "Referer": f"{ALT_BASE}/",
        "Origin": ALT_BASE,
        "Accept": "application/json, text/plain, */*"
    }

    try:
        resp = await client.get(STREAM_API, headers=headers, timeout=15.0)
        if resp.status_code == 200:
            data = resp.json()
            streams_dict = data.get("streams", {})
            if isinstance(streams_dict, dict):
                for cat, match_list in streams_dict.items():
                    if isinstance(match_list, list):
                        for m in match_list:
                            name = m.get("name")
                            ts = m.get("match_timestamp") or 0
                            dt = datetime.fromtimestamp(ts, tz=timezone.utc) if ts else datetime.now(timezone.utc)
                            if name:
                                events.append(RawEvent(title=name, start_dt=dt))
    except Exception as e:
        log.warning(f"Watcher fetch error: {e}")

    return events

async def check_and_monitor():
    log.info("24/7 Monitor started for StreamFree. Checking every 5 minutes...")
    last_state = None

    async with httpx.AsyncClient(headers={"User-Agent": UA}, follow_redirects=True, timeout=15.0) as client:
        while True:
            try:
                raw_events = await get_raw_events(client)
                if raw_events:
                    state_list = []
                    live_matches = []
                    upcoming_matches = []

                    for ev in raw_events:
                        status = determine_status(ev.start_dt, early_minutes=15)
                        if status == "LIVE":
                            live_matches.append(ev.title)
                        elif status == "UPCOMING":
                            upcoming_matches.append(ev.title)

                        state_list.append(f"{ev.title}_{status}")

                    state_list.sort()
                    new_state = "|".join(state_list)

                    log.info(f"Summary -> Total: {len(raw_events)}, LIVE: {len(live_matches)}, UPCOMING: {len(upcoming_matches)}")

                    if last_state is None:
                        last_state = new_state
                        log.info("Monitor initialized. Waiting for status transitions...")
                    elif last_state != new_state:
                        log.info("CHANGE DETECTED! Triggering GitHub Actions...")
                        success = await trigger_github_workflow(
                            client,
                            reason=f"Status update: {len(live_matches)} LIVE, {len(upcoming_matches)} UPCOMING",
                        )
                        if success:
                            last_state = new_state
                    else:
                        log.info("No status change. Sleeping for 5 minutes...")
                else:
                    log.warning("No events found from StreamFree.")
            except Exception as e:
                log.error(f"Unexpected monitor error: {e}")

            await asyncio.sleep(300)

async def run_render_health_server():
    port = int(os.getenv("PORT", 8080))

    async def handle_client(reader, writer):
        try:
            await reader.read(1024)
            response = b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK"
            writer.write(response)
            await writer.drain()
        except Exception:
            pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    server = await asyncio.start_server(handle_client, "0.0.0.0", port)
    log.info(f"Render health server listening on port {port}")
    async with server:
        await server.serve_forever()

async def main():
    await asyncio.gather(
        check_and_monitor(),
        run_render_health_server(),
    )

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Monitor service stopped.")
