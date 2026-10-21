import asyncio
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import httpx
from selectolax.lexbor import LexborHTMLParser as HTMLParser

GITHUB_TOKEN = os.getenv("GH_PAT")
GITHUB_REPO = os.getenv("GH_REPO", "shoilifashion-hue/STC-Sports")
EVENT_TYPE = os.getenv("EVENT_TYPE", "source_updated")

BASE_URL = "https://streamecenter.live"
UA = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36"

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")
log = logging.getLogger("MONITOR")

@dataclass(slots=True)
class RawEvent:
    sport: str
    title: str
    start_dt: datetime
    sources_count: int

def parse_event_time(value: str) -> datetime | None:
    try:
        value = value.strip()
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        dt = datetime.fromisoformat(value)
        return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
    except Exception:
        return None

def determine_status(start_dt: datetime, early_minutes: int = 15) -> str:
    now_utc = datetime.now(timezone.utc)
    start_utc = start_dt.astimezone(timezone.utc)
    if (start_utc - timedelta(minutes=early_minutes)) <= now_utc <= (start_utc + timedelta(hours=3)):
        return "LIVE"
    elif now_utc < (start_utc - timedelta(minutes=early_minutes)):
        return "UPCOMING"
    return "ENDED"

async def trigger_github_workflow(client: httpx.AsyncClient) -> bool:
    if not GITHUB_TOKEN or not GITHUB_REPO:
        log.warning("GH_PAT or GH_REPO not found. Cannot trigger GitHub Actions.")
        return False

    url = f"https://api.github.com/repos/{GITHUB_REPO}/dispatches"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": UA,
    }
    payload = {
        "event_type": EVENT_TYPE,
        "client_payload": {"timestamp": datetime.now(timezone.utc).isoformat()}
    }

    try:
        resp = await client.post(url, json=payload, headers=headers, timeout=15.0)
        if resp.status_code == 204:
            log.info(">>> SUCCESS: Triggered GitHub Action successfully!")
            return True
        else:
            log.error(f"Failed to trigger GitHub: {resp.status_code} - {resp.text}")
            return False
    except Exception as e:
        log.error(f"GitHub Dispatch Error: {e}")
        return False

async def get_raw_events(client: httpx.AsyncClient) -> list[RawEvent]:
    events = []
    try:
        resp = await client.get(urljoin(BASE_URL, "game-cards/embed"), timeout=15.0)
        if resp.status_code != 200:
            return events
        soup = HTMLParser(resp.content)
        now_utc = datetime.now(timezone.utc)

        for card in soup.css(".game-card-group"):
            sport_elem = card.css_first("h2")
            sport = sport_elem.text(strip=True) if sport_elem else "Live"

            for game in card.css(".game-card-row"):
                name_elem = game.css_first("h3")
                time_elem = game.css_first(".game-card-when > time")
                if not name_elem or not time_elem:
                    continue

                event_dt = parse_event_time(time_elem.attributes.get("datetime", ""))
                if not event_dt:
                    continue

                event_dt_utc = event_dt.astimezone(timezone.utc)
                if event_dt_utc < now_utc - timedelta(hours=3) or event_dt_utc > now_utc + timedelta(hours=24):
                    continue

                sources_count = len(game.css(".game-card-source > a.game-card-open-link"))
                if sources_count > 0:
                    events.append(RawEvent(sport=sport, title=name_elem.text(strip=True), start_dt=event_dt, sources_count=sources_count))
    except Exception as e:
        log.error(f"Error fetching source: {e}")
    return events

async def check_and_monitor():
    log.info("24/7 Monitor started. Checking source every 5 minutes...")
    last_state = ""

    async with httpx.AsyncClient(headers={"User-Agent": UA}, follow_redirects=True, timeout=15.0) as client:
        while True:
            try:
                log.info("Checking source for new events or live updates...")
                raw_events = await get_raw_events(client)
                
                if raw_events:
                    state_list = [f"{ev.title}_{determine_status(ev.start_dt, 15)}_{ev.sources_count}" for ev in raw_events]
                    state_list.sort()
                    new_state = "|".join(state_list)

                    if last_state != new_state:
                        log.info(f"State changed! Total events: {len(raw_events)}. Sending trigger to GitHub...")
                        success = await trigger_github_workflow(client)
                        if success or not GITHUB_TOKEN:
                            last_state = new_state
                    else:
                        log.info("No status change detected. Sleeping for 5 minutes...")
                else:
                    log.warning("No events found from source.")
            except Exception as e:
                log.error(f"Monitor error: {e}")

            await asyncio.sleep(300)

async def run_render_health_server():
    port = int(os.getenv("PORT", 8080))
    async def handle_client(reader, writer):
        await reader.read(1024)
        writer.write(b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 2\r\n\r\nOK")
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(handle_client, "0.0.0.0", port)
    log.info(f"Health check listening on port {port}")
    async with server:
        await server.serve_forever()

async def main():
    await asyncio.gather(
        check_and_monitor(),
        run_render_health_server(),
    )

if __name__ == "__main__":
    asyncio.run(main())
