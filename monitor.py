import asyncio
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin

import httpx
from selectolax.lexbor import LexborHTMLParser as HTMLParser

# --- গিটহাব ক্রেডেনশিয়াল ---
GITHUB_TOKEN = os.getenv("GH_PAT")
GITHUB_REPO = os.getenv("GH_REPO", "shoilifashion-hue/STC-Sports")
EVENT_TYPE = os.getenv("EVENT_TYPE", "source_updated")

BASE_URL = "https://streamecenter.live"
UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/134.0.0.0 Safari/537.36"
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


# খেলা শুরুর ১৫ মিনিট আগে থেকে LIVE স্ট্যাটাস নির্ধারণ করার লজিক
def determine_status(start_dt: datetime, early_minutes: int = 15) -> str:
    now_utc = datetime.now(timezone.utc)
    start_utc = start_dt.astimezone(timezone.utc)
    live_start_utc = start_utc - timedelta(minutes=early_minutes)
    end_utc = start_utc + timedelta(hours=3)

    if live_start_utc <= now_utc <= end_utc:
        return "LIVE"
    elif now_utc < live_start_utc:
        return "UPCOMING"
    return "ENDED"


async def trigger_github_workflow(client: httpx.AsyncClient, reason: str = "") -> bool:
    """GitHub Actions চালু করার জন্য সিগন্যাল পাঠানো"""
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
        "client_payload": {
            "triggered_at": datetime.now(timezone.utc).isoformat(),
            "reason": reason or "State changed or match went live",
        },
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
        log.error(f"GitHub Dispatch API Error: {e}")
        return False


async def get_raw_events(client: httpx.AsyncClient) -> list[RawEvent]:
    """সোর্স ওয়েবসাইট থেকে বর্তমান খেলার তালিকা ফেচ করা"""
    events = []
    try:
        resp = await client.get(urljoin(BASE_URL, "game-cards/embed"), timeout=15.0)
        if resp.status_code != 200:
            return events
        soup = HTMLParser(resp.content)
        now_utc = datetime.now(timezone.utc)

        for card in soup.css(".game-card-group"):
            for game in card.css(".game-card-row"):
                name_elem = game.css_first("h3")
                time_elem = game.css_first(".game-card-when > time")
                if not name_elem or not time_elem:
                    continue

                event_dt = parse_event_time(time_elem.attributes.get("datetime", ""))
                if not event_dt:
                    continue

                event_dt_utc = event_dt.astimezone(timezone.utc)
                # ৩ ঘণ্টার বেশি পুরনো এবং ২৪ ঘণ্টার দূরের ম্যাচ বাদ
                if event_dt_utc < now_utc - timedelta(hours=3) or event_dt_utc > now_utc + timedelta(hours=24):
                    continue

                sources_count = len(game.css(".game-card-source > a.game-card-open-link"))
                if sources_count > 0:
                    events.append(
                        RawEvent(
                            title=name_elem.text(strip=True),
                            start_dt=event_dt,
                            sources_count=sources_count,
                        )
                    )
    except Exception as e:
        log.error(f"Error fetching source: {e}")
    return events


async def check_and_monitor():
    """২৪ ঘণ্টা প্রতি ৫ মিনিট পর পর চেক করার মূল লুপ"""
    log.info("24/7 Monitor started. Checking source every 5 minutes...")
    last_state = None

    async with httpx.AsyncClient(headers={"User-Agent": UA}, follow_redirects=True, timeout=15.0) as client:
        while True:
            try:
                log.info("Checking source for status updates, 15-min live events, or match changes...")
                raw_events = await get_raw_events(client)

                if raw_events:
                    # প্রতিটি ম্যাচের নাম + ১৫ মিনিট আগের লাইভ স্ট্যাটাস + সোর্স সংখ্যা নিয়ে স্টেট তৈরি
                    state_list = []
                    live_matches = []

                    for ev in raw_events:
                        status = determine_status(ev.start_dt, early_minutes=15)
                        if status == "LIVE":
                            live_matches.append(ev.title)
                        state_list.append(f"{ev.title}_{status}_{ev.sources_count}")

                    state_list.sort()
                    new_state = "|".join(state_list)

                    # ১. প্রথমবার রেন্ডার চালু হলে কেবল স্টেট মেমোরিতে রাখবে (অযথা ট্রিগার করবে না)
                    if last_state is None:
                        last_state = new_state
                        log.info(
                            f"Initialized monitor with {len(raw_events)} events "
                            f"(Currently LIVE: {len(live_matches)}). Waiting for future updates..."
                        )

                    # ২. অবস্থার পরিবর্তন হলে (ম্যাচ ১৫ মিনিট আগে LIVE হলে, যোগ বা বাদ হলে)
                    elif last_state != new_state:
                        log.info(
                            f"State change detected! (Events: {len(raw_events)}, LIVE now: {len(live_matches)}). "
                            "Sending signal to GitHub..."
                        )
                        success = await trigger_github_workflow(
                            client,
                            reason=f"Status update: {len(live_matches)} matches LIVE",
                        )
                        if success:
                            last_state = new_state
                    else:
                        log.info("No status change (no match became LIVE). Sleeping for 5 minutes...")
                else:
                    log.warning("No events found from source.")

            except Exception as e:
                log.error(f"Unexpected monitor error: {e}")

            # ৫ মিনিট অপেক্ষা করবে
            await asyncio.sleep(300)


async def run_render_health_server():
    """Render Web Service-এর পোর্ট সক্রিয় রাখার জন্য ব্যাকগ্রাউন্ড সার্ভার"""
    port = int(os.getenv("PORT", 8080))

    async def handle_client(reader, writer):
        try:
            await reader.read(1024)
            response = (
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: text/plain\r\n"
                b"Content-Length: 2\r\n"
                b"Connection: close\r\n\r\nOK"
            )
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
    log.info(f"Render health check server listening on port {port}")
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
