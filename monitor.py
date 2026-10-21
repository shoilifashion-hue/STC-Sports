import asyncio
import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qsl, urljoin, urlsplit
from zoneinfo import ZoneInfo

import httpx
from selectolax.lexbor import LexborHTMLParser as HTMLParser

# --- গিটহাব ক্রেডেনশিয়াল (Render Environment Variables থেকে আসবে) ---
GITHUB_TOKEN = os.getenv("GH_PAT")  # GitHub Personal Access Token
GITHUB_REPO = os.getenv("GH_REPO")  # যেমন: "username/repository-name"
EVENT_TYPE = os.getenv("EVENT_TYPE", "source_updated")

# রিপোজিটরির রুট ডিরেক্টরি পাথ
ROOT_DIR = Path(__file__).resolve().parent.parent

BASE_URL = "https://streamecenter.live"
ALT_BASE = "https://streame.center"

TAG = "STRMCNTR"

# প্লেলিস্ট এবং ক্যাশে স্টেট পাথ
JSON_OUTPUT = ROOT_DIR / "strmcntr.json"
M3U_OUTPUT = ROOT_DIR / "strmcntr.m3u"
CACHE_DIR = ROOT_DIR / "caches"
STATE_FILE = CACHE_DIR / "state.txt"

LOG_DIR = ROOT_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
CACHE_DIR.mkdir(parents=True, exist_ok=True)

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0"
)

HTTP_SEMAPHORE = asyncio.Semaphore(10)
BST_TZ = ZoneInfo("Asia/Dhaka")

LOG_FMT = (
    "[%(asctime)s] "
    "%(levelname)-8s "
    "[%(name)s] "
    "%(message)-70s "
    "(%(filename)s:%(lineno)d)"
)

log = logging.getLogger(TAG)
log.setLevel(logging.INFO)

if not log.handlers:
    formatter = logging.Formatter(LOG_FMT, datefmt="%Y-%m-%d | %H:%M:%S")
    file_handler = logging.FileHandler(LOG_DIR / "fetch.log", encoding="utf-8")
    console_handler = logging.StreamHandler()

    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)

    log.addHandler(file_handler)
    log.addHandler(console_handler)
    log.propagate = False


@dataclass(slots=True)
class SourceLink:
    name: str
    url: str


@dataclass(slots=True)
class RawEvent:
    sport: str
    title: str
    start_dt: datetime
    sources: list[SourceLink]
    logo: str | None = None


def cleanup(s: str) -> str:
    return "".join(i for i in s.split("—")[-1] if i.isascii()).strip()


def fix_sport(s: str) -> str:
    splits = s.split()
    if not splits:
        return ""
    first = splits[0]
    return (
        f"{first.upper() if len(first) < 5 else first.capitalize()} "
        f"{' '.join(x.capitalize() for x in splits[1:])}"
    ).strip()


def create_slug(text: str) -> str:
    text = text.lower().strip()
    return re.sub(r"[\W_]+", "-", text).strip("-")


def parse_event_time(value: str) -> datetime | None:
    try:
        value = value.strip()
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"

        dt = datetime.fromisoformat(value)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)

        return dt
    except (ValueError, TypeError):
        return None


def parse_teams(title: str) -> dict:
    delimiters = [" vs. ", " vs ", " - ", " @ "]
    for d in delimiters:
        if d in title.lower():
            parts = re.split(re.escape(d), title, maxsplit=1, flags=re.IGNORECASE)
            if len(parts) == 2:
                return {
                    "team_a": {"name": parts[0].strip()},
                    "team_b": {"name": parts[1].strip()},
                }
    return {
        "team_a": {"name": title.strip()},
        "team_b": {"name": title.strip()},
    }


# খেলা শুরু হওয়ার ১৫ মিনিট আগে থেকে LIVE স্ট্যাটাস নির্ধারণ
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


async def request(
    client: httpx.AsyncClient,
    url: str,
    url_num: int | None = None,
) -> httpx.Response | None:
    try:
        response = await client.get(url)
        response.raise_for_status()
        return response
    except Exception as e:
        msg = f'URL {url_num}) Failed to fetch "{url}": {e}' if url_num else f'Failed to fetch "{url}": {e}'
        log.error(msg)
        return None


async def trigger_github_workflow(client: httpx.AsyncClient) -> bool:
    """গিটহাব রিপোজিটরিতে repository_dispatch ইভেন্ট পাঠিয়ে অ্যাকশন ট্রিগার করবে"""
    if not GITHUB_TOKEN or not GITHUB_REPO:
        log.warning("GH_PAT or GH_REPO environment variable not set. GitHub Action trigger skipped.")
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
            "reason": "State change or match went live",
        },
    }

    try:
        resp = await client.post(url, json=payload, headers=headers, timeout=15.0)
        if resp.status_code == 204:
            log.info(">>> SUCCESS: Triggered GitHub Action workflow!")
            return True
        else:
            log.error(f"Failed to trigger GitHub Action: {resp.status_code} - {resp.text}")
            return False
    except Exception as e:
        log.error(f"Error dispatching to GitHub API: {e}")
        return False


async def get_raw_events(client: httpx.AsyncClient) -> list[RawEvent]:
    events: list[RawEvent] = []
    html_data = await request(client, urljoin(BASE_URL, "game-cards/embed"))

    if not html_data:
        return events

    soup = HTMLParser(html_data.content)
    now_utc = datetime.now(timezone.utc)

    for card in soup.css(".game-card-group"):
        sport_elem = card.css_first("h2")
        if not sport_elem:
            continue
        sport = cleanup(sport_elem.text(strip=True))

        for game in card.css(".game-card-row"):
            name_elem = game.css_first("h3")
            if not name_elem:
                continue

            event_time_elem = game.css_first(".game-card-when > time")
            if not event_time_elem:
                continue

            event_time = event_time_elem.attributes.get("datetime")
            if not event_time:
                continue

            event_dt = parse_event_time(event_time)
            if not event_dt:
                continue

            event_dt_utc = event_dt.astimezone(timezone.utc)

            # যে খেলা ৩ ঘণ্টার বেশি আগে শেষ হয়েছে শুধু সেগুলো বাদ
            if event_dt_utc < now_utc - timedelta(hours=3):
                continue

            # পরবর্তী ২৪ ঘণ্টার বেশি দূরের ম্যাচ বাদ
            if event_dt_utc > now_utc + timedelta(hours=24):
                continue

            event_name = name_elem.text(strip=True)

            event_logo = None
            img_elem = game.css_first("img") or card.css_first("img")
            if img_elem:
                raw_src = (
                    img_elem.attributes.get("src")
                    or img_elem.attributes.get("data-src")
                    or img_elem.attributes.get("data-lazy-src")
                    or img_elem.attributes.get("data-original")
                )
                if raw_src and not raw_src.startswith("data:"):
                    event_logo = urljoin(BASE_URL, raw_src)

            sources: list[SourceLink] = []
            for source in game.css(".game-card-source > a.game-card-open-link"):
                href = source.attributes.get("href")
                if not href:
                    continue
                lang = source.text(strip=True) or "Default"
                sources.append(
                    SourceLink(
                        name=lang,
                        url=urljoin(BASE_URL, href),
                    )
                )

            if sources:
                events.append(
                    RawEvent(
                        sport=fix_sport(sport),
                        title=event_name,
                        start_dt=event_dt,
                        sources=sources,
                        logo=event_logo,
                    )
                )

    return events


async def check_and_monitor() -> None:
    """২৪/৭ প্রতি ৫ মিনিট পর পর চেক করার মূল লুপ"""
    log.info("Starting 24/7 background monitor service (checking every 5 minutes)...")

    last_state = STATE_FILE.read_text(encoding="utf-8").strip() if STATE_FILE.exists() else ""

    async with httpx.AsyncClient(
        headers={
            "User-Agent": UA,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        },
        follow_redirects=True,
        timeout=15.0,
        http2=True,
    ) as client:
        while True:
            try:
                log.info("Checking source for updates...")
                raw_events = await get_raw_events(client)

                if raw_events:
                    # প্রতিটি খেলার টাইটেল + স্ট্যাটাস (১৫ মিনিট আগে লাইভ সহ) + সোর্স সংখ্যা
                    current_state_list = []
                    for ev in raw_events:
                        st = determine_status(ev.start_dt, early_minutes=15)
                        current_state_list.append(f"{ev.title}_{st}_{len(ev.sources)}")

                    # সর্ট করা হয়েছে যাতে ক্রম পরিবর্তনের কারণে ভুল সিগন্যাল না যায়
                    current_state_list.sort()
                    new_state = "|".join(current_state_list)

                    if last_state != new_state:
                        log.info(
                            f"State change detected! (Previous: {len(last_state.split('|')) if last_state else 0} items, "
                            f"Current: {len(current_state_list)} items). Sending signal to GitHub..."
                        )

                        success = await trigger_github_workflow(client)
                        if success or not GITHUB_TOKEN:
                            last_state = new_state
                            STATE_FILE.write_text(new_state, encoding="utf-8")
                    else:
                        log.info("No status change, event addition, or removal detected. Sleeping...")
                else:
                    log.warning("No events retrieved from source during this cycle.")

            except Exception as e:
                log.error(f"Error occurred during monitor cycle: {e}")

            # ৫ মিনিট (৩০০ সেকেন্ড) স্লিপ
            log.info("Waiting 5 minutes for next check...")
            await asyncio.sleep(300)


async def run_render_health_server():
    """Render Web Service হিসেবে চালালে পোর্ট বাইন্ড করে সার্ভিস সচল রাখার জন্য"""
    port = int(os.getenv("PORT", 0))
    if not port:
        return

    async def handle_client(reader, writer):
        await reader.read(1024)
        response = b"HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: 2\r\n\r\nOK"
        writer.write(response)
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(handle_client, "0.0.0.0", port)
    log.info(f"Render health check port listening on {port}")
    async with server:
        await server.serve_forever()


async def main():
    # একই সাথে ব্যাকগ্রাউন্ড মনিটর এবং রেন্ডার হেলথ পোর্ট চালু থাকবে
    await asyncio.gather(
        check_and_monitor(),
        run_render_health_server(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Monitor service stopped.")
