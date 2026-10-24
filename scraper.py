import asyncio
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urljoin, urlsplit
from zoneinfo import ZoneInfo

import httpx
from selectolax.lexbor import LexborHTMLParser as HTMLParser

# রুট পাথ
ROOT_DIR = Path(__file__).resolve().parent

BASE_URL = "https://streamecenter.live"
EMBED_REFERER = "https://embedcenter.st"
TAG = "STRMCNTR"

JSON_OUTPUT = ROOT_DIR / "strmcntr.json"
M3U_OUTPUT = ROOT_DIR / "strmcntr.m3u"
CACHE_DIR = ROOT_DIR / "caches"

CACHE_DIR.mkdir(parents=True, exist_ok=True)

# আপনার পছন্দের Chrome User-Agent
STREAM_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/111.0.0.0 Safari/537.36"
)

HTTP_SEMAPHORE = asyncio.Semaphore(10)
BST_TZ = ZoneInfo("Asia/Dhaka")

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")
log = logging.getLogger(TAG)

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
    return f"{first.upper() if len(first) < 5 else first.capitalize()} {' '.join(x.capitalize() for x in splits[1:])}".strip()

def create_slug(text: str) -> str:
    return re.sub(r"[\W_]+", "-", text.lower().strip()).strip("-")

def parse_event_time(value: str) -> datetime | None:
    try:
        value = value.strip()
        if not value:
            return None
        if value.endswith("Z"):
            value = value[:-1] + "+00:00"
        dt = datetime.fromisoformat(value)
        return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt
    except Exception:
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except Exception:
            return None

def parse_teams(title: str) -> dict:
    delimiters = [" vs. ", " vs ", " - ", " @ "]
    for d in delimiters:
        if d in title.lower():
            parts = re.split(re.escape(d), title, maxsplit=1, flags=re.IGNORECASE)
            if len(parts) == 2:
                return {"team_a": {"name": parts[0].strip()}, "team_b": {"name": parts[1].strip()}}
    return {"team_a": {"name": title.strip()}, "team_b": {"name": title.strip()}}

def determine_status(start_dt: datetime, early_minutes: int = 15) -> str:
    now_utc = datetime.now(timezone.utc)
    start_utc = start_dt.astimezone(timezone.utc)
    if (start_utc - timedelta(minutes=early_minutes)) <= now_utc <= (start_utc + timedelta(hours=3)):
        return "LIVE"
    elif now_utc < (start_utc - timedelta(minutes=early_minutes)):
        return "UPCOMING"
    return "ENDED"

def find_m3u8_in_text(text: str) -> str | None:
    """JS বা HTML সোর্স কোড থেকে টোকেনসহ m3u8 লিংক খুঁজে বের করে"""
    # JSON এসকেপড স্ল্যাশ (\/) স্বাভাবিক করা
    clean_text = text.replace(r"\/", "/")
    
    # টোকেন ও কুয়েরি প্যারামিটারসহ m3u8 খোঁজার Regex
    match = re.search(r'(https?://[a-zA-Z0-9_\-\.]+(?::\d+)?/[^\s"\'<>\\]+?\.m3u8(?:\?[^\s"\'<>\\]+)?)', clean_text)
    if match:
        return match.group(1)
    return None

async def process_stream(client: httpx.AsyncClient, url: str) -> str | None:
    """ইভেন্ট পেজ এবং প্রয়োজনে ভেতরের iframe থেকে সরাসরি টোকেনসহ রিয়েল m3u8 এক্সট্রাক্ট করে"""
    async with HTTP_SEMAPHORE:
        try:
            resp = await client.get(url, headers={"Referer": BASE_URL, "User-Agent": STREAM_UA}, timeout=12.0)
            if resp.status_code != 200:
                return None

            # ১. সরাসরি পেজের ভেতর m3u8 আছে কি না চেক
            m3u8_url = find_m3u8_in_text(resp.text)
            if m3u8_url:
                return m3u8_url

            # ২. যদি পেজের ভেতর iframe থাকে
            soup = HTMLParser(resp.content)
            iframe = soup.css_first("iframe")
            if not iframe:
                return None

            src = iframe.attributes.get("src", "")
            if not src:
                return None

            iframe_url = urljoin(url, src)

            # ৩. iframe পেজ ফেচ করে ভেতরের রিয়েল স্ক্রিপ্ট থেকে টোকেনসহ m3u8 এক্সট্রাক্ট করা
            resp_iframe = await client.get(
                iframe_url,
                headers={"Referer": url, "User-Agent": STREAM_UA},
                timeout=12.0
            )
            if resp_iframe.status_code == 200:
                m3u8_url = find_m3u8_in_text(resp_iframe.text)
                if m3u8_url:
                    return m3u8_url

            return None
        except Exception:
            return None

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
            if not sport_elem:
                continue
            sport = cleanup(sport_elem.text(strip=True))

            for game in card.css(".game-card-row"):
                name_elem = game.css_first("h3")
                time_elem = game.css_first(".game-card-when > time")
                if not name_elem or not time_elem:
                    continue

                event_dt = parse_event_time(time_elem.attributes.get("datetime", ""))
                if not event_dt:
                    continue

                event_dt_utc = event_dt.astimezone(timezone.utc)
                if event_dt_utc < (now_utc - timedelta(hours=3)) or event_dt_utc > (now_utc + timedelta(hours=48)):
                    continue

                img_elem = game.css_first("img") or card.css_first("img")
                event_logo = None
                if img_elem:
                    src = img_elem.attributes.get("src") or img_elem.attributes.get("data-src")
                    if src and not src.startswith("data:"):
                        event_logo = urljoin(BASE_URL, src)

                sources = [
                    SourceLink(name=s.text(strip=True) or "Default", url=urljoin(BASE_URL, s.attributes.get("href", "")))
                    for s in game.css(".game-card-source > a.game-card-open-link")
                    if s.attributes.get("href")
                ]

                if sources:
                    events.append(RawEvent(
                        sport=fix_sport(sport),
                        title=name_elem.text(strip=True),
                        start_dt=event_dt,
                        sources=sources,
                        logo=event_logo
                    ))
                    
        log.info(f"Scraped {len(events)} raw events from source page.")
    except Exception as e:
        log.error(f"Error scraping events: {e}")
    return events

async def main():
    log.info("Starting one-time playlist generation...")
    async with httpx.AsyncClient(headers={"User-Agent": STREAM_UA}, follow_redirects=True, http2=True) as client:
        raw_events = await get_raw_events(client)
        if not raw_events:
            log.warning("No events found!")
            return

        final_playlist = []

        for ev in raw_events:
            tasks = [process_stream(client, src.url) for src in ev.sources]
            results = await asyncio.gather(*tasks)

            event_channels = []
            for src, m3u8_url in zip(ev.sources, results):
                if m3u8_url:
                    # আপনার দেওয়া ফরম্যাট অনুযায়ী হেডার পাইপলাইন তৈরি
                    raw_link = f"{m3u8_url}|User-Agent={STREAM_UA}&Referer={EMBED_REFERER}&Origin={EMBED_REFERER}"
                    
                    event_channels.append({
                        "channel_name": f"{src.name} Stream",
                        "stream_type": "HLS",
                        "url": m3u8_url,
                        "raw_link": raw_link,
                        "headers": {
                            "User-Agent": STREAM_UA,
                            "Referer": EMBED_REFERER,
                            "Origin": EMBED_REFERER
                        },
                        "drm_key": None
                    })

            start_bst = ev.start_dt.astimezone(BST_TZ)
            status = determine_status(ev.start_dt, early_minutes=15)

            if event_channels or status == "UPCOMING":
                final_playlist.append({
                    "title": ev.title,
                    "sport": ev.sport,
                    "sub_category": ev.sport,
                    "status": status,
                    "is_hot": False,
                    "slug": create_slug(ev.title),
                    "schedule": {
                        "timezone": "BST (UTC+6)",
                        "start_time_bst": start_bst.strftime("%b %d, %Y, %I:%M:%S %p"),
                        "end_time_bst": (start_bst + timedelta(hours=3)).strftime("%b %d, %Y, %I:%M:%S %p"),
                    },
                    "teams": parse_teams(ev.title),
                    "logo": ev.logo,
                    "channels": event_channels,
                    "_timestamp": ev.start_dt.timestamp()
                })

        priority = {"LIVE": 0, "UPCOMING": 1, "ENDED": 2}
        final_playlist.sort(key=lambda x: (priority.get(x["status"], 99), x["_timestamp"]))
        for item in final_playlist:
            item.pop("_timestamp", None)

        # JSON সেভ
        JSON_OUTPUT.write_text(
            json.dumps(
                {
                    "last_updated": datetime.now(BST_TZ).strftime("%b %d, %Y, %I:%M:%S %p"),
                    "event_count": len(final_playlist),
                    "events": {"events": final_playlist}
                },
                indent=2,
                ensure_ascii=False
            ),
            encoding="utf-8"
        )
        
        # M3U ফাইল সেভ (হুবহু আপনার দেওয়া ফরম্যাটে তৈরি)
        m3u_lines = ["#EXTM3U"]
        for ev in final_playlist:
            for ch in ev.get("channels", []):
                tvg_logo = ev["logo"] or ""
                # আপনার দেওয়া স্ট্যান্ডার্ড লাইন ফরম্যাট
                m3u_lines.append(f'#EXTINF:-1 tvg-id="Live.Event.us" tvg-logo="{tvg_logo}" group-title="{ev["sport"]}", {ev["title"]} | {ch["channel_name"]}')
                m3u_lines.append(f'#EXTVLCOPT:http-referrer={EMBED_REFERER}')
                m3u_lines.append(f'#EXTVLCOPT:http-user-agent={STREAM_UA}')
                m3u_lines.append(ch["raw_link"])
                
        M3U_OUTPUT.write_text("\n".join(m3u_lines) + "\n", encoding="utf-8")
        
        log.info(f"Done! Saved {len(final_playlist)} events to JSON and M3U. Exiting.")

if __name__ == "__main__":
    asyncio.run(main())
