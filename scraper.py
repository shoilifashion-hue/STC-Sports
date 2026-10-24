import asyncio
import base64
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urljoin, quote
from zoneinfo import ZoneInfo

import httpx
from selectolax.lexbor import LexborHTMLParser as HTMLParser

# রুট পাথ ও কনফিগারেশন
ROOT_DIR = Path(__file__).resolve().parent

# আপনার দেওয়া নতুন ডোমেন
BASE_URL = "https://streamec.st"
EMBED_REFERER = "https://embedcenter.st"
TAG = "STRMCNTR"

JSON_OUTPUT = ROOT_DIR / "strmcntr.json"
M3U_OUTPUT = ROOT_DIR / "strmcntr.m3u"
CACHE_DIR = ROOT_DIR / "caches"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# আপনার দেওয়া Chrome User-Agent
STREAM_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/111.0.0.0 Safari/537.36"
)

HTTP_SEMAPHORE = asyncio.Semaphore(5)
BST_TZ = ZoneInfo("Asia/Dhaka")

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")
log = logging.getLogger(TAG)

@dataclass(slots=True)
class SourceGroup:
    source: str
    group_id: str
    name: str = "Stream"

@dataclass(slots=True)
class MatchEvent:
    id: str
    sport: str
    title: str
    start_dt: datetime
    sources: list[SourceGroup]
    logo: str | None = None
    is_live: bool = False

def fix_sport(s: str) -> str:
    if not s:
        return "Sports"
    s = s.replace("-", " ")
    splits = s.split()
    first = splits[0]
    return f"{first.upper() if len(first) < 5 else first.capitalize()} {' '.join(x.capitalize() for x in splits[1:])}".strip()

def create_slug(text: str) -> str:
    return re.sub(r"[\W_]+", "-", text.lower().strip()).strip("-")

def parse_teams(title: str) -> dict:
    delimiters = [" vs. ", " vs ", " - ", " @ "]
    for d in delimiters:
        if d in title.lower():
            parts = re.split(re.escape(d), title, maxsplit=1, flags=re.IGNORECASE)
            if len(parts) == 2:
                return {"team_a": {"name": parts[0].strip()}, "team_b": {"name": parts[1].strip()}}
    return {"team_a": {"name": title.strip()}, "team_b": {"name": title.strip()}}

def determine_status(start_dt: datetime, is_live_badge: bool = False) -> str:
    if is_live_badge:
        return "LIVE"
    now_utc = datetime.now(timezone.utc)
    start_utc = start_dt.astimezone(timezone.utc)
    if (start_utc - timedelta(minutes=15)) <= now_utc <= (start_utc + timedelta(hours=3)):
        return "LIVE"
    elif now_utc < (start_utc - timedelta(minutes=15)):
        return "UPCOMING"
    return "ENDED"

def extract_m3u8(text: str) -> str | None:
    """JS বা HTML থেকে কুয়েরি টোকেনসহ রিয়েল .m3u8 লিংক এক্সট্রাক্ট করে"""
    clean_text = text.replace(r"\/", "/")
    
    # ১. সরাসরি m3u8 Regex
    match = re.search(r'(https?://[a-zA-Z0-9_\-\.]+(?::\d+)?/[^\s"\'<>\\]+?\.m3u8(?:\?[^\s"\'<>\\]+)?)', clean_text)
    if match:
        return match.group(1)

    # ২. atob Base64 এনকোডেড থাকলে ডিকোড করে চেক করা
    b64_matches = re.findall(r'atob\(["\']([A-Za-z0-9+/=]{20,})["\']\)', text)
    for b64 in b64_matches:
        try:
            decoded = base64.b64decode(b64).decode("utf-8", errors="ignore")
            m = re.search(r'(https?://[a-zA-Z0-9_\-\.]+(?::\d+)?/[^\s"\'<>\\]+?\.m3u8(?:\?[^\s"\'<>\\]+)?)', decoded)
            if m:
                return m.group(1)
        except Exception:
            pass
    return None

async def resolve_m3u8_from_embed(client: httpx.AsyncClient, embed_url: str) -> str | None:
    """Embed পেজ অথবা নেস্টেড আইফ্রেম ফেচ করে আসল m3u8 খুঁজে বের করে"""
    try:
        resp = await client.get(
            embed_url,
            headers={"Referer": BASE_URL, "User-Agent": STREAM_UA},
            timeout=10.0
        )
        if resp.status_code != 200:
            return None

        m3u8 = extract_m3u8(resp.text)
        if m3u8:
            return m3u8

        # ভেতরে iframe থাকলে সেখানে ঢোকা
        soup = HTMLParser(resp.content)
        iframe = soup.css_first("iframe")
        if iframe:
            src = iframe.attributes.get("src", "")
            if src:
                inner_url = urljoin(embed_url, src)
                resp_inner = await client.get(
                    inner_url,
                    headers={"Referer": embed_url, "User-Agent": STREAM_UA},
                    timeout=10.0
                )
                if resp_inner.status_code == 200:
                    return extract_m3u8(resp_inner.text)
    except Exception:
        pass
    return None

async def get_match_streams(client: httpx.AsyncClient, source: SourceGroup) -> list[str]:
    """streamec.st এর API থেকে রিয়েল স্ট্রিম সংগ্রহ করে"""
    async with HTTP_SEMAPHORE:
        urls = []
        try:
            api_url = f"{BASE_URL}/api/stream/{quote(source.source)}/{quote(source.group_id)}"
            resp = await client.get(api_url, headers={"Referer": BASE_URL, "User-Agent": STREAM_UA}, timeout=10.0)
            if resp.status_code != 200:
                return urls

            data = resp.json()
            if not isinstance(data, list):
                return urls

            for item in data:
                if not isinstance(item, dict):
                    continue

                # সরাসরি m3u8 লিংক থাকলে
                direct = item.get("stream") or item.get("url")
                if direct and ".m3u8" in direct:
                    urls.append(direct)
                    continue

                # Embed প্লেয়ার পেজ থেকে লিংক আনা
                embed_url = item.get("embedUrl")
                if embed_url:
                    found = await resolve_m3u8_from_embed(client, embed_url)
                    if found:
                        urls.append(found)
        except Exception:
            pass
        return urls

async def fetch_all_matches(client: httpx.AsyncClient) -> list[MatchEvent]:
    """streamec.st থেকে সকল LIVE ও SCHEDULED ম্যাচ ফেচ করে"""
    all_events_map: dict[str, MatchEvent] = {}

    # ১. প্রথমে সরাসরি LIVE ম্যাচগুলো ফেচ করা
    try:
        live_resp = await client.get(f"{BASE_URL}/api/matches/live", timeout=12.0)
        if live_resp.status_code == 200:
            live_list = live_resp.json()
            if isinstance(live_list, list):
                for m in live_list:
                    match_id = str(m.get("id") or m.get("title"))
                    dt = datetime.fromtimestamp(m.get("date", 0) / 1000, tz=timezone.utc)
                    badge = m.get("badge") or m.get("poster")
                    logo = f"{BASE_URL}/api/images/badge/{badge}.webp" if badge else None

                    sources = [
                        SourceGroup(source=s.get("source"), group_id=s.get("id"), name=s.get("source", "Stream"))
                        for s in m.get("sources", [])
                        if s.get("source") and s.get("id")
                    ]

                    all_events_map[match_id] = MatchEvent(
                        id=match_id,
                        sport=fix_sport(m.get("category") or m.get("sport") or "Sports"),
                        title=m.get("title", "Live Event"),
                        start_dt=dt,
                        sources=sources,
                        logo=logo,
                        is_live=True
                    )
                log.info(f"Loaded {len(live_list)} LIVE matches from {BASE_URL}")
    except Exception as e:
        log.warning(f"Live API error: {e}")

    # ২. স্ক্রিনশটে থাকা সব ক্যাটাগরির খেলাগুলো আনা (MMA, American Football, Football, Basketball ইত্যাদি)
    try:
        # সকল স্পোর্টস আইডি লোড
        sports_resp = await client.get(f"{BASE_URL}/api/sports", timeout=10.0)
        sport_ids = []
        if sports_resp.status_code == 200:
            sports_data = sports_resp.json()
            if isinstance(sports_data, list):
                sport_ids = [sp.get("id") for sp in sports_data if sp.get("id")]

        if not sport_ids:
            # স্ক্রিনশট অনুযায়ী কমন ক্যাটাগরিগুলো ফলব্যাক
            sport_ids = ["mma", "american-football", "football", "basketball", "racing", "baseball", "hockey", "boxing"]

        # প্রতিটি ক্যাটাগরির ম্যাচ ফেচ
        for sp in sport_ids:
            try:
                resp = await client.get(f"{BASE_URL}/api/matches/{sp}", timeout=8.0)
                if resp.status_code != 200:
                    continue
                matches = resp.json()
                if not isinstance(matches, list):
                    continue

                for m in matches:
                    match_id = str(m.get("id") or m.get("title"))
                    if match_id in all_events_map:
                        continue  # লাইভ হিসেবে ইতিমধ্যে থাকলে স্কিপ

                    dt = datetime.fromtimestamp(m.get("date", 0) / 1000, tz=timezone.utc)
                    badge = m.get("badge") or m.get("poster")
                    logo = f"{BASE_URL}/api/images/badge/{badge}.webp" if badge else None

                    sources = [
                        SourceGroup(source=s.get("source"), group_id=s.get("id"), name=s.get("source", "Stream"))
                        for s in m.get("sources", [])
                        if s.get("source") and s.get("id")
                    ]

                    all_events_map[match_id] = MatchEvent(
                        id=match_id,
                        sport=fix_sport(m.get("category") or m.get("sport") or sp),
                        title=m.get("title", "Sports Event"),
                        start_dt=dt,
                        sources=sources,
                        logo=logo,
                        is_live=False
                    )
            except Exception:
                continue

        log.info(f"Total catalog contains {len(all_events_map)} events (Live + Scheduled) from {BASE_URL}")
    except Exception as e:
        log.error(f"Catalog fetch error: {e}")

    return list(all_events_map.values())

async def main():
    log.info(f"Connecting to {BASE_URL} for playlist generation...")
    async with httpx.AsyncClient(headers={"User-Agent": STREAM_UA, "Referer": BASE_URL}, follow_redirects=True, http2=True) as client:
        events = await fetch_all_matches(client)
        if not events:
            log.warning("No matches found!")
            return

        final_playlist = []
        now_utc = datetime.now(timezone.utc)

        for ev in events:
            status = determine_status(ev.start_dt, ev.is_live)
            event_channels = []

            # শুধুমাত্র LIVE অথবা ১ ঘণ্টার মধ্যে শুরু হতে যাওয়া ম্যাচগুলোর স্ট্রিম ফেচ হবে
            # কারণ সাইট নিজেই ১ ঘণ্টার আগে কোনো লাইভ লিঙ্ক দেয় না
            time_until_start = ev.start_dt.astimezone(timezone.utc) - now_utc
            if status == "LIVE" or (timedelta(0) <= time_until_start <= timedelta(hours=1)):
                tasks = [get_match_streams(client, src) for src in ev.sources]
                results = await asyncio.gather(*tasks)

                idx = 1
                for src, urls in zip(ev.sources, results):
                    for m3u8_url in urls:
                        raw_link = f"{m3u8_url}|User-Agent={STREAM_UA}&Referer={EMBED_REFERER}&Origin={EMBED_REFERER}"
                        event_channels.append({
                            "channel_name": f"{src.name.capitalize()} #{idx}",
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
                        idx += 1

            start_bst = ev.start_dt.astimezone(BST_TZ)
            final_playlist.append({
                "title": ev.title,
                "sport": ev.sport,
                "sub_category": ev.sport,
                "status": status,
                "is_hot": (status == "LIVE"),
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

        # ১. JSON সেভ
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

        # ২. M3U ফাইল সেভ (হুবহু আপনার দেওয়া ফরম্যাট অনুযায়ী)
        m3u_lines = ["#EXTM3U"]
        stream_count = 0
        for ev in final_playlist:
            for ch in ev.get("channels", []):
                tvg_logo = ev["logo"] or ""
                m3u_lines.append(f'#EXTINF:-1 tvg-id="Live.Event.us" tvg-logo="{tvg_logo}" group-title="{ev["sport"]}", {ev["title"]}')
                m3u_lines.append(f'#EXTVLCOPT:http-referrer={EMBED_REFERER}')
                m3u_lines.append(f'#EXTVLCOPT:http-user-agent={STREAM_UA}')
                m3u_lines.append(ch["raw_link"])
                stream_count += 1

        M3U_OUTPUT.write_text("\n".join(m3u_lines) + "\n", encoding="utf-8")

        log.info(f"Done! Successfully generated {len(final_playlist)} total events and {stream_count} active live streams in M3U.")

if __name__ == "__main__":
    asyncio.run(main())
