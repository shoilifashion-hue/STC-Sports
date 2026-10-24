import asyncio
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

# রুট পাথ ও ফাইল কনফিগারেশন
ROOT_DIR = Path(__file__).resolve().parent

API_BASE = "https://streamcenter.st"
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

HTTP_SEMAPHORE = asyncio.Semaphore(6)
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
    is_live_catalog: bool = False

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

def determine_status(start_dt: datetime, is_live_catalog: bool = False) -> str:
    if is_live_catalog:
        return "LIVE"
    now_utc = datetime.now(timezone.utc)
    start_utc = start_dt.astimezone(timezone.utc)
    if (start_utc - timedelta(minutes=15)) <= now_utc <= (start_utc + timedelta(hours=3)):
        return "LIVE"
    elif now_utc < (start_utc - timedelta(minutes=15)):
        return "UPCOMING"
    return "ENDED"

def find_m3u8_link(text: str) -> str | None:
    """JS বা সোর্স পেজ থেকে টোকেনযুক্ত m3u8 লিংক এক্সট্রাক্ট করে"""
    clean_text = text.replace(r"\/", "/")
    # টোকেন ও এক্সপায়ারি কুয়েরিসহ m3u8 Regex
    match = re.search(r'(https?://[a-zA-Z0-9_\-\.]+(?::\d+)?/[^\s"\'<>\\]+?\.m3u8(?:\?[^\s"\'<>\\]+)?)', clean_text)
    if match:
        return match.group(1)
    return None

async def resolve_m3u8_from_embed(client: httpx.AsyncClient, embed_url: str) -> str | None:
    """embedUrl ফেচ করে তার ভেতর থেকে আসল m3u8 বের করে"""
    try:
        resp = await client.get(
            embed_url,
            headers={"Referer": API_BASE, "User-Agent": STREAM_UA},
            timeout=10.0
        )
        if resp.status_code != 200:
            return None

        # ১. সোর্স টেক্সটের ভেতর m3u8 আছে কি না
        m3u8_url = find_m3u8_link(resp.text)
        if m3u8_url:
            return m3u8_url

        # ২. যদি পেজের ভেতর কোনো নেস্টেড iframe থাকে
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
                    return find_m3u8_link(resp_inner.text)
    except Exception:
        pass
    return None

async def get_match_stream_urls(client: httpx.AsyncClient, source: SourceGroup) -> list[str]:
    """StreamCenter API থেকে স্ট্রিম তথ্য নিয়ে আসল m3u8 তৈরি করে"""
    async with HTTP_SEMAPHORE:
        stream_urls = []
        try:
            api_url = f"{API_BASE}/api/stream/{quote(source.source)}/{quote(source.group_id)}"
            resp = await client.get(api_url, headers={"User-Agent": STREAM_UA}, timeout=10.0)
            if resp.status_code != 200:
                return stream_urls

            data = resp.json()
            if not isinstance(data, list):
                return stream_urls

            for item in data:
                # সরাসরি m3u8 থাকলে
                if isinstance(item, dict):
                    direct_url = item.get("stream") or item.get("url")
                    if direct_url and ".m3u8" in direct_url:
                        stream_urls.append(direct_url)
                        continue

                    # embedUrl থাকলে তা ফেচ করে টোকেনসহ আসল m3u8 সংগ্রহ
                    embed_url = item.get("embedUrl")
                    if embed_url:
                        found_m3u8 = await resolve_m3u8_from_embed(client, embed_url)
                        if found_m3u8:
                            stream_urls.append(found_m3u8)
        except Exception:
            pass
        return stream_urls

async def fetch_all_matches(client: httpx.AsyncClient) -> list[MatchEvent]:
    """অফিসিয়াল API থেকে লাইভ ও আপকামিং ম্যাচ ফেচ করে"""
    all_events_map: dict[str, MatchEvent] = {}

    # ১. প্রথমে সরাসরি LIVE ম্যাচগুলো আনা
    try:
        live_resp = await client.get(f"{API_BASE}/api/matches/live", timeout=12.0)
        if live_resp.status_code == 200:
            live_matches = live_resp.json()
            if isinstance(live_matches, list):
                for m in live_matches:
                    match_id = str(m.get("id") or m.get("title"))
                    dt = datetime.fromtimestamp(m.get("date", 0) / 1000, tz=timezone.utc)
                    badge = m.get("badge") or m.get("poster")
                    logo = f"{API_BASE}/api/images/badge/{badge}.webp" if badge else None

                    sources = [
                        SourceGroup(source=s.get("source"), group_id=s.get("id"), name=s.get("source", "Stream"))
                        for s in m.get("sources", [])
                        if s.get("source") and s.get("id")
                    ]

                    all_events_map[match_id] = MatchEvent(
                        id=match_id,
                        sport=fix_sport(m.get("category") or m.get("sport") or "Football"),
                        title=m.get("title", "Live Match"),
                        start_dt=dt,
                        sources=sources,
                        logo=logo,
                        is_live_catalog=True
                    )
                log.info(f"Loaded {len(live_matches)} LIVE matches directly from live API.")
    except Exception as e:
        log.error(f"Error fetching live matches: {e}")

    # ২. এরপর ALL / SCHEDULED ম্যাচগুলো আনা
    try:
        all_resp = await client.get(f"{API_BASE}/api/matches/all", timeout=12.0)
        matches = all_resp.json() if all_resp.status_code == 200 else []
        
        # যদি matches/all কাজ না করে, sports লিস্ট ধরে ব্যাকআপ আনা
        if not matches or not isinstance(matches, list):
            sports_resp = await client.get(f"{API_BASE}/api/sports", timeout=10.0)
            if sports_resp.status_code == 200:
                for sp in sports_resp.json():
                    sp_id = sp.get("id")
                    sp_matches = await client.get(f"{API_BASE}/api/matches/{sp_id}", timeout=8.0)
                    if sp_matches.status_code == 200:
                        matches.extend(sp_matches.json())

        now_utc = datetime.now(timezone.utc)
        for m in matches:
            if not isinstance(m, dict):
                continue
            match_id = str(m.get("id") or m.get("title"))
            if match_id in all_events_map:
                continue  # লাইভ হিসেবে অলরেডি যুক্ত থাকলে স্কিপ

            dt = datetime.fromtimestamp(m.get("date", 0) / 1000, tz=timezone.utc)
            # অতীত ৩ ঘণ্টা ও ভবিষ্যৎ ৪৮ ঘণ্টার ম্যাচ ফিল্টার
            if dt < (now_utc - timedelta(hours=3)) or dt > (now_utc + timedelta(hours=48)):
                continue

            badge = m.get("badge") or m.get("poster")
            logo = f"{API_BASE}/api/images/badge/{badge}.webp" if badge else None

            sources = [
                SourceGroup(source=s.get("source"), group_id=s.get("id"), name=s.get("source", "Stream"))
                for s in m.get("sources", [])
                if s.get("source") and s.get("id")
            ]

            all_events_map[match_id] = MatchEvent(
                id=match_id,
                sport=fix_sport(m.get("category") or m.get("sport") or "Sports"),
                title=m.get("title", "Event"),
                start_dt=dt,
                sources=sources,
                logo=logo,
                is_live_catalog=False
            )
        log.info(f"Total catalog contains {len(all_events_map)} total events (Live + Scheduled).")
    except Exception as e:
        log.error(f"Error fetching upcoming matches: {e}")

    return list(all_events_map.values())

async def main():
    log.info("Starting StreamCenter playlist and stream extractor...")
    async with httpx.AsyncClient(headers={"User-Agent": STREAM_UA}, follow_redirects=True, http2=True) as client:
        events = await fetch_all_matches(client)
        if not events:
            log.warning("No matches found from StreamCenter API!")
            return

        final_playlist = []
        now_utc = datetime.now(timezone.utc)

        for ev in events:
            status = determine_status(ev.start_dt, ev.is_live_catalog)
            event_channels = []

            # লাইভ অথবা ১ ঘণ্টার মধ্যে শুরু হতে যাওয়া ম্যাচের স্ট্রিম ফেচ করা
            if status == "LIVE" or (ev.start_dt.astimezone(timezone.utc) - now_utc) <= timedelta(hours=1):
                tasks = [get_match_stream_urls(client, src) for src in ev.sources]
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

        # M3U ফাইল সেভ (হুবহু আপনার দেওয়া ফরম্যাট অনুযায়ী)
        m3u_lines = ["#EXTM3U"]
        live_stream_count = 0
        for ev in final_playlist:
            for ch in ev.get("channels", []):
                tvg_logo = ev["logo"] or ""
                # আপনার চাওয়া নিখুঁত লাইন ফরম্যাট
                m3u_lines.append(f'#EXTINF:-1 tvg-id="Live.Event.us" tvg-logo="{tvg_logo}" group-title="{ev["sport"]}", {ev["title"]}')
                m3u_lines.append(f'#EXTVLCOPT:http-referrer={EMBED_REFERER}')
                m3u_lines.append(f'#EXTVLCOPT:http-user-agent={STREAM_UA}')
                m3u_lines.append(ch["raw_link"])
                live_stream_count += 1

        M3U_OUTPUT.write_text("\n".join(m3u_lines) + "\n", encoding="utf-8")

        log.info(f"Done! Saved {len(final_playlist)} matches in JSON and {live_stream_count} active streams in M3U.")

if __name__ == "__main__":
    asyncio.run(main())
