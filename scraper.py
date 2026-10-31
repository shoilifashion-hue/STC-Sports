import asyncio
import base64
import html
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import httpx

# রুট পাথ ও কনফিগারেশন
ROOT_DIR = Path(__file__).resolve().parent

BASE_URL = "https://strmfree.st"
ALT_BASE = "https://streamfree.top"
STREAM_API = "https://strmfree.st/streams"
TAG = "STRMFREE"

JSON_OUTPUT = ROOT_DIR / "streamfree.json"
M3U_OUTPUT = ROOT_DIR / "streamfree.m3u"
CACHE_DIR = ROOT_DIR / "caches"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

STREAM_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/111.0.0.0 Safari/537.36"
)

HTTP_SEMAPHORE = asyncio.Semaphore(10)
BST_TZ = ZoneInfo("Asia/Dhaka")

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")
log = logging.getLogger(TAG)

@dataclass
class ScrapedMatch:
    sport: str
    title: str
    stream_key: str
    start_dt: datetime
    team_a_name: str
    team_b_name: str
    team_a_logo: str | None = None
    team_b_logo: str | None = None
    logo: str | None = None
    player_url: str = ""

def cleanup_text(text: str) -> str:
    text = text.replace('\xa0', ' ')
    text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
    text = re.sub(r'(vs\.?)([A-Za-z])', r'\1 \2', text, flags=re.IGNORECASE)
    text = re.sub(r'([A-Za-z])(vs\.?)', r'\1 \2', text, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", text).strip()

def create_slug(text: str) -> str:
    return re.sub(r"[\W_]+", "-", text.lower().strip()).strip("-")

def calculate_match_status(start_dt: datetime, early_minutes: int = 15) -> str:
    now_utc = datetime.now(timezone.utc)
    start_utc = start_dt.astimezone(timezone.utc)
    live_start = start_utc - timedelta(minutes=early_minutes)

    if now_utc < live_start:
        return "UPCOMING"
    return "LIVE"

def extract_m3u8_links(text: str) -> list[str]:
    found = []
    if not text:
        return found
    clean_text = html.unescape(text).replace(r"\/", "/")

    for m in re.findall(r'(https?://[a-zA-Z0-9_\-\.]+(?::\d+)?/[^\s"\'<>\\]*?\.m3u8(?:\?[^\s"\'<>\\]+)?)', clean_text):
        link = m.strip("\"' ")
        if link not in found:
            found.append(link)

    return found

async def resolve_match_streams(client: httpx.AsyncClient, stream_key: str, category: str) -> list[str]:
    """শুধুমাত্র লাইভ ম্যাচগুলোর স্ট্রিম সোর্স ফেচ করা"""
    async with HTTP_SEMAPHORE:
        urls = []
        headers = {
            "User-Agent": STREAM_UA,
            "Referer": f"{BASE_URL}/",
            "Origin": BASE_URL,
            "Accept": "*/*"
        }

        endpoints_to_try = [
            f"{BASE_URL}/{stream_key}",
            f"{BASE_URL}/player/{category}/{stream_key}",
            f"{ALT_BASE}/player/{category}/{stream_key}"
        ]

        for ep in endpoints_to_try:
            try:
                resp = await client.get(ep, headers=headers, follow_redirects=True, timeout=8.0)
                if resp.status_code == 200:
                    found = extract_m3u8_links(resp.text)
                    if found:
                        urls.extend(found)
                        break
            except Exception:
                pass

        return list(dict.fromkeys(urls))

async def fetch_all_matches(client: httpx.AsyncClient) -> list[ScrapedMatch]:
    matches = []
    headers = {
        "User-Agent": STREAM_UA,
        "Referer": f"{ALT_BASE}/",
        "Origin": ALT_BASE,
        "Accept": "application/json, text/plain, */*"
    }

    try:
        resp = await client.get(STREAM_API, headers=headers, timeout=15.0)
        if resp.status_code != 200:
            log.error(f"Failed to fetch {STREAM_API}: Status {resp.status_code}")
            return matches

        data = resp.json()
        streams_dict = data.get("streams", {})

        if isinstance(streams_dict, dict):
            for category_name, match_list in streams_dict.items():
                if not isinstance(match_list, list):
                    continue

                sport = category_name.capitalize()
                for item in match_list:
                    if not isinstance(item, dict):
                        continue

                    title = cleanup_text(item.get("name") or "")
                    stream_key = item.get("stream_key") or item.get("id") or ""
                    if not title or not stream_key:
                        continue

                    ts = item.get("match_timestamp") or item.get("created_at") or 0
                    dt = datetime.fromtimestamp(ts, tz=timezone.utc) if ts else datetime.now(timezone.utc)

                    team1 = item.get("team1") or {}
                    team2 = item.get("team2") or {}
                    team_a_name = team1.get("name") or title
                    team_b_name = team2.get("name") or title
                    team_a_logo = team1.get("logo")
                    team_b_logo = team2.get("logo")

                    main_logo = team_a_logo or team_b_logo

                    matches.append(ScrapedMatch(
                        sport=sport,
                        title=title,
                        stream_key=str(stream_key),
                        start_dt=dt,
                        team_a_name=team_a_name,
                        team_b_name=team_b_name,
                        team_a_logo=team_a_logo,
                        team_b_logo=team_b_logo,
                        logo=main_logo,
                        player_url=f"{BASE_URL}/player/{category_name}/{stream_key}"
                    ))

        log.info(f"Loaded {len(matches)} matches directly from {STREAM_API}!")

    except Exception as e:
        log.error(f"Error fetching API: {e}")

    return matches

async def main():
    log.info("Starting StreamFree Scraper execution (One-time generation)...")
    async with httpx.AsyncClient(headers={"User-Agent": STREAM_UA}, follow_redirects=True, http2=True) as client:
        matches = await fetch_all_matches(client)
        if not matches:
            log.warning("No matches found from StreamFree API!")
            return

        final_playlist = []

        # যে ম্যাচগুলো LIVE অথবা আগামী ৩০ মিনিটে শুরু হবে সেগুলোর স্ট্রিম ফেচ করা
        now_utc = datetime.now(timezone.utc)
        live_or_soon_matches = [
            m for m in matches 
            if calculate_match_status(m.start_dt, early_minutes=15) == "LIVE" 
            or (m.start_dt - now_utc) <= timedelta(minutes=30)
        ]

        log.info(f"Resolving streams for {len(live_or_soon_matches)} active/upcoming matches...")
        stream_tasks = [resolve_match_streams(client, m.stream_key, m.sport.lower()) for m in live_or_soon_matches]
        resolved_results = await asyncio.gather(*stream_tasks)
        
        streams_map = {m.stream_key: res for m, res in zip(live_or_soon_matches, resolved_results)}

        for m in matches:
            start_bst = m.start_dt.astimezone(BST_TZ)
            start_time_str = start_bst.strftime("%b %d, %Y, %I:%M %p")
            end_time_str = (start_bst + timedelta(hours=3)).strftime("%b %d, %Y, %I:%M %p")

            stream_urls = streams_map.get(m.stream_key, [])

            event_channels = []
            idx = 1
            for m3u8_url in stream_urls:
                raw_link = f"{m3u8_url}|User-Agent={STREAM_UA}&Referer={BASE_URL}/&Origin={BASE_URL}"
                event_channels.append({
                    "channel_name": f"Stream #{idx}",
                    "stream_type": "HLS",
                    "url": m3u8_url,
                    "raw_link": raw_link,
                    "headers": {
                        "User-Agent": STREAM_UA,
                        "Referer": f"{BASE_URL}/",
                        "Origin": BASE_URL
                    },
                    "drm_key": None
                })
                idx += 1

            actual_status = calculate_match_status(m.start_dt, early_minutes=15)

            teams_data = {
                "team_a": {"name": m.team_a_name, "logo": m.team_a_logo},
                "team_b": {"name": m.team_b_name, "logo": m.team_b_logo}
            }

            final_playlist.append({
                "title": m.title,
                "sport": m.sport,
                "sub_category": m.sport,
                "status": actual_status,
                "is_hot": (actual_status == "LIVE"),
                "slug": create_slug(m.title),
                "schedule": {
                    "timezone": "BST (UTC+6)",
                    "start_time_bst": start_time_str,
                    "end_time_bst": end_time_str,
                },
                "teams": teams_data,
                "logo": m.logo,
                "channels": event_channels,
                "_timestamp": m.start_dt.timestamp()
            })

        priority = {"LIVE": 0, "UPCOMING": 1, "ENDED": 2}
        final_playlist.sort(key=lambda x: (priority.get(x["status"], 99), x["_timestamp"]))
        for item in final_playlist:
            item.pop("_timestamp", None)

        # ১. JSON ফাইল সেভ
        JSON_OUTPUT.write_text(
            json.dumps(
                {
                    "last_updated": datetime.now(BST_TZ).strftime("%b %d, %Y, %I:%M %p"),
                    "event_count": len(final_playlist),
                    "events": {"events": final_playlist}
                },
                indent=2,
                ensure_ascii=False
            ),
            encoding="utf-8"
        )

        # ২. M3U প্লেলিস্ট সেভ
        m3u_lines = ["#EXTM3U"]
        stream_count = 0
        for ev in final_playlist:
            for ch in ev.get("channels", []):
                tvg_logo = ev["logo"] or ""
                m3u_lines.append(f'#EXTINF:-1 tvg-id="Live.Event.us" tvg-logo="{tvg_logo}" group-title="{ev["sport"]}", {ev["title"]}')
                m3u_lines.append(f'#EXTVLCOPT:http-referrer={BASE_URL}/')
                m3u_lines.append(f'#EXTVLCOPT:http-user-agent={STREAM_UA}')
                m3u_lines.append(ch["raw_link"])
                stream_count += 1

        M3U_OUTPUT.write_text("\n".join(m3u_lines) + "\n", encoding="utf-8")

        log.info(f"Done! Saved {len(final_playlist)} matches and {stream_count} active live streams. Exiting cleanly.")

if __name__ == "__main__":
    asyncio.run(main())
