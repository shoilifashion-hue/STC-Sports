import asyncio
import base64
import html
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urljoin, quote
from zoneinfo import ZoneInfo

import httpx
from selectolax.lexbor import LexborHTMLParser as HTMLParser

# রুট পাথ ও কনফিগারেশন
ROOT_DIR = Path(__file__).resolve().parent

BASE_URL = "https://streamfree.top"
STREAM_API = "https://strmfree.st/streams"
SITE_REFERER = "https://streamfree.top/"
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

HTTP_SEMAPHORE = asyncio.Semaphore(5)
BST_TZ = ZoneInfo("Asia/Dhaka")

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")
log = logging.getLogger(TAG)

@dataclass
class ScrapedMatch:
    sport: str
    title: str
    site_status: str
    start_dt: datetime
    player_url: str
    team_a_logo: str | None = None
    team_b_logo: str | None = None
    logo: str | None = None
    direct_streams: list[str] = field(default_factory=list)

def cleanup_text(text: str) -> str:
    text = text.replace('\xa0', ' ')
    text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
    text = re.sub(r'(vs\.?)([A-Za-z])', r'\1 \2', text, flags=re.IGNORECASE)
    text = re.sub(r'([A-Za-z])(vs\.?)', r'\1 \2', text, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", text).strip()

def create_slug(text: str) -> str:
    return re.sub(r"[\W_]+", "-", text.lower().strip()).strip("-")

def parse_teams(title: str, team_a_logo: str | None = None, team_b_logo: str | None = None) -> dict:
    delimiters = [" vs. ", " vs ", " - ", " @ "]
    for d in delimiters:
        if d in title.lower():
            parts = re.split(re.escape(d), title, maxsplit=1, flags=re.IGNORECASE)
            if len(parts) == 2:
                return {
                    "team_a": {"name": parts[0].strip(), "logo": team_a_logo},
                    "team_b": {"name": parts[1].strip(), "logo": team_b_logo}
                }
    return {
        "team_a": {"name": title.strip(), "logo": team_a_logo},
        "team_b": {"name": title.strip(), "logo": team_b_logo}
    }

def calculate_match_status(start_dt: datetime, site_status: str, early_minutes: int = 15) -> str:
    upper_status = site_status.upper()
    if any(k in upper_status for k in ["ENDED", "FINISHED", "FINAL", "FT"]):
        return "ENDED"

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

    # index.m3u8?_t=... অথবা edgestream/অন্যান্য m3u8
    for m in re.findall(r'(https?://[a-zA-Z0-9_\-\.]+(?::\d+)?/[^\s"\'<>\\]*?\.m3u8(?:\?[^\s"\'<>\\]+)?)', clean_text):
        link = m.strip("\"' ")
        if link not in found:
            found.append(link)

    return found

async def resolve_stream_from_player(client: httpx.AsyncClient, player_url: str) -> list[str]:
    if not player_url or "${" in player_url:
        return []

    async with HTTP_SEMAPHORE:
        stream_urls = []
        try:
            headers = {
                "User-Agent": STREAM_UA,
                "Referer": SITE_REFERER,
                "Origin": BASE_URL,
            }
            resp = await client.get(player_url, headers=headers, follow_redirects=True, timeout=12.0)
            if resp.status_code == 200:
                stream_urls.extend(extract_m3u8_links(resp.text))

                soup = HTMLParser(resp.content)
                for iframe in soup.css("iframe"):
                    src = iframe.attributes.get("src", "")
                    if src and not src.startswith("javascript:"):
                        iframe_url = urljoin(str(resp.url), src)
                        resp_iframe = await client.get(iframe_url, headers={"User-Agent": STREAM_UA, "Referer": player_url}, timeout=10.0)
                        if resp_iframe.status_code == 200:
                            stream_urls.extend(extract_m3u8_links(resp_iframe.text))

        except Exception as e:
            log.debug(f"Player resolve error for {player_url}: {e}")

        return list(dict.fromkeys(stream_urls))

def parse_streamfree_api_data(data) -> list[ScrapedMatch]:
    """https://strmfree.st/streams API রেসপন্স নিখুঁতভাবে পার্স করা"""
    matches = []
    now_utc = datetime.now(timezone.utc)

    # যদি ডিকশনারির ভেতরে কোনো লিস্ট থাকে (e.g. {"streams": [...]} or {"data": [...]})
    items = []
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        for k in ["streams", "data", "matches", "events"]:
            if isinstance(data.get(k), list):
                items = data[k]
                break
        if not items:
            items = list(data.values())

    for item in items:
        if not isinstance(item, dict):
            continue

        # টেমপ্লেট কোড('${...}') থাকলে বাদ দেওয়া
        raw_title = item.get("name") or item.get("title") or item.get("stream_name") or item.get("match_name") or ""
        if "${" in str(raw_title):
            raw_title = ""

        stream_key = item.get("stream_key") or item.get("slug") or item.get("id") or ""
        if "${" in str(stream_key):
            stream_key = ""

        category = item.get("category") or item.get("sport") or "Sports"
        if "${" in str(category):
            category = "Sports"

        category = category.capitalize()

        # টাইটেল তৈরি
        if not raw_title and stream_key:
            raw_title = str(stream_key).replace("-vs-", " vs ").replace("-", " ").title()
        
        if not raw_title:
            t1 = item.get("home_team") or item.get("team1")
            t2 = item.get("away_team") or item.get("team2")
            if t1 and t2:
                raw_title = f"{t1} vs {t2}"

        if not raw_title:
            continue

        title = cleanup_text(str(raw_title))

        # প্লেয়ার ইউআরএল তৈরি
        if stream_key:
            player_url = f"{BASE_URL}/player/{quote(category.lower())}/{quote(str(stream_key))}"
        else:
            player_url = item.get("player_url") or item.get("url") or f"{BASE_URL}/player/{quote(category.lower())}/{create_slug(title)}"

        # সরাসরি কোনো m3u8 স্ট্রিম লিংক থাকলে বের করা
        direct_streams = []
        for key in ["stream", "url", "hls", "m3u8", "link", "source"]:
            val = item.get(key)
            if val and isinstance(val, str) and ".m3u8" in val:
                direct_streams.append(val)

        # যদি sources বা streams লিস্ট থাকে
        nested_sources = item.get("sources") or item.get("streams")
        if isinstance(nested_sources, list):
            for s in nested_sources:
                if isinstance(s, dict):
                    u = s.get("url") or s.get("stream") or s.get("link")
                    if u and isinstance(u, str) and ".m3u8" in u:
                        direct_streams.append(u)
                elif isinstance(s, str) and ".m3u8" in s:
                    direct_streams.append(s)

        # স্ট্যাটাস
        is_live = item.get("live") or item.get("is_live") or item.get("isLive")
        site_status = "LIVE" if is_live else str(item.get("status") or "UPCOMING").upper()

        # সময় পার্স
        dt = now_utc
        time_val = item.get("time") or item.get("start_time") or item.get("date") or item.get("timestamp")
        if time_val:
            try:
                if isinstance(time_val, (int, float)):
                    dt = datetime.fromtimestamp(time_val / 1000 if time_val > 1e11 else time_val, tz=timezone.utc)
                elif isinstance(time_val, str):
                    dt = datetime.fromisoformat(time_val.replace("Z", "+00:00"))
            except Exception:
                dt = now_utc

        # লোগো
        team_a_logo = item.get("home_logo") or item.get("team1_logo") or item.get("home_badge")
        team_b_logo = item.get("away_logo") or item.get("team2_logo") or item.get("away_badge")
        main_logo = item.get("logo") or item.get("poster") or item.get("image") or team_a_logo

        matches.append(ScrapedMatch(
            sport=category,
            title=title,
            site_status=site_status,
            start_dt=dt,
            player_url=player_url,
            team_a_logo=team_a_logo,
            team_b_logo=team_b_logo,
            logo=main_logo,
            direct_streams=direct_streams
        ))

    return matches

async def fetch_all_matches(client: httpx.AsyncClient) -> list[ScrapedMatch]:
    matches = []

    # ১. সরাসরি strmfree.st/streams API থেকে ডাটা ফেচ করা (সবচেয়ে নিখুঁত)
    try:
        api_headers = {
            "User-Agent": STREAM_UA,
            "Referer": SITE_REFERER,
            "Origin": BASE_URL,
            "Accept": "application/json, text/plain, */*"
        }
        resp = await client.get(STREAM_API, headers=api_headers, timeout=12.0)
        if resp.status_code == 200:
            try:
                data = resp.json()
                matches = parse_streamfree_api_data(data)
                log.info(f"Loaded {len(matches)} matches from {STREAM_API} API!")
            except Exception as e:
                log.warning(f"Error parsing API JSON: {e}")
    except Exception as e:
        log.warning(f"API request failed: {e}")

    # ২. ব্যাকআপ: যদি API খালি থাকে, তবে হোমপেজের আসল HTML কার্ড থেকে বের করা
    if not matches:
        try:
            hp_resp = await client.get(BASE_URL, headers={"User-Agent": STREAM_UA}, timeout=15.0)
            if hp_resp.status_code == 200:
                soup = HTMLParser(hp_resp.content)
                # Next.js __NEXT_DATA__ স্ক্রিপ্ট চেক
                next_tag = soup.css_first("script#__NEXT_DATA__")
                if next_tag and next_tag.text():
                    try:
                        n_data = json.loads(next_tag.text())
                        matches = parse_streamfree_api_data(n_data)
                    except Exception:
                        pass
        except Exception as e:
            log.warning(f"Homepage fetch failed: {e}")

    # ডুপ্লিকেশন দূর করা
    unique_matches: dict[str, ScrapedMatch] = {}
    for m in matches:
        # টেমপ্লেট কোড ফিল্টার
        if "${" in m.title or "${" in m.sport:
            continue
        slug = create_slug(m.title)
        if slug not in unique_matches:
            unique_matches[slug] = m

    return list(unique_matches.values())

async def main():
    log.info(f"Starting StreamFree Scraper execution on {BASE_URL}...")
    async with httpx.AsyncClient(headers={"User-Agent": STREAM_UA}, follow_redirects=True, http2=True) as client:
        matches = await fetch_all_matches(client)
        if not matches:
            log.warning("No valid matches found from StreamFree!")
            return

        final_playlist = []

        for m in matches:
            start_bst = m.start_dt.astimezone(BST_TZ)
            start_time_str = start_bst.strftime("%b %d, %Y, %I:%M %p")
            end_time_str = (start_bst + timedelta(hours=3)).strftime("%b %d, %Y, %I:%M %p")

            # স্ট্রিম লিঙ্ক সংগ্রহ (সরাসরি API থেকে পাওয়া লিঙ্ক + প্রয়োজন হলে প্লেয়ার পেজ ফেচ করা)
            stream_urls = list(m.direct_streams)
            if not stream_urls and m.player_url and "${" not in m.player_url:
                stream_urls = await resolve_stream_from_player(client, m.player_url)

            event_channels = []
            idx = 1
            for m3u8_url in stream_urls:
                raw_link = f"{m3u8_url}|User-Agent={STREAM_UA}&Referer={SITE_REFERER}&Origin={BASE_URL}"
                event_channels.append({
                    "channel_name": f"Stream #{idx}",
                    "stream_type": "HLS",
                    "url": m3u8_url,
                    "raw_link": raw_link,
                    "headers": {
                        "User-Agent": STREAM_UA,
                        "Referer": SITE_REFERER,
                        "Origin": BASE_URL
                    },
                    "drm_key": None
                })
                idx += 1

            actual_status = calculate_match_status(m.start_dt, m.site_status, early_minutes=15)
            teams_data = parse_teams(m.title, team_a_logo=m.team_a_logo, team_b_logo=m.team_b_logo)
            primary_logo = m.team_a_logo or m.logo or m.team_b_logo

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
                "logo": primary_logo,
                "channels": event_channels,
                "_timestamp": m.start_dt.timestamp()
            })

        priority = {"LIVE": 0, "UPCOMING": 1, "ENDED": 2}
        final_playlist.sort(key=lambda x: (priority.get(x["status"], 99), x["_timestamp"]))
        for item in final_playlist:
            item.pop("_timestamp", None)

        # JSON সেভ
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

        # M3U প্লেলিস্ট সেভ
        m3u_lines = ["#EXTM3U"]
        stream_count = 0
        for ev in final_playlist:
            for ch in ev.get("channels", []):
                tvg_logo = ev["logo"] or ""
                m3u_lines.append(f'#EXTINF:-1 tvg-id="Live.Event.us" tvg-logo="{tvg_logo}" group-title="{ev["sport"]}", {ev["title"]}')
                m3u_lines.append(f'#EXTVLCOPT:http-referrer={SITE_REFERER}')
                m3u_lines.append(f'#EXTVLCOPT:http-user-agent={STREAM_UA}')
                m3u_lines.append(ch["raw_link"])
                stream_count += 1

        M3U_OUTPUT.write_text("\n".join(m3u_lines) + "\n", encoding="utf-8")

        log.info(f"Done! Successfully generated {len(final_playlist)} matches and {stream_count} active live streams in M3U.")

if __name__ == "__main__":
    asyncio.run(main())
