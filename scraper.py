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
from selectolax.lexbor import LexborHTMLParser as HTMLParser

# রুট পাথ ও কনফিগারেশন
ROOT_DIR = Path(__file__).resolve().parent

BASE_URL = "https://strmfree.st"
ALT_BASE = "https://streamfree.top"
STREAM_API = "https://strmfree.st/streams"
SOURCES_API_TEMPLATE = "https://strmfree.st/api/v1/sources/{stream_key}"
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
    category: str
    start_dt: datetime
    team_a_name: str
    team_b_name: str
    team_a_logo: str | None = None
    team_b_logo: str | None = None
    logo: str | None = None

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

def extract_m3u8(text: str, base_url: str = "") -> str | None:
    """JW Player এবং HTML/JS থেকে টোকেনসহ m3u8 লিংক এক্সট্রাক্ট করে"""
    if not text:
        return None

    clean_text = html.unescape(text).replace(r"\/", "/")

    # ১. JW Player এর file / source প্যারামিটার
    jw_match = re.search(r'(?:file|source|src)\s*:\s*["\'](https?://[^"\']+\.m3u8(?:\?[^"\']*)?)["\']', clean_text, re.IGNORECASE)
    if jw_match:
        return jw_match.group(1).strip("\"' ")

    # ২. live-externalcdn সরাসরি প্যাটার্ন (স্ক্রিনশটের URL)
    cdn_match = re.search(r'(https?://[a-zA-Z0-9_\-\.]+(?::\d+)?/live-externalcdn/[^\s"\'<>\\]*?\.m3u8(?:\?[^\s"\'<>\\]+)?)', clean_text)
    if cdn_match:
        return cdn_match.group(1).strip("\"' ")

    # ৩. যেকোনো index.m3u8 লিংক (টোকেনসহ)
    index_match = re.search(r'(https?://[a-zA-Z0-9_\-\.]+(?::\d+)?/[^\s"\'<>\\]*?index\.m3u8(?:\?[^\s"\'<>\\]+)?)', clean_text)
    if index_match:
        return index_match.group(1).strip("\"' ")

    # ৪. রিলেটিভ লিংক থাকলে (/live-externalcdn/.../index.m3u8)
    rel_match = re.search(r'["\'](/live-externalcdn/[^"\'\s]+\.m3u8(?:\?[^"\'\s]*)?)["\']', clean_text)
    if rel_match and base_url:
        return urljoin(base_url, rel_match.group(1)).strip("\"' ")

    # ৫. সাধারণ m3u8 প্যাটার্ন
    generic_match = re.search(r'(https?://[a-zA-Z0-9_\-\.]+(?::\d+)?/[^\s"\'<>\\]+?\.m3u8(?:\?[^\s"\'<>\\]+)?)', clean_text)
    if generic_match:
        return generic_match.group(1).strip("\"' ")

    # ৬. Base64 atob() ডিকোড
    for b64 in re.findall(r'atob\(["\']([A-Za-z0-9+/=]{20,})["\']\)', text):
        try:
            decoded = base64.b64decode(b64).decode("utf-8", errors="ignore")
            found = extract_m3u8(decoded, base_url)
            if found:
                return found
        except Exception:
            pass

    return None

async def resolve_embed_page(client: httpx.AsyncClient, embed_url: str, referer_url: str) -> str | None:
    """যেমন: https://strmfree.st/embed/combat/canelovsmbilli1080p ফেচ করে আসল m3u8 নিয়ে আসা"""
    try:
        headers = {
            "User-Agent": STREAM_UA,
            "Referer": referer_url,
            "Origin": BASE_URL,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        }
        resp = await client.get(embed_url, headers=headers, follow_redirects=True, timeout=10.0)
        if resp.status_code != 200:
            return None

        m3u8 = extract_m3u8(resp.text, base_url=str(resp.url))
        if m3u8:
            return m3u8

        # ভেতরে iframe থাকলে তা ফেচ করা
        soup = HTMLParser(resp.content)
        iframe = soup.css_first("iframe")
        if iframe:
            src = iframe.attributes.get("src", "")
            if src and not src.startswith("javascript:"):
                inner_url = urljoin(embed_url, src)
                resp_inner = await client.get(inner_url, headers={"User-Agent": STREAM_UA, "Referer": embed_url}, timeout=8.0)
                if resp_inner.status_code == 200:
                    return extract_m3u8(resp_inner.text, base_url=inner_url)
    except Exception as e:
        log.debug(f"Error fetching embed page {embed_url}: {e}")
    return None

async def resolve_match_channels(client: httpx.AsyncClient, match: ScrapedMatch) -> list[dict]:
    """
    https://strmfree.st/api/v1/sources/{stream_key} কল করে FHD ও SD চ্যানেল তৈরি করা
    """
    async with HTTP_SEMAPHORE:
        channels = []
        sources_url = SOURCES_API_TEMPLATE.format(stream_key=match.stream_key)
        player_referer = f"{BASE_URL}/player/{match.category}/{match.stream_key}"

        headers = {
            "User-Agent": STREAM_UA,
            "Referer": player_referer,
            "Origin": BASE_URL,
            "Accept": "application/json, text/plain, */*"
        }

        try:
            resp = await client.get(sources_url, headers=headers, timeout=10.0)
            if resp.status_code != 200:
                return channels

            data = resp.json()
            sources_list = data.get("sources") or data.get("live_sources") or []
            if not isinstance(sources_list, list):
                return channels

            for s in sources_list:
                if not isinstance(s, dict):
                    continue

                quality = s.get("quality", "Direct")
                embed_url = s.get("url", "")
                if not embed_url:
                    continue

                if ".m3u8" in embed_url:
                    m3u8_link = embed_url
                else:
                    m3u8_link = await resolve_embed_page(client, embed_url, player_referer)

                if m3u8_link:
                    # স্ক্রিনশট অনুযায়ী Referer: embed_url এবং Origin: BASE_URL
                    raw_link = f"{m3u8_link}|User-Agent={STREAM_UA}&Referer={embed_url}&Origin={BASE_URL}"
                    channels.append({
                        "channel_name": f"{quality} Stream",
                        "stream_type": "HLS",
                        "quality": quality,
                        "url": m3u8_link,
                        "raw_link": raw_link,
                        "headers": {
                            "User-Agent": STREAM_UA,
                            "Referer": embed_url,
                            "Origin": BASE_URL
                        },
                        "drm_key": None
                    })

        except Exception as e:
            log.debug(f"Error resolving sources for {match.stream_key}: {e}")

        return channels

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
                        category=category_name.lower(),
                        start_dt=dt,
                        team_a_name=team_a_name,
                        team_b_name=team_b_name,
                        team_a_logo=team_a_logo,
                        team_b_logo=team_b_logo,
                        logo=main_logo
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

        # লাইভ ম্যাচগুলোর জন্য সোর্স ফেচ করা
        live_matches = [
            m for m in matches 
            if calculate_match_status(m.start_dt, early_minutes=15) == "LIVE"
        ]

        log.info(f"Fetching active stream links for {len(live_matches)} LIVE matches...")
        tasks = [resolve_match_channels(client, m) for m in live_matches]
        resolved_results = await asyncio.gather(*tasks)

        channels_map = {m.stream_key: ch for m, ch in zip(live_matches, resolved_results)}

        final_playlist = []
        for m in matches:
            start_bst = m.start_dt.astimezone(BST_TZ)
            start_time_str = start_bst.strftime("%b %d, %Y, %I:%M %p")
            end_time_str = (start_bst + timedelta(hours=3)).strftime("%b %d, %Y, %I:%M %p")

            event_channels = channels_map.get(m.stream_key, [])
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

        # JSON ফাইল সেভ
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
                m3u_lines.append(f'#EXTINF:-1 tvg-id="Live.Event.us" tvg-logo="{tvg_logo}" group-title="{ev["sport"]}", {ev["title"]} | {ch["channel_name"]}')
                m3u_lines.append(f'#EXTVLCOPT:http-referrer={BASE_URL}/')
                m3u_lines.append(f'#EXTVLCOPT:http-user-agent={STREAM_UA}')
                m3u_lines.append(ch["raw_link"])
                stream_count += 1

        M3U_OUTPUT.write_text("\n".join(m3u_lines) + "\n", encoding="utf-8")

        log.info(f"Done! Successfully generated {len(final_playlist)} matches and {stream_count} active playable streams in M3U.")

if __name__ == "__main__":
    asyncio.run(main())
