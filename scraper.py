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
    """টোকেনসহ m3u8 লিংক এক্সট্রাক্ট করে"""
    found = []
    if not text:
        return found
    clean_text = html.unescape(text).replace(r"\/", "/")

    # index.m3u8?_t=... প্যাটার্ন
    for m in re.findall(r'(https?://[a-zA-Z0-9_\-\.]+(?::\d+)?/[^\s"\'<>\\]*?index\.m3u8(?:\?[^\s"\'<>\\]+)?)', clean_text):
        link = m.strip("\"' ")
        if link not in found:
            found.append(link)

    # সাধারণ .m3u8
    if not found:
        for m in re.findall(r'(https?://[a-zA-Z0-9_\-\.]+(?::\d+)?/[^\s"\'<>\\]+?\.m3u8(?:\?[^\s"\'<>\\]+)?)', clean_text):
            link = m.strip("\"' ")
            if link not in found:
                found.append(link)

    return found

async def resolve_stream_from_player(client: httpx.AsyncClient, player_url: str) -> list[str]:
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

                # পেজে __NEXT_DATA__ থাকলে চেক করা
                soup = HTMLParser(resp.content)
                next_tag = soup.css_first("script#__NEXT_DATA__")
                if next_tag and next_tag.text():
                    stream_urls.extend(extract_m3u8_links(next_tag.text()))

            # https://strmfree.st/streams API থেকে খোঁজা
            try:
                api_resp = await client.get(
                    STREAM_API,
                    headers={"User-Agent": STREAM_UA, "Referer": player_url, "Origin": BASE_URL},
                    timeout=8.0
                )
                if api_resp.status_code == 200:
                    stream_urls.extend(extract_m3u8_links(api_resp.text))
            except Exception:
                pass

        except Exception as e:
            log.debug(f"Player resolve error for {player_url}: {e}")

        return list(dict.fromkeys(stream_urls))

def parse_next_data_json(json_data: dict) -> list[ScrapedMatch]:
    """Next.js এর __NEXT_DATA__ ট্রি থেকে সব ম্যাচ খুঁজে বের করা"""
    matches = []
    now_utc = datetime.now(timezone.utc)

    # রিকার্সিভলি অবজেক্ট স্ক্যান করা
    def scan_obj(obj):
        if isinstance(obj, dict):
            # ম্যাচ অবজেক্টের বৈশিষ্ট্য চেক করা
            if ("title" in obj or "name" in obj or "teams" in obj) and ("slug" in obj or "playerUrl" in obj or "sport" in obj or "category" in obj):
                title = obj.get("title") or obj.get("name")
                if not title and isinstance(obj.get("teams"), dict):
                    t1 = obj["teams"].get("home", {}).get("name", "")
                    t2 = obj["teams"].get("away", {}).get("name", "")
                    title = f"{t1} vs {t2}" if t1 and t2 else ""

                sport = (obj.get("sport") or obj.get("category") or "Sports").capitalize()
                slug = obj.get("slug") or ""
                
                player_url = obj.get("playerUrl") or obj.get("url")
                if not player_url and slug:
                    player_url = f"{BASE_URL}/player/{sport.lower()}/{slug}"
                elif player_url:
                    player_url = urljoin(BASE_URL, player_url)

                if title and player_url:
                    title = cleanup_text(title)
                    site_status = str(obj.get("status") or "UPCOMING").upper()
                    if obj.get("isLive") or obj.get("live"):
                        site_status = "LIVE"

                    # স্টার্ট টাইম পার্সিং
                    dt = now_utc
                    date_val = obj.get("date") or obj.get("startTime") or obj.get("time") or obj.get("start")
                    if date_val:
                        try:
                            if isinstance(date_val, (int, float)):
                                dt = datetime.fromtimestamp(date_val / 1000 if date_val > 1e11 else date_val, tz=timezone.utc)
                            elif isinstance(date_val, str):
                                dt = datetime.fromisoformat(date_val.replace("Z", "+00:00"))
                        except Exception:
                            dt = now_utc

                    # লোগো
                    team_a_logo = obj.get("homeLogo") or obj.get("team1Logo")
                    team_b_logo = obj.get("awayLogo") or obj.get("team2Logo")
                    main_logo = obj.get("poster") or obj.get("logo") or team_a_logo

                    matches.append(ScrapedMatch(
                        sport=sport,
                        title=title,
                        site_status=site_status,
                        start_dt=dt,
                        player_url=player_url,
                        team_a_logo=team_a_logo,
                        team_b_logo=team_b_logo,
                        logo=main_logo
                    ))

            for v in obj.values():
                scan_obj(v)
        elif isinstance(obj, list):
            for item in obj:
                scan_obj(item)

    scan_obj(json_data)
    return matches

async def scrape_streamfree_homepage(client: httpx.AsyncClient) -> list[ScrapedMatch]:
    matches = []
    try:
        resp = await client.get(BASE_URL, headers={"User-Agent": STREAM_UA}, timeout=15.0)
        if resp.status_code != 200:
            log.error(f"Failed to fetch homepage: Status {resp.status_code}")
            return matches

        soup = HTMLParser(resp.content)

        # ১. Next.js __NEXT_DATA__ স্ক্রিপ্ট থেকে এক্সট্রাক্ট করা (সবচেয়ে নিখুঁত পদ্ধতি)
        next_data_script = soup.css_first("script#__NEXT_DATA__")
        if next_data_script and next_data_script.text():
            try:
                data = json.loads(next_data_script.text())
                matches = parse_next_data_json(data)
                if matches:
                    log.info(f"Found {len(matches)} matches from __NEXT_DATA__ JSON script!")
            except Exception as e:
                log.warning(f"Error parsing __NEXT_DATA__: {e}")

        # ২. সরাসরি strmfree.st/streams API ব্যাকআপ চেক
        if not matches:
            try:
                api_resp = await client.get(
                    STREAM_API,
                    headers={"User-Agent": STREAM_UA, "Referer": SITE_REFERER, "Origin": BASE_URL},
                    timeout=10.0
                )
                if api_resp.status_code == 200:
                    api_data = api_resp.json()
                    matches = parse_next_data_json(api_data)
                    if matches:
                        log.info(f"Found {len(matches)} matches from {STREAM_API} API!")
            except Exception as e:
                log.warning(f"Error calling {STREAM_API}: {e}")

        # ৩. HTML কার্ড ফলব্যাক (Regex দিয়ে লিঙ্ক খোঁজা)
        if not matches:
            # পেজের ভেতর যেখানেই /player/ আছে তা বের করা
            for player_path in set(re.findall(r'["\'](/player/[^"\']+)["\']', resp.text)):
                full_player_url = urljoin(BASE_URL, player_path)
                slug_match = re.search(r'/player/([^/]+)/([^/?#]+)', player_path)
                if slug_match:
                    sport = slug_match.group(1).capitalize()
                    title = slug_match.group(2).replace("-vs-", " vs ").replace("-", " ").title()
                    matches.append(ScrapedMatch(
                        sport=sport,
                        title=cleanup_text(title),
                        site_status="UPCOMING",
                        start_dt=datetime.now(timezone.utc),
                        player_url=full_player_url
                    ))

    except Exception as e:
        log.error(f"Error scraping {BASE_URL}: {e}")

    # ডুপ্লিকেশন দূর করা
    unique_matches: dict[str, ScrapedMatch] = {}
    for m in matches:
        slug = create_slug(m.title)
        if slug not in unique_matches:
            unique_matches[slug] = m

    log.info(f"Scraped {len(unique_matches)} unique matches from {BASE_URL}")
    return list(unique_matches.values())

async def main():
    log.info(f"Starting StreamFree Scraper execution on {BASE_URL}...")
    async with httpx.AsyncClient(headers={"User-Agent": STREAM_UA}, follow_redirects=True, http2=True) as client:
        matches = await scrape_streamfree_homepage(client)
        if not matches:
            log.warning("No matches found from StreamFree!")
            return

        final_playlist = []

        for m in matches:
            start_bst = m.start_dt.astimezone(BST_TZ)
            start_time_str = start_bst.strftime("%b %d, %Y, %I:%M %p")
            end_time_str = (start_bst + timedelta(hours=3)).strftime("%b %d, %Y, %I:%M %p")

            # স্ট্রিম লিঙ্ক সংগ্রহ
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

        log.info(f"Done! Saved {len(final_playlist)} matches and {stream_count} active live streams in M3U.")

if __name__ == "__main__":
    asyncio.run(main())
