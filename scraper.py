import asyncio
import html
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import httpx

# রুট পাথ ও কনফিগারেশন
ROOT_DIR = Path(__file__).resolve().parent

BASE_URL = "https://strmfree.st"
ALT_BASE = "https://streamfree.top"
STREAM_API = "https://strmfree.st/streams"
SOURCES_API_TEMPLATE = "https://strmfree.st/api/v1/sources/{stream_key}"
GET_SERVER_TEMPLATE = "https://strmfree.st/get-stream-key/{stream_key}"
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

async def get_server_type(client: httpx.AsyncClient, stream_key: str) -> str:
    """সাইটের window.sfFetch('/get-stream-key/' + stream_key) অনুকূল করা"""
    try:
        url = GET_SERVER_TEMPLATE.format(stream_key=stream_key)
        headers = {
            "User-Agent": STREAM_UA,
            "Referer": f"{BASE_URL}/",
            "Origin": BASE_URL,
            "Accept": "application/json, text/plain, */*"
        }
        resp = await client.get(url, headers=headers, timeout=6.0)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("server_name") or "externalcdn"
    except Exception:
        pass
    return "externalcdn"

async def resolve_embed_tokens(client: httpx.AsyncClient, embed_url: str) -> dict:
    """এমবেড পেজ থেকে const _0x = {...} টোকেন ডিকশনারি ও VARIANT বের করে"""
    tokens = {}
    variant = ""
    try:
        headers = {
            "User-Agent": STREAM_UA,
            "Referer": f"{BASE_URL}/",
            "Origin": BASE_URL,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
        }
        resp = await client.get(embed_url, headers=headers, follow_redirects=True, timeout=8.0)
        if resp.status_code == 200:
            html_text = resp.text

            # VARIANT খোঁজা (যেমন: const VARIANT = 'canelovsmbilli' + QUALITY + ...)
            v_match = re.search(r"const\s+VARIANT\s*=\s*['\"]([^'\"]+)['\"]", html_text)
            if v_match:
                variant = v_match.group(1)

            # _0x টোকেন অবজেক্ট পার্স করা (স্ক্রিনশটের লাইন ১৪৪)
            # const _0x = {"1080p": {"_e": ..., "_n": ..., "_t": ...}, ...};
            match_0x = re.search(r"const\s+_0x\s*=\s*(\{.*?\});", html_text)
            if match_0x:
                tokens = json.loads(match_0x.group(1))
    except Exception as e:
        log.debug(f"Error parsing embed tokens from {embed_url}: {e}")

    return {"tokens": tokens, "variant": variant}

async def resolve_match_channels(client: httpx.AsyncClient, match: ScrapedMatch) -> list[dict]:
    """
    স্ক্রিনশটের আসল জাভাস্ক্রিপ্ট সূত্রের হুবহু পাইথন বাস্তবায়ন:
    /live-externalcdn/{VARIANT}/index.m3u8?_t={_t}&_e={_e}&_n={_n}
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
            resp = await client.get(sources_url, headers=headers, timeout=8.0)
            if resp.status_code != 200:
                return channels

            data = resp.json()
            sources_list = data.get("sources") or data.get("live_sources") or []
            if not isinstance(sources_list, list):
                return channels

            # সার্ভার নেম সংগ্রহ (যেমন: externalcdn)
            server_name = await get_server_type(client, match.stream_key)

            for s in sources_list:
                if not isinstance(s, dict):
                    continue

                quality = s.get("quality", "FHD")
                embed_url = s.get("url", "")
                if not embed_url:
                    continue

                # কোয়ালিটি কী তৈরি (যেমন: 1080p বা 540p)
                q_key = "1080p" if "1080" in embed_url or quality.upper() == "FHD" else "540p"
                
                # এমবেড পেজ থেকে টোকেন সংগ্রহ
                embed_data = await resolve_embed_tokens(client, embed_url)
                tokens_dict = embed_data.get("tokens", {})
                variant = embed_data.get("variant") or f"{match.stream_key}{q_key}"

                # টোকেন প্যারামিটার (_t, _e, _n)
                p = tokens_dict.get(q_key) or tokens_dict.get("1080p") or tokens_dict.get("540p")
                
                if p and isinstance(p, dict):
                    # সাইটের লাইন ১৬৮-১৭৭ এর হুবহু সূত্র
                    if server_name == 'externalcdn':
                        path = f"/live-externalcdn/{variant}/index.m3u8"
                    elif server_name != 'origin':
                        path = f"/live-cdn/{variant}/index.m3u8"
                    else:
                        path = f"/live/{variant}/index.m3u8"

                    query = f"?_t={p.get('_t')}&_e={p.get('_e')}&_n={p.get('_n')}"
                    m3u8_link = f"{BASE_URL}{path}{query}"

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
            log.debug(f"Error building channels for {match.stream_key}: {e}")

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

        live_matches = [
            m for m in matches 
            if calculate_match_status(m.start_dt, early_minutes=15) == "LIVE"
        ]

        log.info(f"Building active stream links for {len(live_matches)} LIVE matches...")
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
                m3u_lines.append(f'#EXTINF:-1 tvg-id="Live.Event.us" tvg-logo="{tvg_logo}" group-title="{ev["sport"]}", {ev["title"]} | {ch["channel_name"]}')
                m3u_lines.append(f'#EXTVLCOPT:http-referrer={BASE_URL}/')
                m3u_lines.append(f'#EXTVLCOPT:http-user-agent={STREAM_UA}')
                m3u_lines.append(ch["raw_link"])
                stream_count += 1

        M3U_OUTPUT.write_text("\n".join(m3u_lines) + "\n", encoding="utf-8")

        log.info(f"Done! Successfully generated {len(final_playlist)} matches and {stream_count} active playable streams in M3U.")

if __name__ == "__main__":
    asyncio.run(main())
