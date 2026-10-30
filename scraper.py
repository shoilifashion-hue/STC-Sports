import asyncio
import base64
import html
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urljoin, urlsplit
from zoneinfo import ZoneInfo

import httpx
from selectolax.lexbor import LexborHTMLParser as HTMLParser

ROOT_DIR = Path(__file__).resolve().parent

API_URL = "https://ovogoal.pages.dev/api/v2/flyembed2.json"
BASE_URL = "https://pirlotvfr.online"
TAG = "PIRLOTV"

JSON_OUTPUT = ROOT_DIR / "strmcntr.json"
M3U_OUTPUT = ROOT_DIR / "strmcntr.m3u"
CACHE_DIR = ROOT_DIR / "caches"
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# আপনার ব্রাউজারের হুবহু User-Agent
STREAM_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/156.0.0.0 Safari/537.36"
)

HTTP_SEMAPHORE = asyncio.Semaphore(5)
BST_TZ = ZoneInfo("Asia/Dhaka")

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")
log = logging.getLogger(TAG)

@dataclass
class ScrapedMatch:
    sport: str
    title: str
    start_dt: datetime
    stream_urls: list[str] = field(default_factory=list)
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

def calculate_match_status(start_dt: datetime, has_live_stream: bool, early_minutes: int = 15) -> str:
    # যদি অলরেডি m3u8 স্ট্রিম চালু থাকে, তবে সরাসরি LIVE
    if has_live_stream:
        return "LIVE"

    now_utc = datetime.now(timezone.utc)
    start_utc = start_dt.astimezone(timezone.utc)
    live_start = start_utc - timedelta(minutes=early_minutes)

    if now_utc < live_start:
        return "UPCOMING"
    elif live_start <= now_utc <= (start_utc + timedelta(hours=3, minutes=30)):
        return "LIVE"
    return "ENDED"

def parse_match_time(time_str: str) -> datetime:
    now = datetime.now(timezone.utc)
    m = re.search(r'(\d{1,2}):(\d{2})', str(time_str))
    if m:
        try:
            hour = int(m.group(1))
            minute = int(m.group(2))
            candidate = datetime(now.year, now.month, now.day, hour, minute, tzinfo=timezone.utc)
            if candidate < now - timedelta(hours=14):
                candidate += timedelta(days=1)
            elif candidate > now + timedelta(hours=14):
                candidate -= timedelta(days=1)
            return candidate
        except Exception:
            pass
    return now

def unpack_dean_edwards(script: str) -> str:
    if not script:
        return ""
    ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

    def decode_radix(val: str, radix: int) -> int:
        if radix <= 36:
            return int(val, radix)
        num = 0
        for char in val:
            idx = ALPHABET.index(char)
            num = num * radix + idx
        return num

    pattern = r"}\s*\(\s*['\"](.*?)['\"]\s*,\s*(\d+)\s*,\s*(\d+)\s*,\s*['\"](.*?)['\"]\s*\.split\s*\(\s*['\"][|]['\"]\s*\)"
    unpacked_pieces = []

    for match in re.finditer(pattern, script, re.DOTALL):
        payload, radix_str, count_str, symtab_str = match.groups()
        radix = int(radix_str)
        symtab = symtab_str.split('|')

        def replace_token(m):
            token = m.group(0)
            try:
                idx = decode_radix(token, radix)
                if idx < len(symtab) and symtab[idx]:
                    return symtab[idx]
            except Exception:
                pass
            return token

        unpacked = re.sub(r'\b\w+\b', replace_token, payload)
        unpacked_pieces.append(unpacked)

    return "\n".join(unpacked_pieces)

def extract_m3u8(text: str) -> str | None:
    if not text:
        return None

    unpacked_text = unpack_dean_edwards(text)
    full_text = text + "\n" + unpacked_text
    clean_text = html.unescape(full_text).replace(r"\/", "/")

    # ১. tims-001.sytes.net এর আসল লিঙ্ক (স্ক্রিনশট অনুযায়ী)
    secure_match = re.search(r'(https?://[a-zA-Z0-9_\-\.]*sytes\.net[^\s"\'<>\\]*?\.m3u8(?:\?[^\s"\'<>\\]+)?)', clean_text)
    if secure_match:
        return secure_match.group(1).strip("\"' ")

    # ২. Clappr বা Player এর source: "https://..."
    source_match = re.search(r'(?:source|file|src|stream|hls)\s*[:=]\s*["\'](https?://[^"\']+\.m3u8(?:\?[^"\']*)?)["\']', clean_text, re.IGNORECASE)
    if source_match:
        return source_match.group(1).strip("\"' ")

    # ৩. যেকোনো সরাসরি .m3u8
    generic_match = re.search(r'(https?://[^\s"\'<>\\]+?\.m3u8(?:\?[^\s"\'<>\\]+)?)', clean_text)
    if generic_match:
        url = generic_match.group(1).strip("\"' ")
        if "flyembed" not in url:
            return url

    # ৪. Base64 atob() চেক
    for b64 in re.findall(r'atob\s*\(\s*["\']([A-Za-z0-9+/=]{20,})["\']\s*\)', full_text):
        try:
            decoded = base64.b64decode(b64).decode("utf-8", errors="ignore")
            found = extract_m3u8(decoded)
            if found:
                return found
        except Exception:
            pass

    return None

async def resolve_pirlo_stream(client: httpx.AsyncClient, raw_iframe_url: str) -> tuple[str | None, str | None]:
    """
    flyembed.cfd -> ehgembxe.cfd -> tims-001.sytes.net m3u8
    """
    async with HTTP_SEMAPHORE:
        iframe_url = raw_iframe_url.strip()
        if iframe_url.startswith("//"):
            iframe_url = f"https:{iframe_url}"
        elif not iframe_url.startswith("http"):
            iframe_url = f"https://{iframe_url}"

        try:
            # ধাপ ১: flyembed.cfd/embed/41.php ফেচ করা
            headers_fly = {
                "User-Agent": STREAM_UA,
                "Referer": "https://pirlotvfr.online/",
                "Sec-Fetch-Dest": "iframe",
                "Sec-Fetch-Mode": "navigate",
                "Sec-Fetch-Site": "cross-site",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
            }

            resp = await client.get(iframe_url, headers=headers_fly, follow_redirects=True, timeout=12.0)
            if resp.status_code != 200:
                return None, None

            # পেজে সরাসরি m3u8 পেলে
            m3u8 = extract_m3u8(resp.text)
            if m3u8:
                return m3u8, str(resp.url)

            # ধাপ ২: ভেতরের আসল প্লেয়ার আইফ্রেম খোঁজা (ehgembxe.cfd)
            soup = HTMLParser(resp.content)
            target_iframes = []
            for iframe in soup.css("iframe"):
                src = iframe.attributes.get("src", "")
                if src and not src.startswith("javascript:"):
                    target_iframes.append(urljoin(str(resp.url), src))

            # regex দিয়েও কোনো লুকানো আইফ্রেম বা স্ক্রিপ্ট ইউআরএল খোঁজা
            for match_src in re.findall(r'src=["\']([^"\']*(?:ehgembxe|embed|player|live)[^"\']*)["\']', resp.text):
                full_src = urljoin(str(resp.url), match_src)
                if full_src not in target_iframes:
                    target_iframes.append(full_src)

            # ধাপ ৩: ehgembxe.cfd পেজগুলো ব্রাউজার হেডারে ফেচ করা
            for inner_url in target_iframes:
                parsed_inner = urlsplit(inner_url)
                origin_inner = f"{parsed_inner.scheme}://{parsed_inner.netloc}"

                headers_ehg = {
                    "User-Agent": STREAM_UA,
                    "Referer": str(resp.url),
                    "Origin": origin_inner,
                    "Sec-Fetch-Dest": "iframe",
                    "Sec-Fetch-Mode": "navigate",
                    "Sec-Fetch-Site": "cross-site",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
                }

                resp_inner = await client.get(inner_url, headers=headers_ehg, timeout=12.0)
                if resp_inner.status_code == 200:
                    found_m3u8 = extract_m3u8(resp_inner.text)
                    if found_m3u8:
                        return found_m3u8, origin_inner

                    # যদি ehgembxe এর ভেতরে আরও একটি নেস্টেড আইফ্রেম থাকে
                    inner_soup = HTMLParser(resp_inner.content)
                    for deep_iframe in inner_soup.css("iframe"):
                        deep_src = deep_iframe.attributes.get("src", "")
                        if deep_src and not deep_src.startswith("javascript:"):
                            deep_url = urljoin(inner_url, deep_src)
                            resp_deep = await client.get(deep_url, headers={"User-Agent": STREAM_UA, "Referer": inner_url}, timeout=10.0)
                            if resp_deep.status_code == 200:
                                deep_m3u8 = extract_m3u8(resp_deep.text)
                                if deep_m3u8:
                                    return deep_m3u8, origin_inner

        except Exception as e:
            log.debug(f"Stream resolution error for {iframe_url}: {e}")

        return None, None

async def fetch_pirlotv_matches(client: httpx.AsyncClient) -> list[ScrapedMatch]:
    matches = []
    try:
        headers = {"User-Agent": STREAM_UA, "Referer": BASE_URL}
        resp = await client.get(API_URL, headers=headers, timeout=15.0)
        if resp.status_code != 200:
            log.error(f"API request failed: {resp.status_code}")
            return matches

        data = resp.json()
        if not isinstance(data, list):
            return matches

        for m in data:
            if not isinstance(m, dict):
                continue

            t1 = cleanup_text(str(m.get("Team1", "")).strip())
            t2 = cleanup_text(str(m.get("Team2", "")).strip())
            title = f"{t1} vs. {t2}" if (t1 and t2) else (t1 or t2 or "Sports Event")

            sport = cleanup_text(str(m.get("League", "") or m.get("Sport", "") or "Football"))
            start_time_raw = m.get("MatchStartTime", "")
            dt = parse_match_time(start_time_raw)

            team_a_logo = m.get("Team1Logo") or None
            team_b_logo = m.get("Team2Logo") or None
            main_logo = team_a_logo or m.get("LeagueLogo") or team_b_logo

            iframe_url = m.get("IframeURL", "").strip()
            if iframe_url.startswith("//"):
                iframe_url = f"https:{iframe_url}"

            stream_links = [iframe_url] if iframe_url else []

            matches.append(ScrapedMatch(
                sport=sport,
                title=title,
                start_dt=dt,
                stream_urls=stream_links,
                team_a_logo=team_a_logo,
                team_b_logo=team_b_logo,
                logo=main_logo
            ))

        log.info(f"Successfully loaded {len(matches)} matches directly from backend API!")
    except Exception as e:
        log.error(f"Error fetching API: {e}")

    unique_map: dict[str, ScrapedMatch] = {}
    for m in matches:
        slug = create_slug(m.title)
        if slug in unique_map:
            for lk in m.stream_urls:
                if lk not in unique_map[slug].stream_urls:
                    unique_map[slug].stream_urls.append(lk)
        else:
            unique_map[slug] = m

    return list(unique_map.values())

async def main():
    log.info("Starting Pirlo TV Scraper execution...")
    async with httpx.AsyncClient(headers={"User-Agent": STREAM_UA}, follow_redirects=True, http2=True) as client:
        matches = await fetch_pirlotv_matches(client)
        if not matches:
            log.warning("No matches found from Pirlo TV API!")
            return

        final_playlist = []

        for m in matches:
            start_bst = m.start_dt.astimezone(BST_TZ)
            start_time_str = start_bst.strftime("%b %d, %Y, %I:%M %p")
            end_time_str = (start_bst + timedelta(hours=3)).strftime("%b %d, %Y, %I:%M %p")

            # স্ট্রিম লিঙ্ক রেজোলিউশন
            tasks = [resolve_pirlo_stream(client, url) for url in m.stream_urls]
            results = await asyncio.gather(*tasks)

            event_channels = []
            idx = 1
            for req_url, (m3u8_url, referer_used) in zip(m.stream_urls, results):
                if m3u8_url:
                    # আপনার ৩য় স্ক্রিনশট অনুযায়ী নিখুঁত Referer ও Origin
                    ref = referer_used or "https://ehgembxe.cfd/"
                    if not ref.endswith("/"):
                        ref += "/"
                    origin = ref.rstrip("/")

                    raw_link = f"{m3u8_url}|User-Agent={STREAM_UA}&Referer={ref}&Origin={origin}"
                    event_channels.append({
                        "channel_name": f"Stream #{idx}",
                        "stream_type": "HLS",
                        "url": m3u8_url,
                        "raw_link": raw_link,
                        "headers": {
                            "User-Agent": STREAM_UA,
                            "Referer": ref,
                            "Origin": origin
                        },
                        "drm_key": None
                    })
                    log.info(f"[{m.title}] SUCCESS -> Extracted m3u8: {m3u8_url}")
                    idx += 1

            has_live_stream = len(event_channels) > 0
            actual_status = calculate_match_status(m.start_dt, has_live_stream, early_minutes=15)
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
                ref_header = ch.get("headers", {}).get("Referer", "https://ehgembxe.cfd/")
                m3u_lines.append(f'#EXTINF:-1 tvg-id="Live.Event.us" tvg-logo="{tvg_logo}" group-title="{ev["sport"]}", {ev["title"]}')
                m3u_lines.append(f'#EXTVLCOPT:http-referrer={ref_header}')
                m3u_lines.append(f'#EXTVLCOPT:http-user-agent={STREAM_UA}')
                m3u_lines.append(ch["raw_link"])
                stream_count += 1

        M3U_OUTPUT.write_text("\n".join(m3u_lines) + "\n", encoding="utf-8")

        log.info(f"Done! Saved {len(final_playlist)} matches and {stream_count} active live streams in M3U.")

if __name__ == "__main__":
    asyncio.run(main())
