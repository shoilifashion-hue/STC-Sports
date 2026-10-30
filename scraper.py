import asyncio
import base64
import html
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import parse_qs, unquote, urljoin, urlsplit
from zoneinfo import ZoneInfo

import httpx
from selectolax.lexbor import LexborHTMLParser as HTMLParser

ROOT_DIR = Path(__file__).resolve().parent

BASE_URL = "https://pirlotvfr.online"
TAG = "PIRLOTV"

JSON_OUTPUT = ROOT_DIR / "strmcntr.json"
M3U_OUTPUT = ROOT_DIR / "strmcntr.m3u"
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
    row_text: str
    start_dt: datetime
    watch_urls: list[str] = field(default_factory=list)
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

def calculate_match_status(start_dt: datetime, row_text: str, early_minutes: int = 15) -> str:
    """১৫ মিনিট আগে LIVE হবে, এবং সোর্সে শেষ (Finalizada/Ended/FT) হলে কেবল ENDED হবে"""
    upper_txt = row_text.upper()
    is_ended = bool(re.search(r'\b(ENDED|FINISHED|FINAL|FINALIZADA|COMPLETED|FULL[\s-]?TIME|\bFT\b)\b', upper_txt))
    if is_ended:
        return "ENDED"

    now_utc = datetime.now(timezone.utc)
    start_utc = start_dt.astimezone(timezone.utc)
    live_start = start_utc - timedelta(minutes=early_minutes)

    if now_utc < live_start:
        return "UPCOMING"
    return "LIVE"

def parse_match_time(row_text: str) -> datetime:
    """সাইটের '23:30 UTC' অথবা '01:00 UTC' ফরম্যাট থেকে নিখুঁত UTC সময় পার্স করে"""
    now = datetime.now(timezone.utc)
    m = re.search(r'(\d{1,2}):(\d{2})\s*(?:UTC)?', row_text, re.IGNORECASE)
    if m:
        try:
            hour = int(m.group(1))
            minute = int(m.group(2))
            # আজকের দিনের টাইম
            candidate = datetime(now.year, now.month, now.day, hour, minute, tzinfo=timezone.utc)
            # যদি সময়টি ইতিমধ্যে ১২ ঘণ্টার বেশি পেছনে চলে যায়, তবে এটি সম্ভবত আগামীকালের ম্যাচ
            if candidate < now - timedelta(hours=14):
                candidate += timedelta(days=1)
            elif candidate > now + timedelta(hours=14):
                candidate -= timedelta(days=1)
            return candidate
        except Exception:
            pass
    return now

def extract_m3u8(text: str, base_url: str = "") -> str | None:
    if not text:
        return None

    clean_text = html.unescape(text).replace(r"\/", "/")

    # ১. tims-001.sytes.net বা secure m3u8 প্যাটার্ন (৩য় স্ক্রিনশট অনুযায়ী)
    secure_match = re.search(r'(https?://[a-zA-Z0-9_\-\.]*sytes\.net[^\s"\'<>\\]*?\.m3u8(?:\?[^\s"\'<>\\]+)?)', clean_text)
    if secure_match:
        return secure_match.group(1).strip("\"' ")

    # ২. Clappr বা Player কনফিগে source / file / hls
    source_match = re.search(r'(?:source|file|src|stream|hls)\s*[:=]\s*["\'](https?://[^"\']+\.m3u8(?:\?[^"\']*)?)["\']', clean_text, re.IGNORECASE)
    if source_match:
        return source_match.group(1).strip("\"' ")

    # ৩. যেকোনো স্ট্যান্ডার্ড m3u8
    generic_match = re.search(r'(https?://[a-zA-Z0-9_\-\.]+(?::\d+)?/[^\s"\'<>\\]+?\.m3u8(?:\?[^\s"\'<>\\]+)?)', clean_text)
    if generic_match:
        return generic_match.group(1).strip("\"' ")

    # ৪. Base64 atob() থাকলে ডিকোড করে খোঁজা
    for b64 in re.findall(r'atob\(["\']([A-Za-z0-9+/=]{20,})["\']\)', text):
        try:
            decoded = base64.b64decode(b64).decode("utf-8", errors="ignore")
            found = extract_m3u8(decoded, base_url)
            if found:
                return found
        except Exception:
            pass

    return None

async def resolve_pirlo_stream(client: httpx.AsyncClient, watch_url: str) -> tuple[str | None, str | None]:
    """
    watch?url=... থেকে আসল flyembed প্লেয়ার পেজে ঢুকে টোকেনযুক্ত m3u8 এবং Referer বের করে
    """
    async with HTTP_SEMAPHORE:
        try:
            # ১. ওয়াচ পেজের কুয়েরি প্যারামিটার থেকে এমবেড লিঙ্ক আলাদা করা
            qs = parse_qs(urlsplit(watch_url).query)
            embed_url = qs.get("url", [None])[0]
            if embed_url:
                embed_url = unquote(embed_url)
            else:
                embed_url = watch_url

            headers = {
                "User-Agent": STREAM_UA,
                "Referer": BASE_URL,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
            }

            resp = await client.get(embed_url, headers=headers, follow_redirects=True, timeout=12.0)
            if resp.status_code != 200:
                return None, None

            # সোর্স থেকে m3u8 খোঁজা
            m3u8 = extract_m3u8(resp.text, base_url=str(resp.url))
            if m3u8:
                return m3u8, str(resp.url)

            # পেজের ভেতর আইফ্রেম থাকলে তাতে যাওয়া
            soup = HTMLParser(resp.content)
            for iframe in soup.css("iframe"):
                src = iframe.attributes.get("src", "")
                if src and not src.startswith("javascript:"):
                    iframe_url = urljoin(str(resp.url), src)
                    resp_iframe = await client.get(
                        iframe_url,
                        headers={"User-Agent": STREAM_UA, "Referer": str(resp.url)},
                        timeout=10.0
                    )
                    if resp_iframe.status_code == 200:
                        inner_m3u8 = extract_m3u8(resp_iframe.text, base_url=iframe_url)
                        if inner_m3u8:
                            return inner_m3u8, iframe_url

        except Exception as e:
            log.debug(f"Stream resolution error for {watch_url}: {e}")
        return None, None

async def scrape_pirlotv_matches(client: httpx.AsyncClient) -> list[ScrapedMatch]:
    matches = []
    try:
        resp = await client.get(BASE_URL, headers={"User-Agent": STREAM_UA}, timeout=15.0)
        if resp.status_code != 200:
            log.error(f"Failed to fetch {BASE_URL}: Status {resp.status_code}")
            return matches

        soup = HTMLParser(resp.content)
        # প্রতিটি খেলার রো বা কার্ড খোঁজা
        rows = soup.css(".match-row, .partido, .evento, [class*='partido'], [class*='match'], tr")
        if not rows:
            rows = soup.css("div[class*='row'], li")

        for row in rows:
            text = row.text()
            if not ("Ver stream" in text or "watch" in text.lower() or "UTC" in text):
                continue

            # বাটনের ওয়াচ লিংক সংগ্রহ
            links = []
            for a in row.css("a"):
                href = a.attributes.get("href", "")
                if "watch" in href or "url=" in href:
                    full_link = urljoin(BASE_URL, href)
                    if full_link not in links:
                        links.append(full_link)

            if not links:
                continue

            # টাইটেল ও স্পোর্টস ক্যাটাগরি বের করা
            title = ""
            sport = "Football"

            # স্পোর্টস বা লিগের নাম
            sport_elem = row.css_first(".sport, .league, .competicion, [class*='league'], [class*='sport']")
            if sport_elem:
                sport = cleanup_text(sport_elem.text())

            # দল দুটির নাম
            teams_elems = row.css(".team, .equipo, [class*='team'], strong")
            if len(teams_elems) >= 2:
                team1 = cleanup_text(teams_elems[0].text())
                team2 = cleanup_text(teams_elems[1].text())
                if team1 and team2:
                    title = f"{team1} vs. {team2}"

            if not title:
                # টেক্সট থেকে vs খোঁজা
                for line in text.split("\n"):
                    cl = cleanup_text(line)
                    if " vs " in cl.lower() or " vs. " in cl.lower():
                        title = cl
                        break

            if not title or len(title) < 3:
                continue

            title = cleanup_text(title)
            dt = parse_match_time(text)

            # লোগো সংগ্রহ
            valid_logos = []
            for img in row.css("img"):
                src = img.attributes.get("src", "")
                if src and not src.startswith("data:"):
                    full_img = urljoin(BASE_URL, src)
                    if full_img not in valid_logos:
                        valid_logos.append(full_img)

            team_a_logo = valid_logos[0] if len(valid_logos) >= 1 else None
            team_b_logo = valid_logos[1] if len(valid_logos) >= 2 else None
            main_logo = team_a_logo or team_b_logo

            matches.append(ScrapedMatch(
                sport=sport,
                title=title,
                row_text=text,
                start_dt=dt,
                watch_urls=links,
                team_a_logo=team_a_logo,
                team_b_logo=team_b_logo,
                logo=main_logo
            ))

        log.info(f"Successfully scraped {len(matches)} matches from {BASE_URL}")
    except Exception as e:
        log.error(f"Error scraping {BASE_URL}: {e}")

    # ডুপ্লিকেশন দূর করা
    unique_map: dict[str, ScrapedMatch] = {}
    for m in matches:
        slug = create_slug(m.title)
        if slug in unique_map:
            for lk in m.watch_urls:
                if lk not in unique_map[slug].watch_urls:
                    unique_map[slug].watch_urls.append(lk)
        else:
            unique_map[slug] = m

    return list(unique_map.values())

async def main():
    log.info(f"Starting Pirlo TV Scraper execution ({BASE_URL})...")
    async with httpx.AsyncClient(headers={"User-Agent": STREAM_UA}, follow_redirects=True, http2=True) as client:
        matches = await scrape_pirlotv_matches(client)
        if not matches:
            log.warning("No matches found from Pirlo TV!")
            return

        final_playlist = []

        for m in matches:
            start_bst = m.start_dt.astimezone(BST_TZ)
            start_time_str = start_bst.strftime("%b %d, %Y, %I:%M %p")
            end_time_str = (start_bst + timedelta(hours=3)).strftime("%b %d, %Y, %I:%M %p")

            # স্ট্রিম লিঙ্ক ফেচিং
            tasks = [resolve_pirlo_stream(client, url) for url in m.watch_urls]
            results = await asyncio.gather(*tasks)

            event_channels = []
            idx = 1
            for req_url, (m3u8_url, referer_used) in zip(m.watch_urls, results):
                if m3u8_url:
                    ref = referer_used or "https://flyembed.cfd/"
                    # স্ক্রিনশট ৩ অনুযায়ী নিখুঁত পাইপলাইন হেডার তৈরি
                    raw_link = f"{m3u8_url}|User-Agent={STREAM_UA}&Referer={ref}&Origin={ref}"
                    event_channels.append({
                        "channel_name": f"Stream #{idx}",
                        "stream_type": "HLS",
                        "url": m3u8_url,
                        "raw_link": raw_link,
                        "headers": {
                            "User-Agent": STREAM_UA,
                            "Referer": ref,
                            "Origin": ref
                        },
                        "drm_key": None
                    })
                    idx += 1

            actual_status = calculate_match_status(m.start_dt, m.row_text, early_minutes=15)
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
                ref_header = ch.get("headers", {}).get("Referer", "https://flyembed.cfd/")
                m3u_lines.append(f'#EXTINF:-1 tvg-id="Live.Event.us" tvg-logo="{tvg_logo}" group-title="{ev["sport"]}", {ev["title"]}')
                m3u_lines.append(f'#EXTVLCOPT:http-referrer={ref_header}')
                m3u_lines.append(f'#EXTVLCOPT:http-user-agent={STREAM_UA}')
                m3u_lines.append(ch["raw_link"])
                stream_count += 1

        M3U_OUTPUT.write_text("\n".join(m3u_lines) + "\n", encoding="utf-8")

        log.info(f"Done! Successfully generated {len(final_playlist)} matches and {stream_count} active playable streams in M3U.")

if __name__ == "__main__":
    asyncio.run(main())
