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

def parse_start_time(text: str, parent_elem) -> datetime:
    """
    হোমপেজের কার্ডে থাকা সময় যেমন '2h 18m' অথবা ISO ডেট পার্স করে
    """
    now_utc = datetime.now(timezone.utc)
    
    # <time datetime="..."> থাকলে
    time_node = parent_elem.css_first("time") if parent_elem else None
    if time_node:
        iso_val = time_node.attributes.get("datetime", "")
        if iso_val:
            try:
                dt = datetime.fromisoformat(iso_val.replace("Z", "+00:00"))
                return dt.astimezone(timezone.utc)
            except Exception:
                pass

    # '2h 18m' বা '5h' ফরম্যাট হ্যান্ডেল করা
    rel_match = re.search(r'(\d+)\s*h(?:ours?)?\s*(\d+)?\s*m?', text, re.IGNORECASE)
    if rel_match:
        hours = int(rel_match.group(1))
        mins = int(rel_match.group(2)) if rel_match.group(2) else 0
        return now_utc + timedelta(hours=hours, minutes=mins)

    # শুধু '45m' থাকলে
    min_match = re.search(r'(\d+)\s*m(?:inutes?)?', text, re.IGNORECASE)
    if min_match:
        mins = int(min_match.group(1))
        return now_utc + timedelta(minutes=mins)

    # সরাসরি লাইভ থাকলে
    if "LIVE" in text.upper():
        return now_utc

    return now_utc

def calculate_match_status(start_dt: datetime, site_status: str, early_minutes: int = 15) -> str:
    """ম্যাচ শুরুর ১৫ মিনিট আগে LIVE হবে, এবং সোর্সে শেষ হলে ENDED হবে"""
    upper_status = site_status.upper()
    if any(k in upper_status for k in ["ENDED", "FINISHED", "FINAL", "FT"]):
        return "ENDED"

    now_utc = datetime.now(timezone.utc)
    start_utc = start_dt.astimezone(timezone.utc)
    live_start = start_utc - timedelta(minutes=early_minutes)

    if now_utc < live_start:
        return "UPCOMING"

    return "LIVE"

def extract_m3u8_links(text: str, base_url: str = "") -> list[str]:
    """জাভাস্ক্রিপ্ট এবং HTML থেকে টোকেনসহ সম্পূর্ণ index.m3u8 লিংক এক্সট্রাক্ট করে"""
    found = []
    if not text:
        return found

    clean_text = html.unescape(text).replace(r"\/", "/")

    # ৪ নম্বর স্ক্রিনশটের মতো index.m3u8 টোকেনসহ লিঙ্ক
    matches = re.findall(r'(https?://[a-zA-Z0-9_\-\.]+(?::\d+)?/[^\s"\'<>\\]*?index\.m3u8(?:\?[^\s"\'<>\\]+)?)', clean_text)
    for m in matches:
        link = m.strip("\"' ")
        if link not in found:
            found.append(link)

    # যেকোনো .m3u8 লিঙ্ক
    if not found:
        generic_matches = re.findall(r'(https?://[a-zA-Z0-9_\-\.]+(?::\d+)?/[^\s"\'<>\\]+?\.m3u8(?:\?[^\s"\'<>\\]+)?)', clean_text)
        for gm in generic_matches:
            link = gm.strip("\"' ")
            if link not in found:
                found.append(link)

    return found

async def resolve_stream_from_player(client: httpx.AsyncClient, player_url: str) -> list[str]:
    """প্লেয়ার পেজ থেকে সরাসরি টোকেনযুক্ত m3u8 এক্সট্রাক্ট করে"""
    async with HTTP_SEMAPHORE:
        stream_urls = []
        try:
            headers = {
                "User-Agent": STREAM_UA,
                "Referer": SITE_REFERER,
                "Origin": BASE_URL,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
            }
            resp = await client.get(player_url, headers=headers, follow_redirects=True, timeout=12.0)
            if resp.status_code != 200:
                return stream_urls

            # ১. প্লেয়ার পেজের টেক্সটে m3u8 আছে কি না
            stream_urls.extend(extract_m3u8_links(resp.text, base_url=str(resp.url)))
            if stream_urls:
                return stream_urls

            # ২. ৩য় স্ক্রিনশটে থাকা https://strmfree.st/streams কল করা
            api_headers = {
                "User-Agent": STREAM_UA,
                "Referer": player_url,
                "Origin": BASE_URL,
                "Accept": "application/json, text/plain, */*"
            }
            try:
                api_resp = await client.get(STREAM_API, headers=api_headers, timeout=8.0)
                if api_resp.status_code == 200:
                    api_urls = extract_m3u8_links(api_resp.text, base_url=str(api_resp.url))
                    stream_urls.extend(api_urls)
            except Exception:
                pass

            # ৩. পেজের ভেতরের iframe খোঁজা
            soup = HTMLParser(resp.content)
            for iframe in soup.css("iframe"):
                src = iframe.attributes.get("src", "")
                if src and not src.startswith("javascript:"):
                    iframe_url = urljoin(str(resp.url), src)
                    resp_iframe = await client.get(iframe_url, headers={"User-Agent": STREAM_UA, "Referer": player_url}, timeout=10.0)
                    if resp_iframe.status_code == 200:
                        iframe_m3u8 = extract_m3u8_links(resp_iframe.text, base_url=iframe_url)
                        stream_urls.extend(iframe_m3u8)

        except Exception as e:
            log.debug(f"Error fetching player {player_url}: {e}")

        # ডুপ্লিকেট দূর করা
        return list(dict.fromkeys(stream_urls))

async def scrape_streamfree_homepage(client: httpx.AsyncClient) -> list[ScrapedMatch]:
    """streamfree.top হোমপেজ থেকে সকল লাইভ ও আপকামিং ম্যাচ স্ক্র্যাপ করে"""
    matches = []
    try:
        resp = await client.get(BASE_URL, headers={"User-Agent": STREAM_UA}, timeout=15.0)
        if resp.status_code != 200:
            log.error(f"Failed to fetch homepage: Status {resp.status_code}")
            return matches

        soup = HTMLParser(resp.content)

        # কার্ডগুলো খোঁজা (Popular Streams এবং প্রতিটি স্পোর্টস সেকশন)
        cards = soup.css("a[href*='/player/'], div:has(> a[href*='/player/'])")
        if not cards:
            cards = soup.css("[class*='card'], [class*='stream']")

        for card in cards:
            # প্লেয়ার ইউআরএল
            link_node = card if card.tag == "a" else card.css_first("a[href*='/player/']")
            if not link_node:
                continue

            href = link_node.attributes.get("href", "")
            if not href or "/player/" not in href:
                continue

            full_player_url = urljoin(BASE_URL, href)

            # ক্যাটাগরি ও টাইটেল URL বা কার্ড টেক্সট থেকে শনাক্ত
            # ফরম্যাট: /player/{sport}/{match-slug}
            slug_match = re.search(r'/player/([^/]+)/([^/?#]+)', href)
            sport = slug_match.group(1).capitalize() if slug_match else "Sports"

            card_text = cleanup_text(card.text())
            title_node = card.css_first("h3, h4, .title, strong, [class*='title']")
            title = cleanup_text(title_node.text()) if title_node else ""
            
            if not title:
                # স্লাগ থেকে টাইটেল তৈরি (যেমন: brighton-hove-albion-vs-manchester-city)
                if slug_match:
                    raw_title = slug_match.group(2).replace("-vs-", " vs ").replace("-", " ")
                    title = raw_title.title()
                else:
                    title = card_text

            if not title or len(title) < 3:
                continue

            # স্ট্যাটাস ও স্টার্ট টাইম
            site_status = "LIVE" if "LIVE" in card_text.upper() else "UPCOMING"
            start_dt = parse_start_time(card_text, card)

            # টিম লোগো এক্সট্রাক্ট করা
            logos = []
            for img in card.css("img"):
                for attr in ["src", "data-src"]:
                    img_src = img.attributes.get(attr, "")
                    if img_src and not img_src.startswith("data:image"):
                        full_img = urljoin(BASE_URL, img_src)
                        if full_img not in logos:
                            logos.append(full_img)
                        break

            team_a_logo = logos[0] if len(logos) >= 1 else None
            team_b_logo = logos[1] if len(logos) >= 2 else None
            main_logo = team_a_logo or team_b_logo

            matches.append(ScrapedMatch(
                sport=sport,
                title=title,
                site_status=site_status,
                start_dt=start_dt,
                player_url=full_player_url,
                team_a_logo=team_a_logo,
                team_b_logo=team_b_logo,
                logo=main_logo
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

            # প্লেয়ার পেজ ও স্ট্রিমিং ব্যাকএন্ড থেকে আসল m3u8 খোঁজা
            stream_urls = await resolve_stream_from_player(client, m.player_url)

            event_channels = []
            idx = 1
            for m3u8_url in stream_urls:
                # হেডার পাইপলাইন (Origin ও Referer সহ)
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
