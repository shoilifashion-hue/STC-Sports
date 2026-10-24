import asyncio
import base64
import json
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urljoin
from zoneinfo import ZoneInfo

import httpx
from selectolax.lexbor import LexborHTMLParser as HTMLParser

# রুট পাথ ও কনফিগারেশন
ROOT_DIR = Path(__file__).resolve().parent

BASE_URL = "https://streamec.st"
EMBED_REFERER = "https://embedcenter.st"
TAG = "STRMCNTR"

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

@dataclass(slots=True)
class ScrapedMatch:
    sport: str
    title: str
    status: str
    start_dt: datetime
    embed_urls: list[str]
    logo: str | None = None

def cleanup_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()

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

def parse_match_datetime(time_str: str) -> datetime:
    """স্ক্রিনশটের তারিখ 'Oct 24, 2026, 10:00 PM' পার্স করে"""
    time_str = cleanup_text(time_str)
    formats = [
        "%b %d, %Y, %I:%M %p",
        "%b %d, %Y, %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%B %d, %Y, %I:%M %p"
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(time_str, fmt)
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass
    return datetime.now(timezone.utc)

def extract_m3u8(text: str, base_embed_url: str = "") -> str | None:
    """HTML বা JS থেকে টোকেনযুক্ত সম্পূর্ণ m3u8 লিংক এক্সট্রাক্ট করে"""
    # JSON এসকেপড ও এইচটিএমএল এনটিটি ঠিক করা
    clean_text = text.replace(r"\/", "/").replace("&amp;", "&")

    # ১. edgestream টোকেনসহ লিঙ্ক (যেমন: https://edgestream1.pro/hls/...m3u8?st=...&e=...)
    edge_match = re.search(r'(https?://[a-zA-Z0-9_\-\.]*edgestream[^\s"\'<>\\]*?\.m3u8(?:\?[^\s"\'<>\\]+)?)', clean_text)
    if edge_match:
        return edge_match.group(1).strip("\"' ")

    # ২. যেকোনো স্ট্যান্ডার্ড পূর্ণ .m3u8 লিংক
    generic_match = re.search(r'(https?://[a-zA-Z0-9_\-\.]+(?::\d+)?/[^\s"\'<>\\]+?\.m3u8(?:\?[^\s"\'<>\\]+)?)', clean_text)
    if generic_match:
        return generic_match.group(1).strip("\"' ")

    # ৩. স্ক্রিনশটের zeuryuegn48.m3u8 এর মতো ফাইলনেম থাকলে
    rel_match = re.search(r'["\']([a-zA-Z0-9_\-\.]+\.m3u8(?:\?[^"\'\s]*)?)["\']', clean_text)
    if rel_match and base_embed_url:
        return urljoin(base_embed_url, rel_match.group(1)).strip("\"' ")

    # ৪. Base64 ডিকোড চেক
    b64_matches = re.findall(r'atob\(["\']([A-Za-z0-9+/=]{20,})["\']\)', text)
    for b64 in b64_matches:
        try:
            decoded = base64.b64decode(b64).decode("utf-8", errors="ignore")
            found = extract_m3u8(decoded, base_embed_url)
            if found:
                return found
        except Exception:
            pass

    return None

async def resolve_embed_stream(client: httpx.AsyncClient, embed_url: str) -> str | None:
    """যেমন: https://streamec.st/embed/ch48.php ফেচ করে আসল m3u8 বের করে"""
    async with HTTP_SEMAPHORE:
        try:
            resp = await client.get(
                embed_url,
                headers={"Referer": BASE_URL, "User-Agent": STREAM_UA},
                timeout=12.0
            )
            if resp.status_code != 200:
                return None

            # পেজের ভেতর থেকে সরাসরি m3u8 খোঁজা
            m3u8 = extract_m3u8(resp.text, base_embed_url=embed_url)
            if m3u8:
                return m3u8

            # ভেতরে iframe থাকলে তা ফেচ করা
            soup = HTMLParser(resp.content)
            iframe = soup.css_first("iframe")
            if iframe:
                src = iframe.attributes.get("src", "")
                if src:
                    inner_url = urljoin(embed_url, src)
                    resp_inner = await client.get(
                        inner_url,
                        headers={"Referer": embed_url, "User-Agent": STREAM_UA},
                        timeout=12.0
                    )
                    if resp_inner.status_code == 200:
                        return extract_m3u8(resp_inner.text, base_embed_url=inner_url)
        except Exception:
            pass
        return None

def parse_cards_soup(soup: HTMLParser, source_page_url: str) -> list[ScrapedMatch]:
    """Game Cards পেজের স্ট্রাকচার থেকে ম্যাচ ও এমবেড লিংক বের করে"""
    matches = []

    # ১. পুরোনো ও নতুন সব ধরনের ক্লাস সাপোর্ট (game-card-row, game-card-group ইত্যাদি)
    card_rows = soup.css(".game-card-row, .game-card, .match-card, [class*='card-row']")
    
    if card_rows:
        for row in card_rows:
            # টাইটেল
            title_node = row.css_first("h3, h4, .game-card-title, .title, strong")
            if not title_node:
                continue
            title = cleanup_text(title_node.text())

            # টাইম
            time_node = row.css_first("time, .game-card-when, [class*='when'], [class*='time']")
            time_str = cleanup_text(time_node.text()) if time_node else ""
            dt = parse_match_datetime(time_str)

            # স্ট্যাটাস
            row_text = row.text().upper()
            status = "LIVE" if "LIVE" in row_text else "UPCOMING"

            # স্পোর্ট
            sport = "Sports"
            parent = row.parent
            while parent:
                sport_h = parent.css_first("h2, .game-card-group-title, [class*='sport']")
                if sport_h and sport_h.text().strip():
                    sport = cleanup_text(sport_h.text())
                    break
                parent = parent.parent

            # Embed বাটনগুলোর লিঙ্ক খোঁজা
            links = []
            for a in row.css("a"):
                href = a.attributes.get("href", "")
                txt = cleanup_text(a.text()).lower()
                if "embed" in txt or "watch" in txt or "/embed/" in href or "/watch/" in href:
                    if href and not href.startswith("javascript:"):
                        full_link = urljoin(source_page_url, href)
                        if full_link not in links:
                            links.append(full_link)

            # লোগো
            img = row.css_first("img")
            logo = urljoin(source_page_url, img.attributes.get("src")) if img and img.attributes.get("src") else None

            if links:
                matches.append(ScrapedMatch(sport=sport, title=title, status=status, start_dt=dt, embed_urls=links, logo=logo))

    # ২. ফলব্যাক: বাটন বা <a> ট্যাগ ধরে খোঁজা (যদি সাইট নতুন DOM স্ট্রাকচার ব্যবহার করে)
    if not matches:
        embed_links = soup.css("a")
        for a in embed_links:
            href = a.attributes.get("href", "")
            btn_txt = cleanup_text(a.text()).lower()
            if not ("embed" in btn_txt or "/embed/" in href):
                continue

            full_embed = urljoin(source_page_url, href)
            # বাটনের প্যারেন্ট রো খোঁজা
            row_elem = a.parent
            for _ in range(4):
                if not row_elem:
                    break
                t_str = row_elem.text()
                if "vs" in t_str.lower() or " - " in t_str:
                    break
                row_elem = row_elem.parent

            if not row_elem:
                continue

            row_text = row_elem.text()
            # টাইটেল খোঁজা
            t_node = row_elem.css_first("h3, h4, strong")
            title = cleanup_text(t_node.text()) if t_node else ""
            if not title:
                for line in row_text.split("\n"):
                    cl = cleanup_text(line)
                    if "vs" in cl.lower() or " - " in cl:
                        title = cl
                        break

            if not title or len(title) < 3:
                continue

            # ডুপ্লিকেট এড়ানো
            if any(m.title == title for m in matches):
                continue

            status = "LIVE" if "LIVE" in row_text.upper() else "UPCOMING"
            
            # টাইম পার্স
            time_match = re.search(r'([A-Za-z]{3}\s+\d{1,2},\s+\d{4},\s+\d{1,2}:\d{2}\s+(?:AM|PM))', row_text)
            time_str = time_match.group(1) if time_match else ""
            dt = parse_match_datetime(time_str)

            img = row_elem.css_first("img")
            logo = urljoin(source_page_url, img.attributes.get("src")) if img and img.attributes.get("src") else None

            matches.append(ScrapedMatch(sport="Sports", title=title, status=status, start_dt=dt, embed_urls=[full_embed], logo=logo))

    return matches

async def scrape_all_sources(client: httpx.AsyncClient) -> list[ScrapedMatch]:
    """সবগুলো সক্রিয় পেজ ট্রাই করে ম্যাচ খুঁজে আনে"""
    PAGES_TO_TRY = [
        f"{BASE_URL}/game-cards",
        f"{BASE_URL}/game-cards/embed",
        "https://streamcenter.st/game-cards",
        f"{BASE_URL}/"
    ]

    for target_url in PAGES_TO_TRY:
        try:
            log.info(f"Trying to fetch match cards from: {target_url}")
            resp = await client.get(target_url, headers={"User-Agent": STREAM_UA, "Referer": BASE_URL}, timeout=15.0)
            if resp.status_code == 200:
                soup = HTMLParser(resp.content)
                matches = parse_cards_soup(soup, target_url)
                if matches:
                    log.info(f"Successfully scraped {len(matches)} matches from {target_url}!")
                    return matches
        except Exception as e:
            log.warning(f"Error fetching from {target_url}: {e}")

    return []

async def main():
    log.info("Starting STC Scraper execution...")
    async with httpx.AsyncClient(headers={"User-Agent": STREAM_UA, "Referer": BASE_URL}, follow_redirects=True, http2=True) as client:
        scraped_matches = await scrape_all_sources(client)
        if not scraped_matches:
            log.warning("No matches found from any target page!")
            return

        final_playlist = []
        now_utc = datetime.now(timezone.utc)

        for m in scraped_matches:
            start_bst = m.start_dt.astimezone(BST_TZ)

            # শুধুমাত্র LIVE অথবা শুরু হতে ১ ঘণ্টার কম বাকি থাকা ম্যাচের স্ট্রিম ফেচ করা
            time_until_start = m.start_dt - now_utc
            should_fetch_stream = (m.status == "LIVE") or (timedelta(0) <= time_until_start <= timedelta(hours=1))

            event_channels = []
            if should_fetch_stream:
                tasks = [resolve_embed_stream(client, url) for url in m.embed_urls]
                results = await asyncio.gather(*tasks)

                idx = 1
                for embed_page_url, m3u8_url in zip(m.embed_urls, results):
                    if m3u8_url:
                        raw_link = f"{m3u8_url}|User-Agent={STREAM_UA}&Referer={EMBED_REFERER}&Origin={EMBED_REFERER}"
                        event_channels.append({
                            "channel_name": f"Stream #{idx}",
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

            final_playlist.append({
                "title": m.title,
                "sport": m.sport,
                "sub_category": m.sport,
                "status": m.status,
                "is_hot": (m.status == "LIVE"),
                "slug": create_slug(m.title),
                "schedule": {
                    "timezone": "BST (UTC+6)",
                    "start_time_bst": start_bst.strftime("%b %d, %Y, %I:%M:%S %p"),
                    "end_time_bst": (start_bst + timedelta(hours=3)).strftime("%b %d, %Y, %I:%M:%S %p"),
                },
                "teams": parse_teams(m.title),
                "logo": m.logo,
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

        log.info(f"Done! Successfully generated {len(final_playlist)} matches and {stream_count} playable streams.")

if __name__ == "__main__":
    asyncio.run(main())
