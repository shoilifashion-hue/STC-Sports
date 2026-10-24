import asyncio
import base64
import html
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urljoin, unquote
from zoneinfo import ZoneInfo

import httpx
from selectolax.lexbor import LexborHTMLParser as HTMLParser

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

@dataclass
class ScrapedMatch:
    sport: str
    title: str
    status: str
    start_dt: datetime
    embed_urls: list[str] = field(default_factory=list)
    logo: str | None = None

def cleanup_text(text: str) -> str:
    # Tigersvs.Auburn এর মতো জোড়া লাগা টেক্সট আলাদা করা
    text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
    text = re.sub(r'(vs\.?)([A-Za-z])', r'\1 \2', text, flags=re.IGNORECASE)
    text = re.sub(r'([A-Za-z])(vs\.?)', r'\1 \2', text, flags=re.IGNORECASE)
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

def extract_m3u8_from_content(text: str, base_url: str = "") -> str | None:
    """যেকোনো টেক্সট বা জাভাস্ক্রিপ্ট থেকে সম্পূর্ণ m3u8 লিংক এক্সট্রাক্ট করে"""
    if not text:
        return None

    # আন-এসকেপ ও কোডিং ক্লিনআপ
    clean_text = html.unescape(text).replace(r"\/", "/")

    # ১. edgestream টোকেনসহ লিঙ্ক (সবচেয়ে নিখুঁত প্যাটার্ন)
    edge_match = re.search(r'(https?://[a-zA-Z0-9_\-\.]*edgestream[^\s"\'<>\\]*?\.m3u8(?:\?[^\s"\'<>\\]+)?)', clean_text)
    if edge_match:
        return edge_match.group(1).strip("\"' ")

    # ২. Clappr, JWPlayer বা HLS.js এর সোর্স থেকে
    source_match = re.search(r'(?:source|file|src|stream|hls)\s*[:=]\s*["\'](https?://[^"\']+\.m3u8(?:\?[^"\']*)?)["\']', clean_text, re.IGNORECASE)
    if source_match:
        return source_match.group(1).strip("\"' ")

    # ৩. যেকোনো স্ট্যান্ডার্ড এইচএলএস m3u8
    generic_match = re.search(r'(https?://[a-zA-Z0-9_\-\.]+(?::\d+)?/[^\s"\'<>\\]+?\.m3u8(?:\?[^\s"\'<>\\]+)?)', clean_text)
    if generic_match:
        return generic_match.group(1).strip("\"' ")

    # ৪. রিলেটিভ ফাইল নেম (যেমন: zeuryuegn48.m3u8)
    rel_match = re.search(r'["\']([a-zA-Z0-9_\-\.]+\.m3u8(?:\?[^"\'\s]*)?)["\']', clean_text)
    if rel_match and base_url:
        return urljoin(base_url, rel_match.group(1)).strip("\"' ")

    # ৫. Base64 atob() থাকলে ডিকোড করে খোঁজা
    for b64 in re.findall(r'atob\(["\']([A-Za-z0-9+/=]{20,})["\']\)', text):
        try:
            decoded = base64.b64decode(b64).decode("utf-8", errors="ignore")
            found = extract_m3u8_from_content(decoded, base_url)
            if found:
                return found
        except Exception:
            pass

    return None

async def resolve_embed_stream(client: httpx.AsyncClient, embed_url: str) -> str | None:
    """যেমন: https://streamec.st/embed/ch48.php ফেচ করে আসল m3u8 বের করে"""
    async with HTTP_SEMAPHORE:
        try:
            # সঠিক হেডার সেট করা
            headers = {
                "Referer": f"{BASE_URL}/",
                "User-Agent": STREAM_UA,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
            }
            resp = await client.get(embed_url, headers=headers, timeout=12.0)
            if resp.status_code != 200:
                return None

            # পেজের স্ক্রিপ্ট থেকে খোঁজা
            m3u8 = extract_m3u8_from_content(resp.text, base_url=embed_url)
            if m3u8:
                return m3u8

            # যদি পেজের ভেতর আইফ্রেম থাকে
            soup = HTMLParser(resp.content)
            for iframe in soup.css("iframe"):
                src = iframe.attributes.get("src", "")
                if src and not src.startswith("javascript:"):
                    inner_url = urljoin(embed_url, src)
                    resp_inner = await client.get(
                        inner_url,
                        headers={"Referer": embed_url, "User-Agent": STREAM_UA},
                        timeout=12.0
                    )
                    if resp_inner.status_code == 200:
                        inner_m3u8 = extract_m3u8_from_content(resp_inner.text, base_url=inner_url)
                        if inner_m3u8:
                            return inner_m3u8
        except Exception as e:
            log.debug(f"Error resolving stream for {embed_url}: {e}")
        return None

def extract_embed_urls_from_element(elem, source_url: str) -> list[str]:
    """একটি রো বা এলিমেন্ট থেকে সবরকম এমবেড ও চ্যানেল লিঙ্ক খুঁজে বের করে"""
    found_urls = []

    # ১. <a> ট্যাগের সবরকম অ্যাট্রিবিউট চেক করা
    for a in elem.css("a, button, [role='button']"):
        for attr in ["href", "data-embed", "data-url", "data-src", "data-stream", "onclick"]:
            val = a.attributes.get(attr, "")
            if not val:
                continue

            # সরাসরি লিঙ্ক থাকলে
            if "/embed/" in val or "embed" in val.lower() or "/watch/" in val:
                # onclick="location.href='...'" হলে কোটেশনের ভেতরের লিঙ্ক আলাদা করা
                m = re.search(r"['\"]([^'\"]+)['\"]", val)
                clean_val = m.group(1) if m else val
                if not clean_val.startswith("javascript:"):
                    full = urljoin(source_url, clean_val)
                    if full not in found_urls:
                        found_urls.append(full)

            # ch48 বা stream31 এর মতো আইডি থাকলে
            ch_match = re.search(r'(ch\d+|stream\d+)', val, re.IGNORECASE)
            if ch_match:
                ch_id = ch_match.group(1).lower()
                direct_ch_url = f"{BASE_URL}/embed/{ch_id}.php"
                if direct_ch_url not in found_urls:
                    found_urls.append(direct_ch_url)

    # ২. পুরো রো-এর টেক্সট বা সোর্সে কোনো চ্যানেল আইডি আছে কি না (যেমন ch48.php)
    raw_html = elem.html or ""
    for ch_found in re.findall(r'embed/(ch\d+\.php)', raw_html, re.IGNORECASE):
        ch_url = f"{BASE_URL}/embed/{ch_found.lower()}"
        if ch_url not in found_urls:
            found_urls.append(ch_url)

    return found_urls

def parse_cards_soup(soup: HTMLParser, source_page_url: str) -> list[ScrapedMatch]:
    raw_matches: list[ScrapedMatch] = []

    # কার্ড রো খোঁজা
    card_rows = soup.css(".game-card-row, .game-card, .match-card, [class*='card-row']")

    if card_rows:
        for row in card_rows:
            title_node = row.css_first("h3, h4, .game-card-title, .title, strong")
            if not title_node:
                continue
            title = cleanup_text(title_node.text())

            time_node = row.css_first("time, .game-card-when, [class*='when'], [class*='time']")
            time_str = cleanup_text(time_node.text()) if time_node else ""
            dt = parse_match_datetime(time_str)

            row_text = row.text().upper()
            status = "LIVE" if "LIVE" in row_text else "UPCOMING"

            sport = "Sports"
            parent = row.parent
            while parent:
                sport_h = parent.css_first("h2, .game-card-group-title, [class*='sport']")
                if sport_h and sport_h.text().strip():
                    sport = cleanup_text(sport_h.text())
                    break
                parent = parent.parent

            # রো থেকে এমবেড লিঙ্কগুলো সংগ্রহ
            links = extract_embed_urls_from_element(row, source_page_url)

            img = row.css_first("img")
            logo = urljoin(source_page_url, img.attributes.get("src")) if img and img.attributes.get("src") else None

            if links and title:
                raw_matches.append(ScrapedMatch(sport=sport, title=title, status=status, start_dt=dt, embed_urls=links, logo=logo))

    # যদি কার্ড ক্লাস না মিলে, তবে বাটন ধরে স্ক্যান করা
    if not raw_matches:
        for a in soup.css("a"):
            btn_txt = a.text().strip().lower()
            href = a.attributes.get("href", "")
            if not ("embed" in btn_txt or "watch" in btn_txt or "/embed/" in href):
                continue

            row_elem = a.parent
            for _ in range(4):
                if not row_elem:
                    break
                if "vs" in row_elem.text().lower() or " - " in row_elem.text():
                    break
                row_elem = row_elem.parent

            if not row_elem:
                continue

            t_node = row_elem.css_first("h3, h4, strong")
            title = cleanup_text(t_node.text()) if t_node else ""
            if not title:
                for line in row_elem.text().split("\n"):
                    cl = cleanup_text(line)
                    if "vs" in cl.lower() or " - " in cl:
                        title = cl
                        break

            if not title or len(title) < 3:
                continue

            status = "LIVE" if "LIVE" in row_elem.text().upper() else "UPCOMING"
            links = extract_embed_urls_from_element(row_elem, source_page_url)
            dt = datetime.now(timezone.utc)
            img = row_elem.css_first("img")
            logo = urljoin(source_page_url, img.attributes.get("src")) if img and img.attributes.get("src") else None

            raw_matches.append(ScrapedMatch(sport="Sports", title=title, status=status, start_dt=dt, embed_urls=links, logo=logo))

    # ডুপ্লিকেশন দূর করা
    unique_matches: dict[str, ScrapedMatch] = {}
    for m in raw_matches:
        slug = create_slug(m.title)
        if slug in unique_matches:
            for u in m.embed_urls:
                if u not in unique_matches[slug].embed_urls:
                    unique_matches[slug].embed_urls.append(u)
        else:
            unique_matches[slug] = m

    return list(unique_matches.values())

async def scrape_all_sources(client: httpx.AsyncClient) -> list[ScrapedMatch]:
    PAGES_TO_TRY = [
        f"{BASE_URL}/game-cards",
        f"{BASE_URL}/game-cards/embed",
        "https://streamcenter.st/game-cards"
    ]

    for target_url in PAGES_TO_TRY:
        try:
            resp = await client.get(target_url, headers={"User-Agent": STREAM_UA, "Referer": BASE_URL}, timeout=15.0)
            if resp.status_code == 200:
                soup = HTMLParser(resp.content)
                matches = parse_cards_soup(soup, target_url)
                if matches:
                    log.info(f"Successfully scraped {len(matches)} unique matches from {target_url}")
                    return matches
        except Exception as e:
            log.warning(f"Error fetching from {target_url}: {e}")

    return []

async def main():
    log.info("Starting STC Scraper execution...")
    async with httpx.AsyncClient(headers={"User-Agent": STREAM_UA, "Referer": BASE_URL}, follow_redirects=True, http2=True) as client:
        scraped_matches = await scrape_all_sources(client)
        if not scraped_matches:
            log.warning("No matches found from target pages!")
            return

        final_playlist = []

        for m in scraped_matches:
            start_bst = m.start_dt.astimezone(BST_TZ)

            # প্রতিটি এমবেড ইউআরএল থেকে স্ট্রিম লোড করার চেষ্টা করা
            tasks = [resolve_embed_stream(client, url) for url in m.embed_urls]
            results = await asyncio.gather(*tasks)

            event_channels = []
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

            actual_status = "LIVE" if event_channels or m.status == "LIVE" else "UPCOMING"

            final_playlist.append({
                "title": m.title,
                "sport": m.sport,
                "sub_category": m.sport,
                "status": actual_status,
                "is_hot": (actual_status == "LIVE"),
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

        # M3U ফাইল সেভ
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

        log.info(f"Done! Saved {len(final_playlist)} matches and {stream_count} active live streams in channels.")

if __name__ == "__main__":
    asyncio.run(main())
