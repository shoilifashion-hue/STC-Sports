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
    stream_links: list[str] = field(default_factory=list)
    logo: str | None = None

def cleanup_text(text: str) -> str:
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

def extract_m3u8(text: str, base_url: str = "") -> str | None:
    """HTML বা JS রেসপন্স থেকে টোকেনসহ সম্পূর্ণ m3u8 লিংক এক্সট্রাক্ট করে"""
    if not text:
        return None

    clean_text = html.unescape(text).replace(r"\/", "/")

    # ১. edgestream টোকেনসহ লিঙ্ক
    edge_match = re.search(r'(https?://[a-zA-Z0-9_\-\.]*edgestream[^\s"\'<>\\]*?\.m3u8(?:\?[^\s"\'<>\\]+)?)', clean_text)
    if edge_match:
        return edge_match.group(1).strip("\"' ")

    # ২. Clappr বা HLS প্লেয়ারের সোর্স কনফিগ
    source_match = re.search(r'(?:source|file|src|stream|hls)\s*[:=]\s*["\'](https?://[^"\']+\.m3u8(?:\?[^"\']*)?)["\']', clean_text, re.IGNORECASE)
    if source_match:
        return source_match.group(1).strip("\"' ")

    # ৩. যেকোনো পূর্ণ ডোমেইনের m3u8
    generic_match = re.search(r'(https?://[a-zA-Z0-9_\-\.]+(?::\d+)?/[^\s"\'<>\\]+?\.m3u8(?:\?[^\s"\'<>\\]+)?)', clean_text)
    if generic_match:
        return generic_match.group(1).strip("\"' ")

    # ৪. রিলেটিভ m3u8
    rel_match = re.search(r'["\']([a-zA-Z0-9_\-\.]+\.m3u8(?:\?[^"\'\s]*)?)["\']', clean_text)
    if rel_match and base_url:
        return urljoin(base_url, rel_match.group(1)).strip("\"' ")

    # ৫. Base64 ডিকোড চেক
    for b64 in re.findall(r'atob\(["\']([A-Za-z0-9+/=]{20,})["\']\)', text):
        try:
            decoded = base64.b64decode(b64).decode("utf-8", errors="ignore")
            found = extract_m3u8(decoded, base_url)
            if found:
                return found
        except Exception:
            pass

    return None

async def resolve_full_stream_chain(client: httpx.AsyncClient, link_url: str) -> str | None:
    """
    স্ক্রিনশটের পুরো চেইন এক্সিকিউট করে:
    /api/stream-link/iframe/... -> chXX.php -> hls.php?stream=... -> edgestream1.pro/...m3u8?st=...
    """
    async with HTTP_SEMAPHORE:
        try:
            # ধাপ ১: /api/stream-link/iframe/... ফেচ করা (রিডাইরেক্ট হয়ে chXX.php এ যাবে)
            headers = {
                "User-Agent": STREAM_UA,
                "Referer": f"{BASE_URL}/game-cards",
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
            }
            resp = await client.get(link_url, headers=headers, follow_redirects=True, timeout=12.0)
            if resp.status_code != 200:
                return None

            # পেজের ভেতর সরাসরি m3u8 থাকলে
            m3u8 = extract_m3u8(resp.text, base_url=str(resp.url))
            if m3u8:
                return m3u8

            # ধাপ ২: স্ক্রিনশটে থাকা hls.php?stream=... আইফ্রেম খোঁজা
            soup = HTMLParser(resp.content)
            
            # সব iframe চেক
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
                            return inner_m3u8

            # ধাপ ৩: স্ক্রিপ্টের ভেতর hls.php এর লিঙ্ক খোঁজা
            hls_match = re.search(r'["\']([^"\']*hls\.php\?[^"\']+)["\']', resp.text)
            if hls_match:
                hls_url = urljoin(str(resp.url), hls_match.group(1))
                resp_hls = await client.get(
                    hls_url,
                    headers={"User-Agent": STREAM_UA, "Referer": str(resp.url)},
                    timeout=10.0
                )
                if resp_hls.status_code == 200:
                    found = extract_m3u8(resp_hls.text, base_url=hls_url)
                    if found:
                        return found

        except Exception as e:
            log.debug(f"Chain resolution error: {e}")
        return None

def parse_cards_soup(soup: HTMLParser, source_page_url: str) -> list[ScrapedMatch]:
    raw_matches: list[ScrapedMatch] = []

    # স্ক্রিনশটের আসল ট্যাগ: <article class="game-card-row">
    rows = soup.css("article.game-card-row, .game-card-row")

    for row in rows:
        # ১. টাইটেল বের করা
        title = ""
        # স্ক্রিনশট ৩ এ দেখা যাচ্ছে aria-label="Sources for Boston College Eagles vs. Georgia Tech Yellow Jackets"
        sources_div = row.css_first(".game-card-sources")
        if sources_div:
            label = sources_div.attributes.get("aria-label", "")
            if "Sources for " in label:
                title = label.replace("Sources for ", "").strip()

        if not title:
            content_div = row.css_first(".game-card-content, .game-card-side")
            t_elem = content_div.css_first("h3, h4, .title, strong") if content_div else None
            title = cleanup_text(t_elem.text()) if t_elem else ""

        if not title:
            # টেক্সট থেকে vs খুঁজে টাইটেল বের করা
            for line in row.text().split("\n"):
                cl = cleanup_text(line)
                if "vs" in cl.lower() or " - " in cl:
                    title = cl
                    break

        if not title or len(title) < 3:
            continue

        title = cleanup_text(title)

        # ২. টাইম বের করা
        time_node = row.css_first(".game-card-when, time, [class*='when']")
        time_str = cleanup_text(time_node.text()) if time_node else ""
        dt = parse_match_datetime(time_str)

        # ৩. স্ট্যাটাস (লাইভ স্কোরবোর্ড থাকলে অথবা LIVE লেখা থাকলে)
        has_scoreboard = bool(row.css_first(".live-scoreboard, [class*='live-score']"))
        status = "LIVE" if (has_scoreboard or "LIVE" in row.text().upper()) else "UPCOMING"

        # ৪. স্পোর্টস ক্যাটাগরি
        sport = "Sports"
        parent = row.parent
        while parent:
            sport_h = parent.css_first("h2, .game-card-group-title, [class*='sport']")
            if sport_h and sport_h.text().strip():
                sport = cleanup_text(sport_h.text())
                break
            parent = parent.parent

        # ৫. স্ক্রিনশট ২ ও ৩ এর আসল লিঙ্ক: <a class="game-card-open-link" href="/api/stream-link/iframe/...">
        links = []
        for a in row.css("a.game-card-open-link, a[href*='/api/stream-link/']"):
            href = a.attributes.get("href", "")
            if href:
                full_url = urljoin(source_page_url, href)
                if full_url not in links:
                    links.append(full_url)

        # ৬. লোগো
        img = row.css_first("img")
        logo = urljoin(source_page_url, img.attributes.get("src")) if img and img.attributes.get("src") else None

        if links:
            raw_matches.append(ScrapedMatch(
                sport=sport,
                title=title,
                status=status,
                start_dt=dt,
                stream_links=links,
                logo=logo
            ))

    # ডুপ্লিকেশন দূর করা
    unique_matches: dict[str, ScrapedMatch] = {}
    for m in raw_matches:
        slug = create_slug(m.title)
        if slug in unique_matches:
            for lk in m.stream_links:
                if lk not in unique_matches[slug].stream_links:
                    unique_matches[slug].stream_links.append(lk)
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
                    log.info(f"Successfully scraped {len(matches)} matches from {target_url}")
                    return matches
        except Exception as e:
            log.warning(f"Error fetching from {target_url}: {e}")

    return []

async def main():
    log.info("Starting STC Scraper execution with complete redirect-chain resolution...")
    async with httpx.AsyncClient(headers={"User-Agent": STREAM_UA, "Referer": BASE_URL}, follow_redirects=True, http2=True) as client:
        scraped_matches = await scrape_all_sources(client)
        if not scraped_matches:
            log.warning("No matches found from target pages!")
            return

        final_playlist = []

        for m in scraped_matches:
            # সময় সেকেন্ড ছাড়া ফরম্যাট (যেমন: Oct 25, 2026, 01:30 AM)
            start_bst = m.start_dt.astimezone(BST_TZ)
            start_time_str = start_bst.strftime("%b %d, %Y, %I:%M %p")
            end_time_str = (start_bst + timedelta(hours=3)).strftime("%b %d, %Y, %I:%M %p")

            # প্রতিটি ওপেন-লিঙ্কের রিডাইরেক্ট চেইন সমাধান করে আসল m3u8 আনা
            tasks = [resolve_full_stream_chain(client, url) for url in m.stream_links]
            results = await asyncio.gather(*tasks)

            event_channels = []
            idx = 1
            for req_url, m3u8_url in zip(m.stream_links, results):
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
                    "start_time_bst": start_time_str,
                    "end_time_bst": end_time_str,
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
                    "last_updated": datetime.now(BST_TZ).strftime("%b %d, %Y, %I:%M %p"),
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
