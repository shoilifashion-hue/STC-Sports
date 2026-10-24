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
    time_str: str
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
    """স্ক্রিনশটে প্রদর্শিত 'Oct 24, 2026, 10:00 PM' ফরম্যাট পার্স করে"""
    time_str = cleanup_text(time_str)
    # বিভিন্ন ফরম্যাট ট্রাই করা
    formats = [
        "%b %d, %Y, %I:%M %p",
        "%b %d, %Y, %H:%M",
        "%Y-%m-%d %H:%M:%S",
        "%B %d, %Y, %I:%M %p"
    ]
    for fmt in formats:
        try:
            dt = datetime.strptime(time_str, fmt)
            # সাইটের টাইম সাধারণত UTC বা US Eastern হয়, ডিফল্ট UTC ধরা হলো
            return dt.replace(tzinfo=timezone.utc)
        except Exception:
            pass
    # পার্স না হলে বর্তমান সময় দিয়ে দেওয়া
    return datetime.now(timezone.utc)

def extract_m3u8(text: str, base_embed_url: str = "") -> str | None:
    """JS বা HTML রেসপন্স থেকে টোকেনসহ সম্পূর্ণ m3u8 লিংক এক্সট্রাক্ট করে"""
    clean_text = text.replace(r"\/", "/")

    # ১. edgestream এর টোকেনসহ লিঙ্ক সরাসরি ম্যাচ করা
    edge_match = re.search(r'(https?://[a-zA-Z0-9_\-\.]*edgestream[^\s"\'<>\\]*?\.m3u8(?:\?[^\s"\'<>\\]+)?)', clean_text)
    if edge_match:
        return edge_match.group(1)

    # ২. যেকোনো পূর্ণ ডোমেইনের m3u8 লিংক
    generic_match = re.search(r'(https?://[a-zA-Z0-9_\-\.]+(?::\d+)?/[^\s"\'<>\\]+?\.m3u8(?:\?[^\s"\'<>\\]+)?)', clean_text)
    if generic_match:
        return generic_match.group(1)

    # ৩. স্ক্রিনশটে দেখা zeuryuegn48.m3u8 এর মতো রিলেটিভ লিংক থাকলে
    rel_match = re.search(r'["\']([a-zA-Z0-9_\-\.]+\.m3u8(?:\?[^"\'\s]*)?)["\']', clean_text)
    if rel_match and base_embed_url:
        return urljoin(base_embed_url, rel_match.group(1))

    # ৪. Base64 atob() থাকলে ডিকোড করে চেক করা
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
    """যেমন: https://streamec.st/embed/ch48.php লোড করে আসল m3u8 বের করে"""
    async with HTTP_SEMAPHORE:
        try:
            resp = await client.get(
                embed_url,
                headers={"Referer": BASE_URL, "User-Agent": STREAM_UA},
                timeout=12.0
            )
            if resp.status_code != 200:
                return None

            # পেজের ভেতর থেকে m3u8 খোঁজা
            m3u8 = extract_m3u8(resp.text, base_embed_url=embed_url)
            if m3u8:
                return m3u8

            # যদি ভেতরের প্লেয়ার iframe দিয়ে embedcenter বা edgestream লোড করে
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

async def scrape_streamec_homepage(client: httpx.AsyncClient) -> list[ScrapedMatch]:
    """https://streamec.st এর হোমপেজ থেকে সরাসরি ম্যাচ ও এমবেড লিঙ্কগুলো স্ক্র্যাপ করে"""
    matches = []
    try:
        resp = await client.get(BASE_URL, headers={"User-Agent": STREAM_UA}, timeout=15.0)
        if resp.status_code != 200:
            log.error(f"Failed to fetch {BASE_URL}: Status {resp.status_code}")
            return matches

        soup = HTMLParser(resp.content)

        # সাইটের কাঠামো অনুযায়ী রো/ইভেন্ট খোঁজা
        # প্রতিটি ক্যাটাগরি বা টেবিলের রো খোঁজা হচ্ছে
        rows = soup.css(".event-row, .game-row, .match-row, tr, [class*='event'], [class*='game']")
        
        # যদি নির্দিষ্ট ক্লাস না থাকে তবে পুরো পেজের লিংক ও টেক্সট ব্লকগুলো স্ক্যান করা
        for row in rows:
            text = row.text()
            if not ("LIVE" in text or "SCHEDULED" in text):
                continue

            # স্ট্যাটাস
            is_live = "LIVE" in text
            status = "LIVE" if is_live else "UPCOMING"

            # বাটনগুলো থেকে Embed বা Watch লিংক খোঁজা (যেমন: /embed/ch48.php)
            links = []
            for a in row.css("a"):
                href = a.attributes.get("href", "")
                btn_text = cleanup_text(a.text()).lower()
                if "embed" in btn_text or "watch" in btn_text or "/embed/" in href or "/watch/" in href:
                    if href and not href.startswith("javascript:"):
                        full_url = urljoin(BASE_URL, href)
                        if full_url not in links:
                            links.append(full_url)

            if not links:
                continue

            # স্পোর্ট ও টাইটেল বের করা
            title_elem = row.css_first("h3, h4, .title, .team-name, .teams, strong")
            title = cleanup_text(title_elem.text()) if title_elem else ""
            if not title:
                # বিকল্প: লাইনের ভেতর vs খোঁজা
                for line in text.split("\n"):
                    clean_l = cleanup_text(line)
                    if "vs" in clean_l.lower() or " - " in clean_l:
                        title = clean_l
                        break

            if not title or len(title) < 3:
                continue

            # ক্যাটাগরি / স্পোর্ট খোঁজা (উপরে বা প্যারেন্ট সেকশনে)
            sport = "Sports"
            parent = row.parent
            while parent:
                header = parent.css_first("h1, h2, .category-title, [class*='sport']")
                if header and header.text().strip():
                    cat_candidate = cleanup_text(header.text())
                    if len(cat_candidate) < 30 and "welcome" not in cat_candidate.lower():
                        sport = cat_candidate
                        break
                parent = parent.parent

            # টাইম খোঁজা
            time_str = ""
            time_match = re.search(r'([A-Za-z]{3}\s+\d{1,2},\s+\d{4},\s+\d{1,2}:\d{2}\s+(?:AM|PM))', text)
            if time_match:
                time_str = time_match.group(1)

            # লোগো
            img = row.css_first("img")
            logo = urljoin(BASE_URL, img.attributes.get("src")) if img and img.attributes.get("src") else None

            matches.append(ScrapedMatch(
                sport=sport,
                title=title,
                status=status,
                time_str=time_str,
                embed_urls=links,
                logo=logo
            ))

        log.info(f"Successfully scraped {len(matches)} matches directly from {BASE_URL} homepage HTML.")
    except Exception as e:
        log.error(f"Error scraping homepage: {e}")

    return matches

async def main():
    log.info(f"Scraping {BASE_URL} directly...")
    async with httpx.AsyncClient(headers={"User-Agent": STREAM_UA, "Referer": BASE_URL}, follow_redirects=True, http2=True) as client:
        scraped_matches = await scrape_streamec_homepage(client)
        if not scraped_matches:
            log.warning("No matches extracted from homepage!")
            return

        final_playlist = []

        for m in scraped_matches:
            dt = parse_match_datetime(m.time_str)
            start_bst = dt.astimezone(BST_TZ)

            # প্রতিটি এমবেড ইউআরএল (যেমন: /embed/ch48.php) থেকে m3u8 এক্সট্রাক্ট করা
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
                "_timestamp": dt.timestamp()
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
