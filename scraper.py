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

BASE_URL = "https://streamcenter.st"
ALT_URL = "https://streamec.st"
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
    site_status: str
    start_dt: datetime
    stream_links: list[str] = field(default_factory=list)
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

def extract_match_datetime_from_row(row_elem) -> datetime:
    """
    পুরো রো স্ক্যান করে নিখুঁতভাবে খেলা শুরুর আসল সময় বের করে (যেমন: Oct 25, 2026, 8:15 AM)
    """
    # ১. <time datetime="..."> বা যেকোনো datetime অ্যাট্রিবিউট চেক করা
    for elem in row_elem.css("[datetime], time"):
        iso_val = elem.attributes.get("datetime", "").strip()
        if iso_val:
            try:
                clean_iso = iso_val.replace("Z", "+00:00")
                dt = datetime.fromisoformat(clean_iso)
                return dt.astimezone(timezone.utc)
            except Exception:
                pass

    raw_html = row_elem.html or ""
    m_attr = re.search(r'(?:datetime|data-date|data-time)=["\']([^"\']+)["\']', raw_html)
    if m_attr:
        try:
            clean_iso = m_attr.group(1).replace("Z", "+00:00")
            dt = datetime.fromisoformat(clean_iso)
            return dt.astimezone(timezone.utc)
        except Exception:
            pass

    # ২. পুরো রো-এর টেক্সট ক্লিন করে Regex দিয়ে তারিখ ও সময় আলাদা করা
    clean_txt = cleanup_text(row_elem.text())
    m = re.search(r'([A-Za-z]{3})\s+(\d{1,2}),?\s+(\d{4}),?\s+(\d{1,2}):(\d{2})\s*(AM|PM)?', clean_txt, re.IGNORECASE)
    if m:
        month_map = {
            "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
            "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12
        }
        try:
            m_str = m.group(1)[:3].lower()
            if m_str in month_map:
                month = month_map[m_str]
                day = int(m.group(2))
                year = int(m.group(3))
                hour = int(m.group(4))
                minute = int(m.group(5))
                ampm = m.group(6).upper() if m.group(6) else "AM"
                if ampm == "PM" and hour < 12:
                    hour += 12
                elif ampm == "AM" and hour == 12:
                    hour = 0

                # সাইটের রেন্ডার করা টাইম সরাসরি UTC-তে পার্স করা
                return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
        except Exception:
            pass

    return datetime.now(timezone.utc)

def calculate_match_status(start_dt: datetime, site_status: str, early_minutes: int = 15) -> str:
    """ম্যাচ শুরুর ঠিক ১৫ মিনিট আগে স্ট্যাটাস LIVE হবে"""
    now_utc = datetime.now(timezone.utc)
    start_utc = start_dt.astimezone(timezone.utc)

    # শুরু হতে ১৫ মিনিটের বেশি বাকি থাকলে নিশ্চিতভাবে UPCOMING
    if now_utc < (start_utc - timedelta(minutes=early_minutes)):
        return "UPCOMING"

    # শুরু হওয়ার ১৫ মিনিট আগে থেকে পরবর্তী ৩ ঘণ্টা পর্যন্ত LIVE
    if (start_utc - timedelta(minutes=early_minutes)) <= now_utc <= (start_utc + timedelta(hours=3, minutes=30)):
        return "LIVE"

    return "ENDED"

def extract_m3u8(text: str, base_url: str = "") -> str | None:
    if not text:
        return None

    clean_text = html.unescape(text).replace(r"\/", "/")

    edge_match = re.search(r'(https?://[a-zA-Z0-9_\-\.]*edgestream[^\s"\'<>\\]*?\.m3u8(?:\?[^\s"\'<>\\]+)?)', clean_text)
    if edge_match:
        return edge_match.group(1).strip("\"' ")

    source_match = re.search(r'(?:source|file|src|stream|hls)\s*[:=]\s*["\'](https?://[^"\']+\.m3u8(?:\?[^"\']*)?)["\']', clean_text, re.IGNORECASE)
    if source_match:
        return source_match.group(1).strip("\"' ")

    generic_match = re.search(r'(https?://[a-zA-Z0-9_\-\.]+(?::\d+)?/[^\s"\'<>\\]+?\.m3u8(?:\?[^\s"\'<>\\]+)?)', clean_text)
    if generic_match:
        return generic_match.group(1).strip("\"' ")

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
    async with HTTP_SEMAPHORE:
        visited = set()
        queue = [link_url]

        for _ in range(6):
            if not queue:
                break
            current_url = queue.pop(0)
            if current_url in visited:
                continue
            visited.add(current_url)

            try:
                headers = {
                    "User-Agent": STREAM_UA,
                    "Referer": "https://embedcenter.st/",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8"
                }
                resp = await client.get(current_url, headers=headers, follow_redirects=True, timeout=12.0)
                if resp.status_code != 200:
                    continue

                m3u8 = extract_m3u8(resp.text, base_url=str(resp.url))
                if m3u8:
                    return m3u8

                hls_matches = re.findall(r'["\']([^"\']*hls\.php\?stream=[^"\']+)["\']', resp.text)
                for hls_path in hls_matches:
                    full_hls = urljoin(str(resp.url), hls_path)
                    if full_hls not in visited:
                        queue.insert(0, full_hls)

                soup = HTMLParser(resp.content)
                for iframe in soup.css("iframe"):
                    src = iframe.attributes.get("src", "")
                    if src and not src.startswith("javascript:"):
                        iframe_url = urljoin(str(resp.url), src)
                        if iframe_url not in visited:
                            if "hls.php" in iframe_url:
                                queue.insert(0, iframe_url)
                            else:
                                queue.append(iframe_url)

            except Exception:
                pass

        return None

def parse_cards_soup(soup: HTMLParser, source_page_url: str) -> list[ScrapedMatch]:
    raw_matches: list[ScrapedMatch] = []
    rows = soup.css("article.game-card-row, .game-card-row")

    for row in rows:
        title = ""
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
            for line in row.text().split("\n"):
                cl = cleanup_text(line)
                if "vs" in cl.lower() or " - " in cl:
                    title = cl
                    break

        if not title or len(title) < 3:
            continue

        title = cleanup_text(title)

        # পুরো রো থেকে আসল তারিখ ও সময় পার্স করা
        dt = extract_match_datetime_from_row(row)

        row_txt = row.text().upper()
        site_status = "SCHEDULED" if "SCHEDULED" in row_txt else ("LIVE" if "LIVE" in row_txt else "UPCOMING")

        sport = "Sports"
        parent = row.parent
        while parent:
            sport_h = parent.css_first("h2, .game-card-group-title, [class*='sport']")
            if sport_h and sport_h.text().strip():
                sport = cleanup_text(sport_h.text())
                break
            parent = parent.parent

        links = []
        for a in row.css("a.game-card-open-link, a[href*='/api/stream-link/']"):
            href = a.attributes.get("href", "")
            if href:
                full_url = urljoin(source_page_url, href)
                if full_url not in links:
                    links.append(full_url)

        valid_logos = []
        for img in row.css("img"):
            for attr in ["src", "data-src", "data-lazy-src", "data-original"]:
                src = img.attributes.get(attr, "")
                if src and not src.startswith("data:image/svg") and not src.startswith("data:image/gif"):
                    full_logo = urljoin(source_page_url, src.strip())
                    if full_logo not in valid_logos:
                        valid_logos.append(full_logo)
                    break

        team_a_logo = valid_logos[0] if len(valid_logos) >= 1 else None
        team_b_logo = valid_logos[1] if len(valid_logos) >= 2 else None
        main_logo = team_a_logo or team_b_logo

        if links:
            raw_matches.append(ScrapedMatch(
                sport=sport,
                title=title,
                site_status=site_status,
                start_dt=dt,
                stream_links=links,
                team_a_logo=team_a_logo,
                team_b_logo=team_b_logo,
                logo=main_logo
            ))

    unique_matches: dict[str, ScrapedMatch] = {}
    for m in raw_matches:
        slug = create_slug(m.title)
        if slug in unique_matches:
            for lk in m.stream_links:
                if lk not in unique_matches[slug].stream_links:
                    unique_matches[slug].stream_links.append(lk)
            if not unique_matches[slug].team_a_logo and m.team_a_logo:
                unique_matches[slug].team_a_logo = m.team_a_logo
                unique_matches[slug].logo = m.team_a_logo
            if not unique_matches[slug].team_b_logo and m.team_b_logo:
                unique_matches[slug].team_b_logo = m.team_b_logo
        else:
            unique_matches[slug] = m

    return list(unique_matches.values())

async def scrape_all_sources(client: httpx.AsyncClient) -> list[ScrapedMatch]:
    PAGES_TO_TRY = [
        f"{BASE_URL}/game-cards",
        f"{ALT_URL}/game-cards",
        f"{BASE_URL}/game-cards/embed"
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
            # বাংলাদেশ সময় অনুযায়ী সঠিক ফরম্যাট (যেমন: Oct 25, 2026, 08:15 AM)
            start_bst = m.start_dt.astimezone(BST_TZ)
            start_time_str = start_bst.strftime("%b %d, %Y, %I:%M %p")
            end_time_str = (start_bst + timedelta(hours=3)).strftime("%b %d, %Y, %I:%M %p")

            # স্ট্রিম লিঙ্ক সংগ্রহ
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

            # সঠিক সময় ভিত্তিক স্ট্যাটাস (১৫ মিনিট আগের লজিক)
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

        # সর্টিং: LIVE ম্যাচ সবার আগে, তারপর ক্রমান্বয়ে UPCOMING ম্যাচগুলো আসবে
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
                m3u_lines.append(f'#EXTVLCOPT:http-referrer={EMBED_REFERER}')
                m3u_lines.append(f'#EXTVLCOPT:http-user-agent={STREAM_UA}')
                m3u_lines.append(ch["raw_link"])
                stream_count += 1

        M3U_OUTPUT.write_text("\n".join(m3u_lines) + "\n", encoding="utf-8")

        log.info(f"Done! Saved {len(final_playlist)} matches and {stream_count} active live streams in channels.")

if __name__ == "__main__":
    asyncio.run(main())
