import asyncio
import html
import json
import logging
import re
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import httpx
from playwright.async_api import async_playwright

ROOT_DIR = Path(__file__).resolve().parent

API_URL = "https://ovogoal.pages.dev/api/v2/flyembed2.json"
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

BST_TZ = ZoneInfo("Asia/Dhaka")
BROWSER_SEMAPHORE = asyncio.Semaphore(3)

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

async def fetch_pirlotv_matches(client: httpx.AsyncClient) -> list[ScrapedMatch]:
    matches = []
    try:
        headers = {"User-Agent": STREAM_UA, "Referer": BASE_URL}
        resp = await client.get(API_URL, headers=headers, timeout=15.0)
        if resp.status_code != 200:
            log.error(f"API request failed with status: {resp.status_code}")
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

async def sniff_stream_with_browser(browser, embed_url: str) -> tuple[str | None, str | None]:
    """
    JW Player অটোপ্লে এবং ক্লিক ইন্টারঅ্যাকশনসহ ফাস্ট m3u8 ক্যাপচার
    """
    async with BROWSER_SEMAPHORE:
        m3u8_url = None
        referer_header = None
        captured_event = asyncio.Event()

        context = await browser.new_context(
            user_agent=STREAM_UA,
            extra_http_headers={"Referer": f"{BASE_URL}/"},
            viewport={"width": 1280, "height": 720}
        )
        page = await context.new_page()

        # নেটওয়ার্ক রিকোয়েস্ট থেকে m3u8 ধরা
        def check_url(url: str, headers: dict):
            nonlocal m3u8_url, referer_header
            # flyembed নিজস্ব ইউআরএল ছাড়া আসল m3u8 খোঁজা
            if ".m3u8" in url and "flyembed" not in url:
                if not m3u8_url:
                    m3u8_url = url
                    referer_header = headers.get("referer") or headers.get("origin")
                    captured_event.set()

        async def handle_request(request):
            check_url(request.url, request.headers)

        async def handle_response(response):
            check_url(response.url, response.headers)

        page.on("request", handle_request)
        page.on("response", handle_response)

        try:
            # ১. পেজ লোড হওয়া
            await page.goto(embed_url, timeout=9000, wait_until="domcontentloaded")

            # ২. JW Player এর ওপর স্বয়ংক্রিয় ক্লিক করা (যাতে অটোপ্লে শুরু হয়)
            try:
                # পেজের সব আইফ্রেমের ভেতরে প্লেয়ার খোঁজা
                for frame in page.frames:
                    try:
                        # প্লে বাটনে ক্লিক
                        play_btn = await frame.query_selector(".jw-display-icon-display, .jw-preview, video, #player, .play-button")
                        if play_btn:
                            await play_btn.click(timeout=1500)
                            break
                    except Exception:
                        pass
                # মূল পেজেও ক্লিক ট্রাই করা
                await page.click("body", timeout=1000)
            except Exception:
                pass

            # ৩. m3u8 পাওয়ার জন্য সর্বোচ্চ ৫ সেকেন্ড অপেক্ষা
            try:
                await asyncio.wait_for(captured_event.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                pass
        except Exception as e:
            log.debug(f"Browser navigation note for {embed_url}: {e}")
        finally:
            await context.close()

        return m3u8_url, referer_header

async def process_match_streams(browser, match, should_sniff):
    channels = []
    if not should_sniff or not match.stream_urls:
        return channels

    idx = 1
    for url in match.stream_urls:
        m3u8_url, referer_used = await sniff_stream_with_browser(browser, url)
        if m3u8_url:
            ref = referer_used or "https://ehgembxe.cfd/"
            if not ref.endswith("/"):
                ref += "/"
            origin = ref.rstrip("/")

            raw_link = f"{m3u8_url}|User-Agent={STREAM_UA}&Referer={ref}&Origin={origin}"
            channels.append({
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
            log.info(f"[{match.title}] SUCCESS -> Captured m3u8: {m3u8_url}")
            idx += 1

    return channels

async def main():
    log.info("Starting Pirlo TV Scraper execution with fast Playwright engine...")

    async with httpx.AsyncClient(headers={"User-Agent": STREAM_UA}, follow_redirects=True, http2=True) as client:
        matches = await fetch_pirlotv_matches(client)
        if not matches:
            log.warning("No matches found from Pirlo TV API!")
            return

    now_utc = datetime.now(timezone.utc)

    # ক্রোম চালু করার সময় Autoplay Policy সম্পূর্ণ আনলক করা হয়েছে
    async with async_playwright() as p:
        browser = await p.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-gpu",
                "--autoplay-policy=no-user-gesture-required",  # ভিডিও সাথে সাথে চালু করার ফ্ল্যাগ
                "--disable-web-security"
            ]
        )

        tasks = []
        sniff_targets = []

        for m in matches:
            time_until_start = m.start_dt.astimezone(timezone.utc) - now_utc
            # লাইভ অথবা আগামী ১ ঘণ্টার ম্যাচ ফিল্টার
            is_active_window = (-timedelta(hours=3, minutes=30)) <= time_until_start <= timedelta(minutes=60)

            if is_active_window and m.stream_urls:
                sniff_targets.append(m.title)
                tasks.append(process_match_streams(browser, m, True))
            else:
                tasks.append(process_match_streams(browser, m, False))

        log.info(f"Targeting {len(sniff_targets)} active/live matches for stream capture: {sniff_targets}")

        all_channels_results = await asyncio.gather(*tasks)
        await browser.close()

    final_playlist = []
    for m, event_channels in zip(matches, all_channels_results):
        start_bst = m.start_dt.astimezone(BST_TZ)
        start_time_str = start_bst.strftime("%b %d, %Y, %I:%M %p")
        end_time_str = (start_bst + timedelta(hours=3)).strftime("%b %d, %Y, %I:%M %p")

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

    log.info(f"Done! Successfully generated {len(final_playlist)} matches and {stream_count} active live streams in M3U.")

if __name__ == "__main__":
    asyncio.run(main())
