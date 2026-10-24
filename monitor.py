import asyncio
import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import httpx
from selectolax.lexbor import LexborHTMLParser as HTMLParser

# --- গিটহাব ক্রেডেনশিয়াল ---
GITHUB_TOKEN = os.getenv("GH_PAT")
GITHUB_REPO = os.getenv("GH_REPO", "shoilifashion-hue/STC-Sports")
EVENT_TYPE = os.getenv("EVENT_TYPE", "source_updated")

# বর্তমান কার্যকর সোর্স ইউআরএলসমূহ
BASE_URL = "https://streamcenter.st"
ALT_URL = "https://streamec.st"

PAGES_TO_TRY = [
    f"{BASE_URL}/game-cards",
    f"{ALT_URL}/game-cards",
    f"{BASE_URL}/game-cards/embed"
]

UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/111.0.0.0 Safari/537.36"
)

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("MONITOR")


@dataclass(slots=True)
class RawEvent:
    title: str
    start_dt: datetime
    sources_count: int


def cleanup_text(text: str) -> str:
    text = text.replace('\xa0', ' ')
    text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)
    text = re.sub(r'(vs\.?)([A-Za-z])', r'\1 \2', text, flags=re.IGNORECASE)
    text = re.sub(r'([A-Za-z])(vs\.?)', r'\1 \2', text, flags=re.IGNORECASE)
    return re.sub(r"\s+", " ", text).strip()


def extract_match_datetime(row_elem) -> datetime:
    """পুরো রো থেকে আসল সময় পার্স করে"""
    # ১. <time datetime="..."> অ্যাট্রিবিউট চেক
    for elem in row_elem.css("[datetime], time"):
        iso_val = elem.attributes.get("datetime", "").strip()
        if iso_val:
            try:
                clean_iso = iso_val.replace("Z", "+00:00")
                dt = datetime.fromisoformat(clean_iso)
                return dt.astimezone(timezone.utc)
            except Exception:
                pass

    # ২. Regex দিয়ে তারিখ অংশ বের করা (যেমন: Oct 25, 2026, 8:15 AM)
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
                return datetime(year, month, day, hour, minute, tzinfo=timezone.utc)
        except Exception:
            pass

    return datetime.now(timezone.utc)


# খেলা শুরুর ১৫ মিনিট আগে থেকে LIVE স্ট্যাটাস নির্ধারণ করার লজিক
def determine_status(start_dt: datetime, early_minutes: int = 15) -> str:
    now_utc = datetime.now(timezone.utc)
    start_utc = start_dt.astimezone(timezone.utc)
    live_start_utc = start_utc - timedelta(minutes=early_minutes)
    end_utc = start_utc + timedelta(hours=3, minutes=30)

    if live_start_utc <= now_utc <= end_utc:
        return "LIVE"
    elif now_utc < live_start_utc:
        return "UPCOMING"
    return "ENDED"


async def trigger_github_workflow(client: httpx.AsyncClient, reason: str = "") -> bool:
    """GitHub Actions চালু করার জন্য সিগন্যাল পাঠানো"""
    if not GITHUB_TOKEN or not GITHUB_REPO:
        log.warning("GH_PAT or GH_REPO not found. Cannot trigger GitHub Actions.")
        return False

    url = f"https://api.github.com/repos/{GITHUB_REPO}/dispatches"
    headers = {
        "Authorization": f"Bearer {GITHUB_TOKEN}",
        "Accept": "application/vnd.github+json",
        "X-GitHub-Api-Version": "2022-11-28",
        "User-Agent": UA,
    }
    payload = {
        "event_type": EVENT_TYPE,
        "client_payload": {
            "triggered_at": datetime.now(timezone.utc).isoformat(),
            "reason": reason or "State changed or match went live",
        },
    }

    try:
        resp = await client.post(url, json=payload, headers=headers, timeout=15.0)
        if resp.status_code == 204:
            log.info(">>> SUCCESS: Triggered GitHub Action successfully!")
            return True
        else:
            log.error(f"Failed to trigger GitHub: {resp.status_code} - {resp.text}")
            return False
    except Exception as e:
        log.error(f"GitHub Dispatch API Error: {e}")
        return False


async def get_raw_events(client: httpx.AsyncClient) -> list[RawEvent]:
    """বর্তমান সাইটের game-cards পেজ থেকে খেলার তালিকা ফেচ করা"""
    events = []
    
    for target_url in PAGES_TO_TRY:
        try:
            resp = await client.get(target_url, headers={"User-Agent": UA, "Referer": BASE_URL}, timeout=15.0)
            if resp.status_code != 200:
                continue

            soup = HTMLParser(resp.content)
            rows = soup.css("article.game-card-row, .game-card-row")

            for row in rows:
                title = ""
                # aria-label থেকে পরিষ্কার টাইটেল বের করা
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

                event_dt = extract_match_datetime(row)
                sources_count = len(row.css("a.game-card-open-link, a[href*='/api/stream-link/']"))

                if sources_count > 0:
                    events.append(
                        RawEvent(
                            title=cleanup_text(title),
                            start_dt=event_dt,
                            sources_count=sources_count,
                        )
                    )

            if events:
                log.info(f"Loaded {len(events)} events from {target_url}")
                return events

        except Exception as e:
            log.warning(f"Error fetching {target_url}: {e}")

    return events


async def check_and_monitor():
    """২৪ ঘণ্টা প্রতি ৫ মিনিট পর পর চেক করার মূল লুপ"""
    log.info("24/7 Monitor started. Checking source every 5 minutes...")
    last_state = None

    async with httpx.AsyncClient(headers={"User-Agent": UA}, follow_redirects=True, timeout=15.0) as client:
        while True:
            try:
                log.info("Checking source for status updates, 15-min live events, or match changes...")
                raw_events = await get_raw_events(client)

                if raw_events:
                    state_list = []
                    live_matches = []

                    for ev in raw_events:
                        status = determine_status(ev.start_dt, early_minutes=15)
                        if status == "LIVE":
                            live_matches.append(ev.title)
                        state_list.append(f"{ev.title}_{status}_{ev.sources_count}")

                    state_list.sort()
                    new_state = "|".join(state_list)

                    # ১. প্রথমবার রেন্ডার চালু হলে স্টেট মেমোরিতে রাখা
                    if last_state is None:
                        last_state = new_state
                        log.info(
                            f"Initialized monitor with {len(raw_events)} events "
                            f"(Currently LIVE: {len(live_matches)}). Waiting for future updates..."
                        )

                    # ২. অবস্থার পরিবর্তন হলে (ম্যাচ ১৫ মিনিট আগে LIVE হলে বা নতুন ম্যাচ এলে)
                    elif last_state != new_state:
                        log.info(
                            f"State change detected! (Events: {len(raw_events)}, LIVE now: {len(live_matches)}). "
                            "Sending signal to GitHub..."
                        )
                        success = await trigger_github_workflow(
                            client,
                            reason=f"Status update: {len(live_matches)} matches LIVE",
                        )
                        if success:
                            last_state = new_state
                    else:
                        log.info("No status change (no match became LIVE). Sleeping for 5 minutes...")
                else:
                    log.warning("No events found from source.")

            except Exception as e:
                log.error(f"Unexpected monitor error: {e}")

            # ৫ মিনিট অপেক্ষা
            await asyncio.sleep(300)


async def run_render_health_server():
    """Render Web Service-এর পোর্ট সক্রিয় রাখার জন্য ব্যাকগ্রাউন্ড সার্ভার"""
    port = int(os.getenv("PORT", 8080))

    async def handle_client(reader, writer):
        try:
            await reader.read(1024)
            response = (
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Type: text/plain\r\n"
                b"Content-Length: 2\r\n"
                b"Connection: close\r\n\r\nOK"
            )
            writer.write(response)
            await writer.drain()
        except Exception:
            pass
        finally:
            try:
                writer.close()
                await writer.wait_closed()
            except Exception:
                pass

    server = await asyncio.start_server(handle_client, "0.0.0.0", port)
    log.info(f"Render health check server listening on port {port}")
    async with server:
        await server.serve_forever()


async def main():
    await asyncio.gather(
        check_and_monitor(),
        run_render_health_server(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Monitor service stopped.")
