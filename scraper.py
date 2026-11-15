import asyncio
import json
import logging
import re
from pathlib import Path
from urllib.parse import urljoin
import httpx

# কনফিগারেশন ও রুট পাথ
ROOT_DIR = Path(__file__).resolve().parent

API_BASE = "https://limon-iptv-link.limon20202.workers.dev"
CHANNELS_API = f"{API_BASE}/api/channels"
PLAY_API = f"{API_BASE}/api/play"

REFERER_URL = "https://limon-iptv-pro.vercel.app/"
ORIGIN_URL = "https://limon-iptv-pro.vercel.app"

M3U_OUTPUT = ROOT_DIR / "playlist.m3u"
JSON_OUTPUT = ROOT_DIR / "playlist.json"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

HEADERS = {
    "User-Agent": USER_AGENT,
    "Referer": REFERER_URL,
    "Origin": ORIGIN_URL,
    "Accept": "*/*"
}

# ক্লাউডফ্লেয়ার যাতে কোনো রিকোয়েস্ট ড্রপ না করে সেজন্য সেমাফোর ৫ রাখা হয়েছে
SEMAPHORE = asyncio.Semaphore(5)

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s")
log = logging.getLogger("IPTV_SCRAPER")


def extract_first_r_stream(manifest_text: str, base_url: str) -> str | None:
    """
    মাস্টার প্লেলিস্টে একাধিক /r/ লিঙ্ক থাকলে строго একদম প্রথম (Primary) লিঙ্কটি বের করে।
    """
    if not manifest_text:
        return None

    # ১. লাইন বাই লাইন স্ক্যান করে কমেন্ট (#) বাদে প্রথম /r/ লিঙ্কটি গ্রহণ
    lines = [line.strip() for line in manifest_text.splitlines() if line.strip()]
    for line in lines:
        if line.startswith("#"):
            continue
        full_candidate = urljoin(base_url, line)
        if "/r/" in full_candidate:
            clean_url = full_candidate.strip("\"' ")
            if not clean_url.endswith(".m3u8") and "?" not in clean_url:
                clean_url += ".m3u8"
            return clean_url

    # ২. রেজেক্স দিয়ে টেক্সট থেকে ক্রমানুসারে প্রথম /r/ ম্যাচটি খোঁজা (রিলেটিভ বা ফুল ডোমেইন)
    pattern = r'((?:https?://[^\s"\'<>]+)?/r/[^\s"\'<>]+\.m3u8(?:\?[^\s"\'<>]*)?)'
    match = re.search(pattern, manifest_text)
    if match:
        clean_url = urljoin(base_url, match.group(1).strip("\"' "))
        return clean_url

    # ৩. এক্সটেনশন ছাড়া থাকলে সেটির ফলব্যাক
    pattern_fallback = r'((?:https?://[^\s"\'<>]+)?/r/[^\s"\'<>]+)'
    match_fallback = re.search(pattern_fallback, manifest_text)
    if match_fallback:
        clean_url = urljoin(base_url, match_fallback.group(1).strip("\"' "))
        if not clean_url.endswith(".m3u8") and "?" not in clean_url:
            clean_url += ".m3u8"
        return clean_url

    return None


async def fetch_channels_raw(client: httpx.AsyncClient) -> list[dict]:
    """চ্যানেল তালিকা ও সংশ্লিষ্ট টোকেন সংগ্রহ করা"""
    channels = []
    try:
        log.info(f"Fetching channels from {CHANNELS_API} ...")
        resp = await client.get(CHANNELS_API, headers=HEADERS, timeout=20.0)
        if resp.status_code != 200:
            log.error(f"Failed to fetch channels: Status {resp.status_code}")
            return channels

        lines = resp.text.splitlines()
        current_extinf = None

        for line in lines:
            line = line.strip()
            if not line:
                continue

            if line.startswith("#EXTINF"):
                current_extinf = line
            elif line.startswith("gw:"):
                token = line[3:].strip()
                if current_extinf and token:
                    channels.append({
                        "extinf": current_extinf,
                        "token": token
                    })
                    current_extinf = None
            elif line.startswith("http"):
                if current_extinf:
                    channels.append({
                        "extinf": current_extinf,
                        "direct_url": line
                    })
                    current_extinf = None

        log.info(f"Parsed {len(channels)} total channels from response.")
    except Exception as e:
        log.error(f"Error reading channels API: {e}")

    return channels


async def resolve_channel_stream(client: httpx.AsyncClient, channel: dict) -> dict | None:
    """টোকেন দিয়ে /api/play কল করে শুধুমাত্র প্রথম /r/ লিঙ্কটি নিশ্চিত করা"""
    if "direct_url" in channel:
        direct = channel["direct_url"]
        if "/s/" in direct:
            direct = direct.replace("/s/", "/r/")
        return {
            "extinf": channel["extinf"],
            "url": direct
        }

    token = channel.get("token")
    if not token:
        return None

    async with SEMAPHORE:
        try:
            # ধাপ ১: /api/play কল করে প্রাথমিক মাস্টার URL নেওয়া
            params = {"c": token}
            resp = await client.get(PLAY_API, params=params, headers=HEADERS, timeout=12.0)
            if resp.status_code != 200:
                return None

            data = resp.json()
            initial_url = data.get("url")
            if not initial_url:
                return None

            # ধাপ ২: মাস্টার প্লেলিস্ট ফেচ করে সেখান থেকে কেবল সর্বপ্রথম /r/ লিঙ্কটি নেওয়া
            final_stream_url = None
            try:
                manifest_resp = await client.get(initial_url, headers=HEADERS, timeout=10.0)
                if manifest_resp.status_code == 200:
                    final_stream_url = extract_first_r_stream(manifest_resp.text, initial_url)
            except Exception:
                pass

            # যদি ফেচিং ফেইল করে, সরাসরি রূপান্তর ফলব্যাক
            if not final_stream_url:
                if "/s/" in initial_url:
                    final_stream_url = initial_url.replace("/s/", "/r/")
                else:
                    final_stream_url = initial_url

            if "/s/" in final_stream_url:
                final_stream_url = final_stream_url.replace("/s/", "/r/")

            if not final_stream_url.endswith(".m3u8") and "?" not in final_stream_url:
                final_stream_url += ".m3u8"

            return {
                "extinf": channel["extinf"],
                "url": final_stream_url
            }

        except Exception as e:
            log.debug(f"Error resolving token: {e}")
            return None


async def main():
    log.info("Starting IPTV Playlist Generator...")
    async with httpx.AsyncClient(headers=HEADERS, follow_redirects=True) as client:
        raw_channels = await fetch_channels_raw(client)
        if not raw_channels:
            log.warning("No channels found! Exiting.")
            return

        log.info(f"Extracting 1st primary /r/ stream for {len(raw_channels)} channels...")
        tasks = [resolve_channel_stream(client, ch) for ch in raw_channels]
        resolved_results = await asyncio.gather(*tasks)

        valid_streams = [res for res in resolved_results if res is not None]
        log.info(f"Successfully resolved {len(valid_streams)} playable primary streams.")

        # ১. M3U প্লেলিস্ট (শুধুমাত্র #EXTINF এবং প্রথম ক্লিন /r/ লিঙ্ক)
        m3u_lines = ["#EXTM3U"]
        
        # ২. JSON ডেটা (যেখানে রেফারার ও অন্যান্য ডেটা সংরক্ষিত থাকবে)
        json_data = []

        for item in valid_streams:
            extinf = item["extinf"]
            stream_url = item["url"]

            # M3U আউটপুট: কোনো অতিরিক্ত হেডার বা পাইপ ছাড়া একদম ক্লিন লিঙ্ক
            m3u_lines.append(extinf)
            m3u_lines.append(stream_url)

            # JSON আউটপুট: সমস্ত হেডার সহ বিস্তারিত ডেটা
            play_link = f"{stream_url}|Referer={REFERER_URL}&Origin={ORIGIN_URL}&User-Agent={USER_AGENT}"
            json_data.append({
                "info": extinf,
                "url": stream_url,
                "playable_link": play_link,
                "headers": {
                    "Referer": REFERER_URL,
                    "Origin": ORIGIN_URL,
                    "User-Agent": USER_AGENT
                }
            })

        # ফাইল সেভ করা
        M3U_OUTPUT.write_text("\n".join(m3u_lines) + "\n", encoding="utf-8")
        JSON_OUTPUT.write_text(json.dumps(json_data, indent=2, ensure_ascii=False), encoding="utf-8")

        log.info(f"Saved {len(valid_streams)} clean channels to {M3U_OUTPUT.name} and full details to {JSON_OUTPUT.name}.")


if __name__ == "__main__":
    asyncio.run(main())
