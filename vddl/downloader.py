import base64
import concurrent.futures
import datetime
import json
import math
import random
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from collections import deque
from html import unescape
from pathlib import Path
from typing import Any, Deque, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, parse_qsl, unquote, urlencode, urljoin, urlparse, urlsplit, urlunsplit

import requests
from requests.adapters import HTTPAdapter

from .constants import (
    CHUNK_SIZE,
    DASH_CONTENT_TYPES,
    DEFAULT_HEADERS,
    DIRECT_MEDIA_SUFFIXES,
    DIRECT_RANGE_MIN_SIZE,
    DIRECT_RANGE_PART_SIZE_MAX,
    DIRECT_RANGE_PART_SIZE_MIN,
    DIRECT_RANGE_RETRIES,
    FRAGMENT_500_REDUCTION_THRESHOLD,
    FRAGMENT_REQUEUE_LIMIT,
    FRAGMENT_REQUEST_RETRIES,
    FRAGMENT_SKIP_STATUS_CODES,
    FRAGMENT_WORKER_COOLDOWN_SECONDS,
    GENERIC_HLS_STEMS,
    HLS_CONTENT_TYPES,
    HLS_TARGET_DURATION_FALLBACK,
    PROBE_READ_SIZE,
)
from .errors import DownloadError
from .models import (
    BrowserResource,
    DirectDownloadProbe,
    DownloadStats,
    EpisodeOption,
    FormatOption,
    ProbeResult,
    RangeJob,
    SegmentJob,
    SegmentResult,
    WorkerWindowState,
)
from .output import Colorizer, ProgressPrinter, _supports_color


class Downloader:
    def __init__(
        self,
        output: Optional[str],
        retries: int,
        timeout: float,
        workers: int,
        referer: Optional[str],
        quality: Optional[str],
        list_formats: bool,
        screen_mode: bool = False,
    ) -> None:
        self.output = output
        self.retries = retries
        self.timeout = timeout
        self.workers = workers
        self.referer = referer
        self.quality = (quality or "best").strip().lower()
        self.list_formats = list_formats
        self.session = requests.Session()
        self.session.headers.update(DEFAULT_HEADERS)
        pool_size = max(16, max(self.workers, 6) * 4)
        adapter = HTTPAdapter(
            pool_connections=pool_size,
            pool_maxsize=pool_size,
            pool_block=True,
        )
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        if referer:
            self.session.headers["Referer"] = referer
        self.stdout_colors = Colorizer(_supports_color(sys.stdout))
        self.printer = ProgressPrinter(self.stdout_colors, screen_mode=screen_mode)
        self._progress_lock = threading.Lock()
        self._browser_driver = None
        self._browser_lock = threading.Lock()

    def close(self) -> None:
        driver = self._browser_driver
        self._browser_driver = None
        if driver is not None:
            try:
                driver.quit()
            except Exception:
                pass

    def _download_tag(self) -> str:
        return self.stdout_colors.tag("[download]")

    def _fixup_tag(self) -> str:
        return self.stdout_colors.tag("[FixupM3u8]")

    def _probe_tag(self) -> str:
        return self.stdout_colors.tag("[probe]")

    def _hls_tag(self) -> str:
        return self.stdout_colors.tag("[hls]")

    def _image_hls_tag(self) -> str:
        return self.stdout_colors.tag("[ImageHls]")

    def _http_tag(self) -> str:
        return self.stdout_colors.tag("[http]")

    def _skip_tag(self) -> str:
        return self.stdout_colors.tag("[download]")

    def download(self, url: str) -> Path:
        if self._looks_like_mpd(url):
            self.printer.message(f"{self._probe_tag()} DASH manifest URL detected")
            raise DownloadError(
                "Native mode currently supports direct media URLs and HLS manifests only"
            )
        if self._looks_like_m3u8(url):
            self.printer.message(f"{self._probe_tag()} HLS manifest URL detected")
            if self.list_formats:
                self._list_hls_formats(url)
                return Path(".")
            return self._download_hls(url)

        self.printer.message(f"{self._probe_tag()} Inspecting resource type for {url}")
        probe = self._probe_resource(url)
        if probe.kind == "hls":
            self.printer.message(f"{self._probe_tag()} Detected HLS stream")
            if self.list_formats:
                self._list_hls_formats(probe.final_url)
                return Path(".")
            return self._download_hls(probe.final_url)
        if probe.kind in {"dash", "webpage"}:
            if probe.kind == "webpage":
                extracted = self._extract_supported_webpage_url(probe.final_url)
                if extracted:
                    self.printer.message(
                        f"{self._probe_tag()} Extracted media URL from webpage: "
                        f"{self.stdout_colors.notice(extracted)}"
                    )
                    return self.download(extracted)
            self.printer.message(
                f"{self._probe_tag()} Detected {'DASH stream' if probe.kind == 'dash' else 'webpage URL'}"
            )
            raise DownloadError(
                "Native mode currently supports direct media URLs and HLS manifests only; "
                "provide a direct .mp4 or .m3u8 URL"
            )
        self.printer.message(f"{self._probe_tag()} Detected direct media file")
        return self._download_http_file(probe.final_url)

    @staticmethod
    def _looks_like_m3u8(url: str) -> bool:
        return ".m3u8" in url.lower().split("?")[0]

    @staticmethod
    def _looks_like_mpd(url: str) -> bool:
        return ".mpd" in url.lower().split("?")[0]

    @staticmethod
    def _replace_suffix(name: str, suffix: str) -> str:
        path = Path(name)
        if path.suffix.lower() == suffix.lower():
            return str(path)
        if path.suffix:
            return str(path.with_suffix(suffix))
        return str(path.with_name(path.name + suffix))

    @staticmethod
    def _sanitize_output_part(value: str) -> str:
        cleaned = re.sub(r'[<>:"/\\|?*\x00-\x1f]+', "_", value).strip(" .")
        return cleaned or "download"

    @staticmethod
    def _is_generic_hls_stem(stem: str) -> bool:
        return stem.strip().lower() in GENERIC_HLS_STEMS

    def _guess_name_from_url(self, url: str) -> str:
        parsed = urlparse(url)
        path = Path(unquote(parsed.path))
        query = parse_qs(parsed.query)

        for key in ("filename", "file", "name", "title", "download"):
            values = query.get(key)
            if not values:
                continue
            candidate = Path(unquote(values[-1])).name.strip()
            if candidate:
                return self._sanitize_output_part(candidate)

        filename = path.name.strip()
        stem = path.stem.strip()
        if filename and stem and not self._is_generic_hls_stem(stem):
            return self._sanitize_output_part(filename)

        parent_name = path.parent.name.strip()
        if parent_name:
            suffix = path.suffix if path.suffix and path.suffix != "." else ""
            return self._sanitize_output_part(parent_name + suffix)

        if filename:
            return self._sanitize_output_part(filename)
        return "download.bin"

    def _set_runtime_referer(self, referer_url: str) -> None:
        self.session.headers["Referer"] = referer_url
        parsed = urlparse(referer_url)
        if parsed.scheme and parsed.netloc:
            self.session.headers["Origin"] = f"{parsed.scheme}://{parsed.netloc}"

    def _infer_output_name(
        self,
        url: str,
        response: Optional[requests.Response] = None,
        *,
        force_suffix: Optional[str] = None,
    ) -> str:
        if self.output:
            name = self.output
            return self._replace_suffix(name, force_suffix) if force_suffix else name

        if response is not None:
            cd = response.headers.get("Content-Disposition", "")
            match = re.search(r'filename\*?=(?:UTF-8\'\')?"?([^";]+)"?', cd)
            if match:
                name = Path(match.group(1)).name
                return self._replace_suffix(name, force_suffix) if force_suffix else name

        name = self._guess_name_from_url(url)
        if force_suffix:
            name = self._replace_suffix(name, force_suffix)
        elif name.endswith(".m3u8"):
            name = Path(name).stem + ".mp4"
        return name

    def _infer_hls_output_name(self, manifest_url: str, final_url: str) -> str:
        if self.output:
            return self._infer_output_name(manifest_url, force_suffix=".mp4")

        manifest_name = self._infer_output_name(manifest_url, force_suffix=".mp4")
        manifest_stem = Path(manifest_name).stem
        if not self._is_generic_hls_stem(manifest_stem):
            return manifest_name

        return self._infer_output_name(final_url, force_suffix=".mp4")

    @staticmethod
    def _content_type(value: str) -> str:
        return value.split(";", 1)[0].strip().lower()

    @staticmethod
    def _is_hls_content_type(content_type: str) -> bool:
        return content_type in HLS_CONTENT_TYPES

    @staticmethod
    def _is_dash_content_type(content_type: str) -> bool:
        return content_type in DASH_CONTENT_TYPES

    @staticmethod
    def _is_html_content_type(content_type: str) -> bool:
        return content_type.startswith("text/html") or content_type == "application/xhtml+xml"

    @staticmethod
    def _is_media_content_type(content_type: str) -> bool:
        return (
            content_type.startswith("video/")
            or content_type.startswith("audio/")
            or content_type in {"application/octet-stream", "binary/octet-stream"}
        )

    @staticmethod
    def _looks_like_direct_media(url: str) -> bool:
        return Path(urlparse(url).path).suffix.lower() in DIRECT_MEDIA_SUFFIXES

    @staticmethod
    def _host_matches(url: str, *suffixes: str) -> bool:
        host = urlparse(url).netloc.lower()
        return any(host == suffix or host.endswith("." + suffix) for suffix in suffixes)

    def _should_use_browser_transport(self, url: str) -> bool:
        return self._host_matches(url, "cdn-nanaplayer.com")

    def _extract_supported_webpage_url(self, url: str) -> Optional[str]:
        if self._host_matches(url, "overmovies.com"):
            self.printer.message(f"{self._probe_tag()} Extracting source from Overmovies page")
            return self._extract_overmovies_media_url(url)
        if self._host_matches(url, "037hddmovie.com"):
            self.printer.message(f"{self._probe_tag()} Extracting source from 037HDDMovie page")
            return self._extract_037hddmovie_media_url(url)
        if self._host_matches(url, "serie-days.com"):
            self.printer.message(f"{self._probe_tag()} Extracting source from Serie-Days page")
            return self._extract_seriedays_media_url(url)
        if self._host_matches(url, "goseries4k.com"):
            self.printer.message(f"{self._probe_tag()} Extracting source from GoSeries4K page")
            return self._extract_goseries4k_media_url(url)
        if self._host_matches(url, "movie2freehd.com"):
            self.printer.message(f"{self._probe_tag()} Extracting source from Movie2FreeHD page")
            return self._extract_movie2freehd_media_url(url)
        if self._host_matches(url, "proxyplayerth.com"):
            self.printer.message(f"{self._probe_tag()} Extracting source from ProxyPlayer page")
            return self._extract_proxyplayerth_media_url(url)
        if self._host_matches(url, "play-heyhd.com"):
            self.printer.message(f"{self._probe_tag()} Extracting source from Play-HeyHD page")
            return self._extract_playheyhd_media_url(url)
        if self._host_matches(url, "ok-nah.com"):
            self.printer.message(f"{self._probe_tag()} Extracting source from OK-NAH page")
            return self._extract_oknah_media_url(url)
        if self._host_matches(url, "24playerhd.com"):
            self.printer.message(f"{self._probe_tag()} Extracting source from 24PlayerHD page")
            return self._extract_24playerhd_media_url(url)
        return None

    def _ensure_browser_driver(self):
        if self._browser_driver is not None:
            return self._browser_driver

        try:
            from selenium import webdriver
            from selenium.webdriver.chrome.options import Options
        except ImportError as exc:
            raise DownloadError(
                "Browser fallback requires selenium. Install it with: python -m pip install selenium"
            ) from exc

        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--autoplay-policy=no-user-gesture-required")
        options.add_argument("--disable-gpu")
        options.add_argument("--log-level=3")

        try:
            driver = webdriver.Chrome(options=options)
        except Exception as exc:
            raise DownloadError(
                "Browser fallback requires Google Chrome/Chromedriver to be available"
            ) from exc

        driver.get("https://example.com/")
        self._browser_driver = driver
        return driver

    @staticmethod
    def _read_cdp_stream(driver: Any, handle: str) -> bytes:
        chunks: List[bytes] = []
        while True:
            payload = driver.execute_cdp_cmd("IO.read", {"handle": handle})
            data = payload.get("data", "")
            if data:
                if payload.get("base64Encoded"):
                    chunks.append(base64.b64decode(data))
                else:
                    chunks.append(data.encode("utf-8"))
            if payload.get("eof"):
                break
        driver.execute_cdp_cmd("IO.close", {"handle": handle})
        return b"".join(chunks)

    def _browser_fetch_resource(self, url: str) -> BrowserResource:
        with self._browser_lock:
            driver = self._ensure_browser_driver()
            root_frame = driver.execute_cdp_cmd("Page.getFrameTree", {})["frameTree"]["frame"]["id"]
            try:
                result = driver.execute_cdp_cmd(
                    "Network.loadNetworkResource",
                    {
                        "frameId": root_frame,
                        "url": url,
                        "options": {"disableCache": True, "includeCredentials": True},
                    },
                )
            except Exception as exc:
                raise DownloadError(f"Browser transport failed for {url}: {exc}") from exc

            resource = result.get("resource") or {}
            status = int(resource.get("httpStatusCode") or 0)
            headers = {
                str(key): str(value)
                for key, value in (resource.get("headers") or {}).items()
            }
            stream = resource.get("stream")
            body = self._read_cdp_stream(driver, stream) if stream else b""
            return BrowserResource(url=url, status_code=status, headers=headers, body=body)

    def _browser_fetch_text(self, url: str) -> Tuple[str, Dict[str, str]]:
        resource = self._browser_fetch_resource(url)
        if resource.status_code >= 400:
            raise DownloadError(f"[download] Got error: HTTP Error {resource.status_code}: browser fetch failed.")
        return self._decode_preview(resource.body), resource.headers

    def _browser_fetch_bytes(self, url: str) -> Tuple[bytes, Dict[str, str]]:
        resource = self._browser_fetch_resource(url)
        if resource.status_code >= 400:
            raise DownloadError(f"[download] Got error: HTTP Error {resource.status_code}: browser fetch failed.")
        return resource.body, resource.headers

    def _extract_overmovies_media_url(self, page_url: str) -> str:
        try:
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
        except ImportError as exc:
            raise DownloadError(
                "Overmovies extraction requires selenium. Install it with: python -m pip install selenium"
            ) from exc

        outer_frame_src, inner_frame_src = self._resolve_overmovies_frames(page_url)
        with self._browser_lock:
            driver = self._ensure_browser_driver()
            wait = WebDriverWait(driver, max(int(self.timeout), 20))
            driver.switch_to.default_content()
            driver.get(page_url)
            option = wait.until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#player-option-1, [id^='player-option-']"))
            )
            driver.execute_script("arguments[0].click();", option)
            outer = wait.until(
                lambda drv: (
                    drv.find_element(By.CSS_SELECTOR, "iframe.metaframe")
                    if drv.find_element(By.CSS_SELECTOR, "iframe.metaframe").get_attribute("src") == outer_frame_src
                    else None
                )
            )
            driver.switch_to.frame(outer)
            try:
                wait.until(
                    lambda drv: drv.execute_script(
                        "return document.readyState === 'complete' && !!document.querySelector('#apicodes-player');"
                    )
                )
                player_container = wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#apicodes-player"))
                )
                driver.execute_script(
                    """
                    arguments[0].innerHTML = '<iframe src="' + arguments[1] + '" width="100%" height="100%" frameborder="0" allowfullscreen="allowfullscreen"></iframe>';
                    """,
                    player_container,
                    inner_frame_src,
                )
                inner = wait.until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#apicodes-player iframe"))
                )
                driver.switch_to.frame(inner)
                play_button = wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#vid_play")))
                driver.execute_script("arguments[0].click();", play_button)
                wait.until(lambda drv: "/dl" in drv.execute_script("return location.href"))
                source = wait.until(
                    lambda drv: drv.execute_script(
                        """
                        if (!window.jwplayer) return null;
                        const item = jwplayer("vplayer").getPlaylistItem();
                        if (!item) return null;
                        return item.file || ((item.sources || [])[0] || {}).file || null;
                        """
                    )
                )
                if not source:
                    raise DownloadError("Failed to extract a playable source URL from Overmovies/NanaPlayer")
                return str(source)
            finally:
                driver.switch_to.default_content()

    def _resolve_overmovies_frames(self, page_url: str) -> Tuple[str, str]:
        resp = self._request_with_retry("GET", page_url, stream=False)
        try:
            html = resp.text
            final_page_url = resp.url
        finally:
            resp.close()

        player_match = re.search(
            r"data-type='([^']+)'\s+data-post='(\d+)'\s+data-nume='(\d+)'",
            html,
        )
        if not player_match:
            raise DownloadError("Failed to locate Overmovies player metadata on the webpage")

        post_type, post_id, nume = player_match.groups()
        ajax_url = urljoin(final_page_url, "/wp-admin/admin-ajax.php")
        ajax_resp = self.session.post(
            ajax_url,
            headers={
                **self.session.headers,
                "X-Requested-With": "XMLHttpRequest",
                "Referer": final_page_url,
            },
            data={"action": "doo_player_ajax", "post": post_id, "nume": nume, "type": post_type},
            timeout=self.timeout,
            allow_redirects=True,
        )
        try:
            ajax_resp.raise_for_status()
            ajax_data = ajax_resp.json()
        except requests.RequestException as exc:
            raise DownloadError(f"Failed to resolve Overmovies AJAX player: {exc}") from exc
        finally:
            ajax_resp.close()

        embed_url = str(ajax_data.get("embed_url", "")).replace("\\/", "/")
        if not embed_url:
            raise DownloadError("Overmovies did not return an embed URL")

        embed_resp = self._request_with_retry(
            "GET",
            embed_url,
            stream=False,
            headers={"Referer": final_page_url},
        )
        try:
            embed_html = embed_resp.text
        finally:
            embed_resp.close()

        servers = re.findall(
            r"loadSerieEpisode\('([^']+)',\s*'?([^,')]+)'?,\s*'([^']+)'\)",
            embed_html,
        )
        if not servers:
            raise DownloadError("Overmovies embed page did not expose any series servers")

        priority = ("s3", "m3u8", "mp4", "sb", "ok", "d2", "fe", "ab")
        picked = None
        for server in priority:
            picked = next((item for item in servers if item[2] == server), None)
            if picked:
                break
        if picked is None:
            picked = servers[0]

        source_api = urljoin(embed_url, f"/ajax/serie/get_sources/{picked[0]}/{picked[1]}/{picked[2]}")
        source_resp = self._request_with_retry(
            "GET",
            source_api,
            stream=False,
            headers={"Referer": embed_url, "X-Requested-With": "XMLHttpRequest"},
        )
        try:
            source_data = source_resp.json()
        finally:
            source_resp.close()

        inner_url = str(source_data.get("sources", "")).replace("\\/", "/")
        if not inner_url:
            raise DownloadError("Overmovies did not return a playable NanaPlayer iframe URL")
        return embed_url, inner_url

    def _extract_037hddmovie_media_url(self, page_url: str) -> str:
        resp = self._request_with_retry("GET", page_url, stream=False)
        try:
            html = resp.text
            final_page_url = resp.url
        finally:
            resp.close()

        iframe_matches = re.findall(r'<iframe[^>]+src="([^"]+)"', html, flags=re.IGNORECASE)
        leo_url = next((url for url in iframe_matches if "leoplayer7.com/watch" in url), None)
        if not leo_url:
            raise DownloadError("037HDDMovie page did not expose a LeoPlayer iframe")

        leo_resp = self._request_with_retry(
            "GET",
            leo_url,
            stream=False,
            headers={"Referer": final_page_url},
        )
        try:
            leo_html = leo_resp.text
        finally:
            leo_resp.close()

        api_match = re.search(r'"api":\s*"([^"]+)"', leo_html)
        if not api_match:
            raise DownloadError("LeoPlayer page did not expose a media API")

        media_api = api_match.group(1).replace("\\/", "/")
        media_resp = self._request_with_retry(
            "GET",
            media_api,
            stream=False,
            headers={"Referer": leo_url, "X-Requested-With": "XMLHttpRequest"},
        )
        try:
            media_data = media_resp.json().get("data", [])
        finally:
            media_resp.close()

        if isinstance(media_data, dict):
            media_items = [item for item in media_data.values() if isinstance(item, dict)]
        elif isinstance(media_data, list):
            media_items = [item for item in media_data if isinstance(item, dict)]
        else:
            media_items = []

        picked_api = None
        for group in ("mediahls1", "mediahls3", "mediahls2"):
            picked_api = next((item.get("api") for item in media_items if item.get("group") == group), None)
            if picked_api:
                break
        if not picked_api:
            raise DownloadError("LeoPlayer did not return a supported stream API")

        picked_resp = self._request_with_retry(
            "GET",
            str(picked_api).replace("\\/", "/"),
            stream=False,
            headers={"Referer": leo_url, "X-Requested-With": "XMLHttpRequest"},
        )
        try:
            source_url = str((picked_resp.json().get("data") or {}).get("source", {}).get("url", "")).replace("\\/", "/")
        finally:
            picked_resp.close()

        source_match = re.search(r"/p2p/([a-f0-9]+)$", source_url)
        if not source_match:
            raise DownloadError("Stream1689 source URL did not contain a media identifier")
        return f"https://master.streamhls.com/hls/{source_match.group(1)}/master"

    def _extract_movie2freehd_media_url(self, page_url: str) -> str:
        episode_options = self._extract_movie2freehd_episode_options(page_url)
        proxy_url = episode_options[0].url if episode_options else None
        if not proxy_url:
            raise DownloadError("Movie2FreeHD page did not expose a supported player URL")
        return self._extract_proxyplayerth_media_url(proxy_url)

    def _extract_movie2freehd_episode_options(self, page_url: str) -> List[EpisodeOption]:
        resp = self._request_with_retry("GET", page_url, stream=False)
        try:
            html = resp.text
        finally:
            resp.close()

        options: List[EpisodeOption] = []
        seen: set[str] = set()

        button_matches = re.findall(
            r'<button[^>]+data-source="([^"]+)"[^>]+data-name="([^"]+)"[^>]*>',
            html,
            flags=re.IGNORECASE,
        )
        for source_url, name in button_matches:
            normalized_url = source_url.replace("\\/", "/")
            if normalized_url in seen:
                continue
            seen.add(normalized_url)
            label = self._sanitize_output_part(unquote(name)).replace("_", " ")
            options.append(
                EpisodeOption(
                    index=len(options) + 1,
                    title=label or f"Episode {len(options) + 1}",
                    url=normalized_url,
                )
            )

        if options:
            return options

        iframe_matches = re.findall(r'<iframe[^>]+src="([^"]+)"', html, flags=re.IGNORECASE)
        proxy_url = next(
            (
                url
                for url in iframe_matches
                if "proxyplayerth.com/vod/" in url or "proxyplayerth.com/vod-stream/" in url
            ),
            None,
        )
        if not proxy_url:
            proxy_candidates = re.findall(
                r'https://proxyplayerth\.com/(?:vod|vod-stream)/[^"\'\s<>]+',
                html,
                flags=re.IGNORECASE,
            )
            proxy_url = next(iter(dict.fromkeys(proxy_candidates)), None)
        if proxy_url:
            options.append(EpisodeOption(index=1, title="Episode 1", url=proxy_url))
        return options

    @staticmethod
    def _clean_html_text(value: str) -> str:
        cleaned = re.sub(r"<[^>]+>", " ", value)
        cleaned = unescape(cleaned).replace("\xa0", " ")
        return " ".join(cleaned.split())

    @staticmethod
    def _infer_episode_number_from_url(url: str, fallback: str = "1") -> str:
        parsed = urlparse(url)
        path = unquote(parsed.path).lower()
        for pattern in (
            r"-ep-(\d+)(?:/|$)",
            r"/episode-?(\d+)(?:/|$)",
            r"/ep(\d+)(?:/|$)",
        ):
            match = re.search(pattern, path, flags=re.IGNORECASE)
            if match:
                return match.group(1)

        query = parse_qs(parsed.query)
        for key in ("episode", "ep"):
            values = query.get(key)
            if values and str(values[-1]).isdigit():
                return str(values[-1])
        return fallback

    def _extract_seriedays_episode_options(self, page_url: str) -> List[EpisodeOption]:
        resp = self._request_with_retry("GET", page_url, stream=False)
        try:
            html = resp.text
            final_page_url = resp.url
        finally:
            resp.close()

        select_match = re.search(
            r'<select[^>]*id\s*=\s*["\']eplist["\'][^>]*>(?P<body>.*?)</select>',
            html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not select_match:
            return []

        options: List[EpisodeOption] = []
        seen: set[str] = set()
        option_matches = re.findall(
            r'<option[^>]*value\s*=\s*["\']([^"\']+)["\'][^>]*>(.*?)</option>',
            select_match.group("body"),
            flags=re.IGNORECASE | re.DOTALL,
        )
        for raw_url, raw_label in option_matches:
            episode_url = urljoin(final_page_url, raw_url.strip())
            if episode_url in seen:
                continue
            seen.add(episode_url)
            label = self._clean_html_text(raw_label)
            options.append(
                EpisodeOption(
                    index=len(options) + 1,
                    title=label or f"Episode {len(options) + 1}",
                    url=episode_url,
                )
            )
        return options

    def _extract_seriedays_media_url(self, page_url: str) -> str:
        resp = self._request_with_retry("GET", page_url, stream=False)
        try:
            html = resp.text
            final_page_url = resp.url
        finally:
            resp.close()

        cfg_match = re.search(
            r"var\s+halim_cfg\s*=\s*(\{.*?\});",
            html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if not cfg_match:
            raise DownloadError("Serie-Days page did not expose player metadata")
        try:
            cfg = json.loads(cfg_match.group(1))
        except json.JSONDecodeError as exc:
            raise DownloadError("Serie-Days player metadata is invalid") from exc

        ajax_match = re.search(
            r"var\s+ajax_player\s*=\s*(\{.*?\});",
            html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        nonce = ""
        if ajax_match:
            try:
                nonce = str((json.loads(ajax_match.group(1)) or {}).get("nonce") or "")
            except json.JSONDecodeError:
                nonce = ""

        post_id = str(cfg.get("post_id") or "").strip()
        cfg_episode = str(cfg.get("episode") or 1).strip()
        episode = self._infer_episode_number_from_url(final_page_url, cfg_episode)
        if not post_id:
            post_match = re.search(r'data-post-id="(\d+)"', html, flags=re.IGNORECASE)
            post_id = post_match.group(1) if post_match else ""
        if not post_id:
            raise DownloadError("Serie-Days page did not expose a post id")

        lang_select_match = re.search(
            r'<select[^>]*id\s*=\s*["\']Lang_select["\'][^>]*>(?P<body>.*?)</select>',
            html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        languages: List[str] = []
        if lang_select_match:
            for raw_lang in re.findall(
                r'<option[^>]*value\s*=\s*["\']([^"\']+)["\']',
                lang_select_match.group("body"),
                flags=re.IGNORECASE,
            ):
                lang = self._clean_html_text(raw_lang)
                if lang and lang not in languages:
                    languages.append(lang)
        for fallback_lang in ("Thai", "Sound Track"):
            if fallback_lang not in languages:
                languages.append(fallback_lang)

        servers: List[str] = []
        for server_id in re.findall(r'data-server="(\d+)"', html, flags=re.IGNORECASE):
            if server_id == "1000":
                continue
            if server_id not in servers:
                servers.append(server_id)
        if not servers:
            servers = ["1"]

        api_url = urljoin(final_page_url, "/api/get.php")
        for lang in languages:
            for server_id in servers:
                api_resp = self.session.post(
                    api_url,
                    headers={
                        **self.session.headers,
                        "X-Requested-With": "XMLHttpRequest",
                        "Referer": final_page_url,
                    },
                    data={
                        "action": "halim_ajax_player",
                        "nonce": nonce,
                        "episode": episode,
                        "postid": post_id,
                        "lang": lang,
                        "server": server_id,
                    },
                    timeout=self.timeout,
                    allow_redirects=True,
                )
                try:
                    if api_resp.status_code >= 400:
                        continue
                    iframe_match = re.search(
                        r'<iframe[^>]+src=["\']([^"\']+)["\']',
                        api_resp.text.replace("\\/", "/"),
                        flags=re.IGNORECASE,
                    )
                    if not iframe_match:
                        continue
                    iframe_url = urljoin(final_page_url, iframe_match.group(1).strip())
                    if self._looks_like_m3u8(iframe_url) or self._looks_like_direct_media(iframe_url):
                        self._set_runtime_referer(final_page_url)
                        return iframe_url
                    nested = self._extract_supported_webpage_url(iframe_url)
                    if nested:
                        return nested
                finally:
                    api_resp.close()

        raise DownloadError("Serie-Days page did not expose a playable media URL")

    def _extract_24playerhd_media_url(
        self,
        page_url: str,
    ) -> str:
        parsed_page = urlparse(page_url)
        direct_m3u8 = self._looks_like_m3u8(page_url)
        if direct_m3u8:
            return page_url

        resp = self._request_with_retry(
            "GET",
            page_url,
            stream=False,
            headers={"Referer": f"{parsed_page.scheme}://{parsed_page.netloc}/"},
        )
        try:
            html = resp.text
            final_page_url = resp.url
        finally:
            resp.close()

        page_id = parse_qs(urlparse(final_page_url).query).get("id", [""])[-1].strip()
        if not page_id:
            id_match = re.search(r"[?&]id=([a-f0-9]+)", final_page_url, flags=re.IGNORECASE)
            page_id = id_match.group(1) if id_match else ""
        if not page_id:
            raise DownloadError("24PlayerHD page did not expose a media id")

        candidates = [
            urljoin(final_page_url, f"/newplaylist_g/{page_id}/{page_id}.m3u8"),
            urljoin(final_page_url, f"/newplaylist/{page_id}/{page_id}.m3u8"),
            urljoin(final_page_url, f"/m3u8/{page_id}/{page_id}.m3u8"),
        ]
        for raw_url in re.findall(r'https?://[^"\'\s<>]+\.m3u8[^"\'\s<>]*', html, flags=re.IGNORECASE):
            candidate = raw_url.replace("\\/", "/").replace("&amp;", "&").strip()
            if candidate not in candidates:
                candidates.append(candidate)

        for candidate in candidates:
            manifest_resp: Optional[requests.Response] = None
            try:
                manifest_resp = self.session.get(
                    candidate,
                    headers={**self.session.headers, "Referer": final_page_url},
                    timeout=self.timeout,
                    allow_redirects=True,
                )
                if manifest_resp.status_code >= 400:
                    continue
                preview = manifest_resp.text[:128].lstrip()
                if preview.upper().startswith("#EXTM3U"):
                    return manifest_resp.url
            except requests.RequestException:
                continue
            finally:
                if manifest_resp is not None:
                    manifest_resp.close()

        return candidates[0]

    @staticmethod
    def _update_url_query(url: str, updates: Dict[str, str]) -> str:
        split = urlsplit(url)
        params = dict(parse_qsl(split.query, keep_blank_values=True))
        params.update(updates)
        query = urlencode(params)
        return urlunsplit((split.scheme, split.netloc, split.path, query, split.fragment))

    @staticmethod
    def _extract_urls_from_html_fragment(fragment: str) -> List[str]:
        candidates: List[str] = []
        for raw_url in re.findall(r'data-id="(https?://[^"]+)"', fragment, flags=re.IGNORECASE):
            candidates.append(raw_url)
        for raw_url in re.findall(r'<iframe[^>]+src="([^"]+)"', fragment, flags=re.IGNORECASE):
            candidates.append(raw_url)
        for raw_url in re.findall(
            r'https?://[^"\'\s<>]+',
            fragment,
            flags=re.IGNORECASE,
        ):
            candidates.append(raw_url)

        normalized: List[str] = []
        seen: set[str] = set()
        for url in candidates:
            clean = url.replace("\\/", "/").replace("&amp;", "&").strip()
            if not clean.startswith(("http://", "https://")) or clean in seen:
                continue
            seen.add(clean)
            normalized.append(clean)
        return normalized

    def _pick_goseries4k_source_url(self, fragment: str) -> Optional[str]:
        candidates = self._extract_urls_from_html_fragment(fragment)
        if not candidates:
            return None

        def score(url: str) -> Tuple[int, int]:
            parsed = urlparse(url)
            host = parsed.netloc.lower()
            weight = 0
            if self._host_matches(url, "play-heyhd.com"):
                weight = 300
                if "/video/" in parsed.path:
                    weight += 20
                if parsed.query:
                    weight += 5
            elif self._host_matches(url, "ok-nah.com"):
                weight = 250
                if "/play/" in parsed.path:
                    weight += 10
            elif self._looks_like_m3u8(url) or self._looks_like_direct_media(url):
                weight = 200
            elif host:
                weight = 100
            return weight, -len(url)

        return max(candidates, key=score)

    def _extract_goseries4k_episode_options(self, page_url: str) -> List[EpisodeOption]:
        resp = self._request_with_retry("GET", page_url, stream=False)
        try:
            html = resp.text
        finally:
            resp.close()

        cache_map: Dict[str, str] = {}
        cache_match = re.search(
            r"window\.miru_ep_cache\s*=\s*(\{.*?\})\s*;",
            html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if cache_match:
            try:
                loaded = json.loads(cache_match.group(1))
                if isinstance(loaded, dict):
                    cache_map = {str(key): str(value) for key, value in loaded.items()}
            except json.JSONDecodeError:
                cache_map = {}

        options: List[EpisodeOption] = []
        seen: set[str] = set()
        button_matches = re.findall(
            r'<button[^>]*class="[^"]*mp-ep-btn[^"]*"[^>]*data-id="(\d+)"[^>]*>(.*?)</button>',
            html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        for episode_id, raw_label in button_matches:
            source_fragment = cache_map.get(episode_id, html)
            source_url = self._pick_goseries4k_source_url(source_fragment)
            if not source_url or source_url in seen:
                continue
            seen.add(source_url)
            label = re.sub(r"<[^>]+>", " ", raw_label)
            label = " ".join(label.split())
            options.append(
                EpisodeOption(
                    index=len(options) + 1,
                    title=label or f"Episode {len(options) + 1}",
                    url=source_url,
                )
            )

        if options:
            return options

        for episode_id in sorted(cache_map.keys(), key=lambda item: int(item) if item.isdigit() else 0):
            source_url = self._pick_goseries4k_source_url(cache_map[episode_id])
            if not source_url or source_url in seen:
                continue
            seen.add(source_url)
            options.append(
                EpisodeOption(
                    index=len(options) + 1,
                    title=f"Episode {len(options) + 1}",
                    url=source_url,
                )
            )

        if options:
            return options

        source_url = self._pick_goseries4k_source_url(html)
        if source_url:
            options.append(EpisodeOption(index=1, title="Episode 1", url=source_url))
        return options

    def _extract_goseries4k_media_url(self, page_url: str) -> str:
        episode_options = self._extract_goseries4k_episode_options(page_url)
        target_url = episode_options[0].url if episode_options else None
        if not target_url:
            raise DownloadError("GoSeries4K page did not expose a supported player URL")

        self._set_runtime_referer(page_url)
        nested = self._extract_supported_webpage_url(target_url)
        return nested or target_url

    def _extract_playheyhd_media_url(self, page_url: str) -> str:
        response: Optional[requests.Response] = None
        html = ""
        final_page_url = page_url
        referers: List[str] = [page_url]
        origin = f"{urlparse(page_url).scheme}://{urlparse(page_url).netloc}/"
        if origin not in referers:
            referers.append(origin)
        default_referer = self.session.headers.get("Referer", "").strip()
        if default_referer and default_referer not in referers:
            referers.append(default_referer)
        if "https://goseries4k.com/" not in referers:
            referers.append("https://goseries4k.com/")

        for referer in referers:
            response = self._request_with_retry(
                "GET",
                page_url,
                stream=False,
                headers={"Referer": referer},
            )
            try:
                html = response.text
                final_page_url = response.url
                if "MASPlayer(" in html and "videoUrl" in html:
                    break
            finally:
                response.close()

        payload_match = re.search(
            r"MASPlayer\(\s*vhash\s*,\s*(\{.*?\})\s*,\s*false\s*\)\s*;",
            html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if payload_match:
            try:
                payload = json.loads(payload_match.group(1))
            except json.JSONDecodeError:
                payload = {}

            if isinstance(payload, dict):
                raw_video_url = str(payload.get("videoUrl") or "").replace("\\/", "/")
                video_server = str(payload.get("videoServer") or "")
                video_disk = str(payload.get("videoDisk") or "")
                if raw_video_url:
                    manifest_url = urljoin(final_page_url, raw_video_url)
                    manifest_url = self._update_url_query(
                        manifest_url,
                        {
                            "s": video_server,
                            "d": base64.b64encode(video_disk.encode("utf-8")).decode("ascii"),
                        },
                    )
                    self._set_runtime_referer(final_page_url)
                    return manifest_url

                source_items = ((payload.get("videoData") or {}).get("videoSources") or [])
                for source in source_items:
                    if not isinstance(source, dict):
                        continue
                    source_url = str(source.get("file") or "").replace("\\/", "/")
                    if not source_url:
                        continue
                    if re.match(r"^https?://\d+/", source_url):
                        split = urlsplit(source_url)
                        source_url = urlunsplit(
                            (split.scheme, urlsplit(final_page_url).netloc, split.path, split.query, split.fragment)
                        )
                    if "/cdn/hls/" in source_url:
                        source_url = self._update_url_query(
                            source_url,
                            {
                                "s": video_server,
                                "d": base64.b64encode(video_disk.encode("utf-8")).decode("ascii"),
                            },
                        )
                    self._set_runtime_referer(final_page_url)
                    return source_url

        file_match = re.search(
            r"""file\s*:\s*['"](?P<url>https?://[^'"]+\.(?:m3u8|mp4|txt)(?:\?[^'"]*)?)['"]""",
            html,
            flags=re.IGNORECASE,
        )
        if file_match:
            self._set_runtime_referer(final_page_url)
            return file_match.group("url").replace("\\/", "/")

        raise DownloadError("Play-HeyHD page did not expose a playable media URL")

    def _extract_oknah_media_url(self, page_url: str) -> str:
        resp = self._request_with_retry(
            "GET",
            page_url,
            stream=False,
            headers={"Referer": page_url},
        )
        try:
            html = resp.text
            final_page_url = resp.url
        finally:
            resp.close()

        source_match = re.search(
            r"""sources\s*:\s*\[\s*\{\s*file\s*:\s*["'](?P<url>https?://[^"']+)["']""",
            html,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if source_match:
            self._set_runtime_referer(final_page_url)
            return source_match.group("url").replace("\\/", "/").replace("&amp;", "&")

        file_match = re.search(
            r"""file\s*:\s*["'](?P<url>https?://[^"']+\.(?:m3u8|mp4)(?:\?[^"']*)?)["']""",
            html,
            flags=re.IGNORECASE,
        )
        if file_match:
            self._set_runtime_referer(final_page_url)
            return file_match.group("url").replace("\\/", "/").replace("&amp;", "&")

        iframe_matches = re.findall(r'<iframe[^>]+src="([^"]+)"', html, flags=re.IGNORECASE)
        next_url = next((url for url in iframe_matches if url.startswith("http")), None)
        if next_url and next_url != final_page_url:
            nested = self._extract_supported_webpage_url(next_url)
            if nested:
                return nested

        raise DownloadError("OK-NAH page did not expose a playable media URL")

    def _extract_proxyplayerth_media_url(self, page_url: str) -> str:
        resp = self._request_with_retry("GET", page_url, stream=False)
        try:
            html = resp.text
            final_page_url = resp.url
        finally:
            resp.close()

        direct_match = re.search(
            r"""var\s+url\s*=\s*['"](?P<url>https?://[^'"]+\.(?:m3u8|mp4)(?:\?[^'"]*)?)['"]""",
            html,
            flags=re.IGNORECASE,
        )
        if direct_match:
            self._set_runtime_referer(final_page_url)
            return direct_match.group("url").replace("\\/", "/")

        file_match = re.search(
            r"""file\s*:\s*['"](?P<url>https?://[^'"]+\.(?:m3u8|mp4)(?:\?[^'"]*)?)['"]""",
            html,
            flags=re.IGNORECASE,
        )
        if file_match:
            self._set_runtime_referer(final_page_url)
            return file_match.group("url").replace("\\/", "/")

        iframe_matches = re.findall(r'<iframe[^>]+src="([^"]+)"', html, flags=re.IGNORECASE)
        next_proxy_url = next(
            (
                url
                for url in iframe_matches
                if "proxyplayerth.com/vod-stream/" in url or "proxyplayerth.com/vod/" in url
            ),
            None,
        )
        if next_proxy_url and next_proxy_url != final_page_url:
            return self._extract_proxyplayerth_media_url(next_proxy_url)

        raise DownloadError("ProxyPlayer page did not expose a playable media URL")

    @staticmethod
    def _decode_preview(data: bytes) -> str:
        return data.decode("utf-8", errors="ignore").lstrip("\ufeff").strip()

    @staticmethod
    def _unwrap_proxy_media_url(url: str) -> str:
        parsed = urlparse(url)
        host = parsed.netloc.lower()
        if host.endswith("googleusercontent.com") and "/gadgets/proxy" in parsed.path.lower():
            target = (parse_qs(parsed.query).get("url") or [""])[-1].strip()
            if target.startswith(("http://", "https://")):
                return target
        return url

    def _format_variant_label(self, variant: Tuple[int, Optional[int], str]) -> str:
        bandwidth, height, _ = variant
        res_text = f"{height}p" if height else "unknown"
        bw_text = f"{bandwidth / 1_000_000:.2f}Mb/s" if bandwidth else "unknown"
        return f"{res_text} @ {bw_text}"

    @staticmethod
    def _format_audio_label(audio_info: Optional[Dict[str, str]]) -> str:
        if not audio_info:
            return "no companion audio"
        name = (audio_info.get("name") or "").strip()
        language = (audio_info.get("language") or "").strip()
        try:
            name.encode("ascii")
        except UnicodeEncodeError:
            name = ""
        pieces = [part for part in (name, language) if part]
        label = " / ".join(pieces) if pieces else "audio"
        if audio_info.get("is_default") == "yes":
            label += " (default)"
        return label

    @staticmethod
    def _state_path(output: Path) -> Path:
        return output.with_suffix(output.suffix + ".vddl-state.json")

    def _write_resume_state(self, output: Path, payload: Dict[str, Any]) -> None:
        state_path = self._state_path(output)
        state = {
            "updated_at": int(time.time()),
            **payload,
        }
        state_path.write_text(json.dumps(state, indent=2), encoding="utf-8")

    def _load_resume_state(self, output: Path) -> Optional[Dict[str, Any]]:
        state_path = self._state_path(output)
        if not state_path.exists():
            return None
        try:
            return json.loads(state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None

    def _delete_resume_state(self, output: Path) -> None:
        self._cleanup_file(self._state_path(output))

    def _load_manifest_text(self, manifest_url: str) -> Tuple[str, str]:
        if self._should_use_browser_transport(manifest_url):
            text, _ = self._browser_fetch_text(manifest_url)
            return text, manifest_url

        resp = self._request_with_retry("GET", manifest_url, stream=False)
        try:
            return resp.text, resp.url
        finally:
            resp.close()

    def _collect_hls_format_options(self, manifest_url: str) -> Tuple[List[FormatOption], str]:
        text, final_url = self._load_manifest_text(manifest_url)

        if "#EXT-X-STREAM-INF" not in text:
            audio_label = "no companion audio"
            inferred_audio = self._infer_streamhls_audio_playlist(final_url)
            if inferred_audio:
                audio_label = self._format_audio_label(
                    {
                        "name": "Thai",
                        "language": "tha",
                        "is_default": "yes",
                    }
                )
            return (
                [
                    FormatOption(
                        index=1,
                        bandwidth=0,
                        height=None,
                        url=final_url,
                        audio_label=audio_label,
                        is_direct=True,
                    )
                ],
                final_url,
            )

        variants = sorted(
            self._parse_variant_playlist(text, final_url),
            key=lambda item: (item[1] or 0, item[0]),
            reverse=True,
        )
        if not variants:
            raise DownloadError("No media variants found in manifest")

        audio_groups = self._parse_audio_renditions(text, final_url)
        options: List[FormatOption] = []
        for idx, variant in enumerate(variants, 1):
            audio_group = self._find_variant_audio_group(text, final_url, variant[2])
            audio_info = audio_groups.get(audio_group) if audio_group else None
            if audio_info is None:
                inferred_audio = self._infer_streamhls_audio_playlist(variant[2])
                if inferred_audio:
                    audio_info = {
                        "url": inferred_audio,
                        "name": "Thai",
                        "language": "tha",
                        "is_default": "yes",
                    }
            options.append(
                FormatOption(
                    index=idx,
                    bandwidth=variant[0],
                    height=variant[1],
                    url=variant[2],
                    audio_label=self._format_audio_label(audio_info),
                )
            )
        return options, final_url

    def get_format_options(self, url: str) -> List[FormatOption]:
        if self._looks_like_m3u8(url):
            options, _ = self._collect_hls_format_options(url)
            return options

        probe = self._probe_resource(url)
        if probe.kind == "webpage":
            extracted = self._extract_supported_webpage_url(probe.final_url)
            if extracted:
                return self.get_format_options(extracted)
            return []
        if probe.kind == "hls":
            options, _ = self._collect_hls_format_options(probe.final_url)
            return options
        return []

    def get_episode_options(self, url: str) -> List[EpisodeOption]:
        if self._host_matches(url, "serie-days.com"):
            return self._extract_seriedays_episode_options(url)
        if self._host_matches(url, "goseries4k.com"):
            return self._extract_goseries4k_episode_options(url)
        if self._host_matches(url, "movie2freehd.com"):
            return self._extract_movie2freehd_episode_options(url)
        return []

    def _pick_hls_variant(
        self,
        variants: List[Tuple[int, Optional[int], str]],
    ) -> Tuple[int, Optional[int], str]:
        if not variants:
            raise DownloadError("No media variants found in manifest")

        quality = self.quality or "best"
        if quality == "worst":
            return min(variants, key=lambda item: (item[1] or 0, item[0]))
        if quality in {"", "best", "auto"}:
            return max(variants, key=lambda item: (item[1] or 0, item[0]))

        match = re.fullmatch(r"(\d+)(?:p)?", quality)
        if not match:
            raise DownloadError(
                f"Unsupported quality selector: {self.quality}. Use best, worst, or a height like 720"
            )

        target = int(match.group(1))
        exact = [item for item in variants if item[1] == target]
        if exact:
            return max(exact, key=lambda item: item[0])

        below = [item for item in variants if item[1] is not None and item[1] < target]
        if below:
            return max(below, key=lambda item: (item[1] or 0, item[0]))

        above = [item for item in variants if item[1] is not None and item[1] > target]
        if above:
            return min(above, key=lambda item: ((item[1] or 0) - target, -item[0]))

        return max(variants, key=lambda item: item[0])

    def _list_hls_formats(self, manifest_url: str) -> None:
        self.printer.message(f"{self._hls_tag()} Fetching HLS manifest")
        options, final_url = self._collect_hls_format_options(manifest_url)
        if len(options) == 1 and options[0].is_direct:
            self.printer.message(f"{self._hls_tag()} Single media playlist")
            self.printer.message(
                f"{self._hls_tag()} 1. direct [audio: {options[0].audio_label}] -> {final_url}"
            )
            return

        self.printer.message(f"{self._hls_tag()} Available formats:")
        for option in options:
            self.printer.message(
                f"{self._hls_tag()} {option.index}. "
                f"{self._format_variant_label((option.bandwidth, option.height, option.url))} "
                f"[audio: {option.audio_label}] -> {option.url}"
            )

    @staticmethod
    def _parse_retry_after(value: Optional[str]) -> Optional[float]:
        if not value:
            return None
        try:
            return max(float(value), 0.0)
        except ValueError:
            return None

    def _signed_url_hint(self, url: str, status: object) -> str:
        if status != 403:
            return ""

        query = parse_qs(urlparse(url).query)
        interesting = [key for key in ("token", "sig", "signature", "t", "s", "e", "expires", "exp") if key in query]
        if not interesting:
            return ""

        now_utc = datetime.datetime.now(datetime.timezone.utc)
        bangkok = datetime.timezone(datetime.timedelta(hours=7))
        hints = [
            "This URL looks signed or session-bound",
        ]
        s_value = next((query[key][-1] for key in ("s", "expires", "exp") if key in query and query[key]), None)
        e_value = query.get("e", [None])[-1]
        if s_value and s_value.isdigit():
            s_ts = int(s_value)
            s_utc = datetime.datetime.fromtimestamp(s_ts, tz=datetime.timezone.utc)
            s_bkk = s_utc.astimezone(bangkok)
            hints.append(
                f"s={s_ts} ({s_utc:%Y-%m-%d %H:%M:%S} UTC / {s_bkk:%Y-%m-%d %H:%M:%S %z})"
            )
            if s_ts < int(now_utc.timestamp()):
                hints.append(
                    f"At {now_utc:%Y-%m-%d %H:%M:%S} UTC, that timestamp is already in the past"
                )
            elif e_value and e_value.isdigit():
                expiry_ts = s_ts + int(e_value)
                expiry_utc = datetime.datetime.fromtimestamp(expiry_ts, tz=datetime.timezone.utc)
                expiry_bkk = expiry_utc.astimezone(bangkok)
                hints.append(
                    f"If e={e_value} is a TTL, expiry would be {expiry_utc:%Y-%m-%d %H:%M:%S} UTC / "
                    f"{expiry_bkk:%Y-%m-%d %H:%M:%S %z}"
                )
        return " " + ". ".join(hints) + ". Open the webpage again and copy a fresh manifest URL, or pass the required --referer if the site expects one."

    @staticmethod
    def _parse_content_range_total(value: str) -> int:
        match = re.match(r"bytes\s+\d+-\d+/(\d+|\*)", value.strip(), flags=re.IGNORECASE)
        if not match:
            return 0
        total = match.group(1)
        return int(total) if total.isdigit() else 0

    def _refresh_progress(self, stats: DownloadStats) -> None:
        with self._progress_lock:
            stats.total_bytes = max(stats.total_bytes, stats.done_bytes)
            self.printer.progress(stats)

    def _update_progress(
        self,
        stats: DownloadStats,
        *,
        byte_delta: int = 0,
        fragment_delta: int = 0,
        total_delta: int = 0,
    ) -> None:
        with self._progress_lock:
            stats.done_bytes = max(stats.done_bytes + byte_delta, 0)
            stats.done_fragments += fragment_delta
            stats.total_bytes = max(stats.total_bytes + total_delta, stats.done_bytes)
            self.printer.progress(stats)

    def _retry_delay(
        self,
        attempt: int,
        *,
        response: Optional[requests.Response] = None,
        fragment: bool = False,
    ) -> float:
        retry_after = self._parse_retry_after(
            response.headers.get("Retry-After") if response is not None else None
        )
        if retry_after is not None:
            return min(retry_after, 30.0)

        if response is not None and response.status_code >= 500:
            if fragment:
                base_delay = min(0.75 * attempt + 0.25, 3.0)
                return max(0.25, base_delay * random.uniform(0.85, 1.15))
            return min(2.0 * attempt + 1.0, 15.0)

        base_delay = min(0.5 * attempt + 0.25, 2.0)
        if fragment:
            return max(0.25, base_delay * random.uniform(0.85, 1.15))
        return base_delay

    @staticmethod
    def _segment_host(seg: SegmentJob) -> str:
        return urlparse(seg.url).netloc.lower() or "<local>"

    def _resolve_hls_workers(self, segments: List[SegmentJob]) -> int:
        if self.workers > 0:
            return min(self.workers, max(len(segments), 1))

        host_count = len({self._segment_host(seg) for seg in segments}) or 1
        auto_workers = 12 if host_count == 1 else max(8, host_count * 3)
        return min(auto_workers, max(len(segments), 1), 24)

    def _build_host_windows(
        self,
        segments: List[SegmentJob],
        effective_workers: Optional[int] = None,
    ) -> Dict[str, WorkerWindowState]:
        hosts = sorted({self._segment_host(seg) for seg in segments})
        if not hosts:
            return {}

        if effective_workers is None:
            effective_workers = self._resolve_hls_workers(segments)
        if len(hosts) == 1:
            per_host_limit = effective_workers
            initial_limit = min(per_host_limit, 4)
        else:
            per_host_limit = max(1, math.ceil(effective_workers / len(hosts)))
            initial_limit = per_host_limit

        return {
            host: WorkerWindowState(
                configured_limit=per_host_limit,
                active_limit=initial_limit,
            )
            for host in hosts
        }

    def _note_fragment_backpressure(
        self,
        host: str,
        window: WorkerWindowState,
        count: int,
        *,
        timeout_related: bool = False,
    ) -> None:
        if count <= 0:
            return

        with window.lock:
            window.consecutive_server_errors += count
            window.success_streak = 0
            min_limit = 3 if window.configured_limit >= 6 else (2 if window.configured_limit >= 4 else 1)
            if (
                window.consecutive_server_errors < FRAGMENT_500_REDUCTION_THRESHOLD
                or window.active_limit <= min_limit
            ):
                return

            step = 2 if window.active_limit >= 6 else 1
            new_limit = max(min_limit, window.active_limit - step)
            if new_limit == window.active_limit:
                return

            window.active_limit = new_limit
            window.cooldown_until = time.time() + FRAGMENT_WORKER_COOLDOWN_SECONDS
            window.consecutive_server_errors = 0

        self.printer.message(
            f"{self._hls_tag()} "
            f"{'Transient fragment failures' if timeout_related else 'HTTP 500 burst'} "
            f"on {self.stdout_colors.warning(host)}; temporarily reducing concurrent fragments "
            f"for this host to "
            f"{self.stdout_colors.warning(str(new_limit))} "
            f"for {FRAGMENT_WORKER_COOLDOWN_SECONDS:.0f}s"
        )

    def _note_fragment_success(
        self,
        host: str,
        window: WorkerWindowState,
        skipped: bool,
    ) -> None:
        now = time.time()
        new_limit: Optional[int] = None

        with window.lock:
            window.consecutive_server_errors = 0
            if skipped:
                return

            window.success_streak += 1
            if (
                window.active_limit >= window.configured_limit
                or now < window.cooldown_until
                or window.success_streak < max(1, math.ceil(window.active_limit / 3))
            ):
                return

            gap = window.configured_limit - window.active_limit
            step = 3 if gap >= 4 else (2 if gap >= 2 else 1)
            new_limit = min(window.configured_limit, window.active_limit + step)
            if new_limit == window.active_limit:
                return

            window.active_limit = new_limit
            window.success_streak = 0
            if new_limit >= window.configured_limit:
                window.cooldown_until = 0.0

        self.printer.message(
            f"{self._hls_tag()} Restoring concurrent fragments for "
            f"{self.stdout_colors.notice(host)} to {self.stdout_colors.notice(str(new_limit))}"
        )

    def _probe_from_response(
        self,
        response: requests.Response,
        preview_text: str = "",
    ) -> ProbeResult:
        content_type = self._content_type(response.headers.get("Content-Type", ""))
        final_url = response.url

        if self._looks_like_m3u8(final_url) or self._is_hls_content_type(content_type):
            return ProbeResult("hls", final_url, content_type, preview_text)
        if self._looks_like_mpd(final_url) or self._is_dash_content_type(content_type):
            return ProbeResult("dash", final_url, content_type, preview_text)
        if self._is_media_content_type(content_type):
            return ProbeResult("file", final_url, content_type, preview_text)
        if self._is_html_content_type(content_type):
            return ProbeResult("webpage", final_url, content_type, preview_text)

        preview_head = preview_text[:PROBE_READ_SIZE].lstrip()
        preview_upper = preview_head.upper()
        if preview_upper.startswith("#EXTM3U"):
            return ProbeResult("hls", final_url, content_type, preview_text)
        if preview_head.startswith("<?xml") and "<MPD" in preview_upper:
            return ProbeResult("dash", final_url, content_type, preview_text)
        if "<MPD" in preview_upper:
            return ProbeResult("dash", final_url, content_type, preview_text)
        if "<html" in preview_upper or "<!DOCTYPE HTML" in preview_upper:
            return ProbeResult("webpage", final_url, content_type, preview_text)
        if preview_head.startswith("#EXT-X-STREAM-INF") or preview_head.startswith("#EXTINF"):
            return ProbeResult("hls", final_url, content_type, preview_text)
        if preview_text and not preview_head.startswith(("{", "[")):
            return ProbeResult("webpage", final_url, content_type, preview_text)

        if self._looks_like_direct_media(final_url):
            return ProbeResult("file", final_url, content_type, preview_text)
        return ProbeResult("file", final_url, content_type, preview_text)

    def _probe_resource(self, url: str) -> ProbeResult:
        if self._looks_like_direct_media(url):
            return ProbeResult("file", url)
        if self._should_use_browser_transport(url):
            try:
                preview, headers = self._browser_fetch_text(url)
                return self._probe_from_response(
                    type(
                        "_BrowserProbeResponse",
                        (),
                        {
                            "headers": headers,
                            "url": url,
                        },
                    )(),
                    preview[:PROBE_READ_SIZE],
                )
            except DownloadError:
                pass

        head_resp = None
        try:
            head_resp = self._request_with_retry("HEAD", url, stream=False)
            probe = self._probe_from_response(head_resp)
            if probe.kind in {"hls", "dash", "webpage"} and probe.content_type:
                return probe
            if probe.kind == "file" and (
                self._is_media_content_type(probe.content_type)
                or self._looks_like_direct_media(probe.final_url)
            ):
                return probe
        except Exception:
            pass
        finally:
            if head_resp is not None:
                head_resp.close()

        resp = self._request_with_retry(
            "GET",
            url,
            stream=True,
            headers={
                "Range": f"bytes=0-{PROBE_READ_SIZE - 1}",
                "Accept-Encoding": "identity",
            },
        )
        preview = b""
        try:
            for chunk in resp.iter_content(chunk_size=PROBE_READ_SIZE):
                if chunk:
                    preview = chunk[:PROBE_READ_SIZE]
                    break
            return self._probe_from_response(resp, self._decode_preview(preview))
        finally:
            resp.close()

    def _sniff_hls_segment_mode(self, segments: List[SegmentJob]) -> str:
        sample_segment = next((seg for seg in segments if not seg.is_init), None)
        if sample_segment is None:
            return ""

        if self._should_use_browser_transport(sample_segment.url):
            try:
                sample, _ = self._browser_fetch_bytes(sample_segment.url)
                sample = sample[:4096]
            except DownloadError:
                return ""
            if self._find_embedded_ts_payload(sample) >= 0:
                return ""
            head = sample[:64]
            if head.startswith(b"\x89PNG\r\n\x1a\n"):
                return "png"
            if head.startswith(b"\xff\xd8\xff"):
                return "jpeg"
            if head.startswith((b"GIF87a", b"GIF89a")):
                return "gif"
            if head.startswith(b"RIFF") and head[8:12] == b"WEBP":
                return "webp"
            return ""

        resp: Optional[requests.Response] = None
        try:
            headers = self._segment_headers(sample_segment)
            headers["Range"] = "bytes=0-4095"
            headers["Accept-Encoding"] = "identity"
            resp = self.session.request(
                "GET",
                sample_segment.url,
                headers={**self.session.headers, **headers},
                stream=True,
                timeout=self.timeout,
                allow_redirects=True,
            )
            resp.raise_for_status()
            sample = b""
            for chunk in resp.iter_content(chunk_size=4096):
                if chunk:
                    sample += chunk
                if len(sample) >= 4096:
                    sample = sample[:4096]
                    break
        except Exception:
            return ""
        finally:
            if resp is not None:
                resp.close()

        if self._find_embedded_ts_payload(sample) >= 0:
            return ""
        head = sample[:64]
        if head.startswith(b"\x89PNG\r\n\x1a\n"):
            return "png"
        if head.startswith(b"\xff\xd8\xff"):
            return "jpeg"
        if head.startswith((b"GIF87a", b"GIF89a")):
            return "gif"
        if head.startswith(b"RIFF") and head[8:12] == b"WEBP":
            return "webp"
        return ""

    @staticmethod
    def _looks_like_image_segment(seg: SegmentJob) -> bool:
        suffix = Path(urlparse(seg.url).path).suffix.lower()
        return suffix in {".gif", ".jpeg", ".jpg", ".png", ".webp"}

    @staticmethod
    def _manifest_declares_audio(playlist_text: str) -> bool:
        return any(
            line.upper().startswith("#EXT-X-MEDIA")
            and "TYPE=AUDIO" in line.upper()
            for line in playlist_text.splitlines()
        )

    def _probe_segment_has_audio(self, seg: SegmentJob) -> Optional[bool]:
        ffprobe_path = shutil.which("ffprobe")
        if not ffprobe_path:
            return None
        if seg.byte_range is not None:
            return None

        try:
            if self._should_use_browser_transport(seg.url):
                body, _ = self._browser_fetch_bytes(seg.url)
            else:
                resp = self.session.get(
                    seg.url,
                    headers={**self.session.headers, "Accept-Encoding": "identity"},
                    timeout=self.timeout,
                    allow_redirects=True,
                )
                try:
                    resp.raise_for_status()
                    body = resp.content
                finally:
                    resp.close()
        except Exception:
            return None

        payload = body
        payload_start = self._find_embedded_ts_payload(body)
        if payload_start >= 0:
            payload = body[payload_start:]
        elif body.startswith((b"\x89PNG\r\n\x1a\n", b"\xff\xd8\xff", b"GIF87a", b"GIF89a")):
            return False

        with tempfile.NamedTemporaryFile(suffix=".ts", delete=False) as tmp:
            tmp_path = Path(tmp.name)
            tmp.write(payload)

        try:
            probe = subprocess.run(
                [
                    ffprobe_path,
                    "-v",
                    "error",
                    "-show_entries",
                    "stream=codec_type",
                    "-of",
                    "default=noprint_wrappers=1:nokey=0",
                    str(tmp_path),
                ],
                capture_output=True,
                text=True,
            )
            if probe.returncode != 0:
                return None
            return "codec_type=audio" in probe.stdout
        finally:
            self._cleanup_file(tmp_path)

    def _warn_if_playlist_appears_video_only(
        self,
        playlist_text: str,
        segments: List[SegmentJob],
    ) -> None:
        if self._manifest_declares_audio(playlist_text):
            return

        sample_segment = next(
            (seg for seg in segments if not seg.is_init and self._looks_like_image_segment(seg)),
            None,
        )
        if sample_segment is None:
            return

        has_audio = self._probe_segment_has_audio(sample_segment)
        if has_audio is False:
            self.printer.message(
                f"{self._hls_tag()} This playlist looks video-only; "
                f"{self.stdout_colors.warning('the output may be silent unless the webpage provides a separate audio track')}"
            )

    def _request_with_retry(
        self,
        method: str,
        url: str,
        *,
        stream: bool = False,
        headers: Optional[Dict[str, str]] = None,
    ) -> requests.Response:
        merged_headers = dict(self.session.headers)
        if headers:
            merged_headers.update(headers)

        last_exc: Optional[Exception] = None
        for attempt in range(1, self.retries + 1):
            try:
                resp = self.session.request(
                    method,
                    url,
                    headers=merged_headers,
                    stream=stream,
                    timeout=self.timeout,
                    allow_redirects=True,
                )
                if resp.status_code >= 500:
                    resp.close()
                    msg = f"[download] Got error: HTTP Error {resp.status_code}: {resp.reason}."
                    if attempt < self.retries:
                        delay = self._retry_delay(attempt, response=resp)
                        self.printer.message(
                            f"{self._http_tag()} Got error: HTTP Error {resp.status_code}: "
                            f"{resp.reason}. Retrying ({attempt}/{self.retries}) after "
                            f"{delay:.1f}s..."
                        )
                        time.sleep(delay)
                        continue
                    self.printer.message(
                        f"{self._http_tag()} Got error: HTTP Error {resp.status_code}: "
                        f"{resp.reason}. Giving up after {self.retries} retries"
                    )
                    raise DownloadError(msg)
                resp.raise_for_status()
                return resp
            except (requests.Timeout, requests.ConnectionError) as exc:
                last_exc = exc
                msg = f"[download] Got error: {exc.__class__.__name__}: {exc}."
                if attempt < self.retries:
                    delay = self._retry_delay(attempt)
                    self.printer.message(
                        f"{self._http_tag()} Got error: {exc.__class__.__name__}: {exc}. "
                        f"Retrying ({attempt}/{self.retries}) after {delay:.1f}s..."
                    )
                    time.sleep(delay)
                    continue
                self.printer.message(
                    f"{self._http_tag()} Got error: {exc.__class__.__name__}: {exc}. "
                    f"Giving up after {self.retries} retries"
                )
                raise DownloadError(msg) from exc
            except requests.HTTPError as exc:
                last_exc = exc
                status = exc.response.status_code if exc.response is not None else "?"
                reason = exc.response.reason if exc.response is not None else str(exc)
                hint = self._signed_url_hint(url, status)
                msg = f"[download] Got error: HTTP Error {status}: {reason}.{hint}"
                if status in {403, 404} or attempt >= self.retries:
                    self.printer.message(
                        f"{self._http_tag()} Got error: HTTP Error {status}: {reason}. "
                        f"Giving up after {min(attempt, self.retries)} retries"
                    )
                    raise DownloadError(msg) from exc
                delay = self._retry_delay(attempt, response=exc.response)
                self.printer.message(
                    f"{self._http_tag()} Got error: HTTP Error {status}: {reason}. "
                    f"Retrying ({attempt}/{self.retries}) after {delay:.1f}s..."
                )
                time.sleep(delay)
        if last_exc:
            raise DownloadError(str(last_exc)) from last_exc
        raise DownloadError("unknown request error")

    def _probe_direct_download(self, url: str) -> DirectDownloadProbe:
        head_resp: Optional[requests.Response] = None
        try:
            head_resp = self._request_with_retry("HEAD", url, stream=False)
            total = int(head_resp.headers.get("Content-Length", "0") or 0)
            supports_ranges = head_resp.headers.get("Accept-Ranges", "").strip().lower() == "bytes"
            return DirectDownloadProbe(
                final_url=head_resp.url,
                total_bytes=total,
                supports_ranges=supports_ranges and total > 0,
                output_name=self._infer_output_name(url, head_resp),
            )
        except Exception:
            pass
        finally:
            if head_resp is not None:
                head_resp.close()

        resp: Optional[requests.Response] = None
        try:
            resp = self.session.request(
                "GET",
                url,
                headers={
                    **self.session.headers,
                    "Range": "bytes=0-0",
                    "Accept-Encoding": "identity",
                },
                stream=True,
                timeout=self.timeout,
                allow_redirects=True,
            )
            if resp.status_code >= 500:
                raise DownloadError(f"HTTP Error {resp.status_code}: {resp.reason}")
            total = self._parse_content_range_total(resp.headers.get("Content-Range", ""))
            supports_ranges = resp.status_code == 206 and total > 0
            if not total:
                total = int(resp.headers.get("Content-Length", "0") or 0)
            return DirectDownloadProbe(
                final_url=resp.url,
                total_bytes=total,
                supports_ranges=supports_ranges,
                output_name=self._infer_output_name(url, resp),
            )
        except requests.RequestException:
            return DirectDownloadProbe(final_url=url, output_name=self._infer_output_name(url))
        finally:
            if resp is not None:
                resp.close()

    def _resolve_direct_workers(self, total_bytes: int) -> int:
        if self.workers > 0:
            return max(1, min(self.workers, 16))
        if total_bytes < 32 * 1024 * 1024:
            return 1
        if total_bytes < 128 * 1024 * 1024:
            return 4
        if total_bytes < 512 * 1024 * 1024:
            return 6
        return 8

    def _build_range_jobs(self, total_bytes: int, workers: int) -> List[RangeJob]:
        target_parts = max(workers * 4, workers)
        part_size = math.ceil(total_bytes / target_parts)
        part_size = max(DIRECT_RANGE_PART_SIZE_MIN, part_size)
        part_size = min(DIRECT_RANGE_PART_SIZE_MAX, part_size)

        jobs: List[RangeJob] = []
        start = 0
        index = 0
        while start < total_bytes:
            end = min(start + part_size - 1, total_bytes - 1)
            jobs.append(RangeJob(index=index, start=start, end=end))
            start = end + 1
            index += 1
        return jobs

    def _download_range_part(
        self,
        url: str,
        job: RangeJob,
        temp_dir: Path,
        stats: DownloadStats,
    ) -> int:
        part_path = temp_dir / f"{job.index:06d}.part"
        if part_path.exists() and part_path.stat().st_size > job.expected_size:
            self._cleanup_file(part_path)

        last_exc: Optional[Exception] = None
        max_attempts = max(1, min(self.retries, DIRECT_RANGE_RETRIES))

        for attempt in range(1, max_attempts + 1):
            existing = part_path.stat().st_size if part_path.exists() else 0
            if existing >= job.expected_size:
                return job.expected_size

            resp: Optional[requests.Response] = None
            range_start = job.start + existing
            try:
                resp = self.session.request(
                    "GET",
                    url,
                    headers={
                        **self.session.headers,
                        "Range": f"bytes={range_start}-{job.end}",
                        "Accept-Encoding": "identity",
                    },
                    stream=True,
                    timeout=self.timeout,
                    allow_redirects=True,
                )
                if resp.status_code in FRAGMENT_SKIP_STATUS_CODES:
                    raise DownloadError(f"HTTP Error {resp.status_code}: {resp.reason}")
                if resp.status_code >= 500:
                    delay = self._retry_delay(attempt, response=resp, fragment=True)
                    resp.close()
                    if attempt < max_attempts:
                        time.sleep(delay)
                        continue
                    raise DownloadError(f"HTTP Error {resp.status_code}: {resp.reason}")
                if resp.status_code != 206:
                    raise DownloadError(
                        f"Server stopped honoring range requests (status {resp.status_code})"
                    )

                with part_path.open("ab" if existing else "wb") as fh:
                    for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                        if not chunk:
                            continue
                        fh.write(chunk)
                        self._update_progress(stats, byte_delta=len(chunk))

                final_size = part_path.stat().st_size
                if final_size != job.expected_size:
                    if attempt < max_attempts:
                        time.sleep(self._retry_delay(attempt, fragment=True))
                        continue
                    raise DownloadError(
                        f"Incomplete range part {job.index + 1}: got {final_size} of {job.expected_size} bytes"
                    )
                return final_size
            except (requests.Timeout, requests.ConnectionError, requests.HTTPError, DownloadError) as exc:
                last_exc = exc
                if attempt < max_attempts:
                    time.sleep(self._retry_delay(attempt, fragment=True))
                    continue
            finally:
                if resp is not None:
                    resp.close()

        if last_exc:
            raise DownloadError(f"Failed to download range {job.index + 1}: {last_exc}") from last_exc
        raise DownloadError(f"Failed to download range {job.index + 1}")

    def _download_http_file_parallel(
        self,
        url: str,
        output: Path,
        total_bytes: int,
        workers: int,
        stats: DownloadStats,
    ) -> None:
        temp_dir = output.with_suffix(output.suffix + ".parts")
        temp_dir.mkdir(parents=True, exist_ok=True)
        jobs = self._build_range_jobs(total_bytes, workers)
        stats.total_fragments = len(jobs)

        pending: Deque[RangeJob] = deque()
        for job in jobs:
            part_path = temp_dir / f"{job.index:06d}.part"
            if not part_path.exists():
                pending.append(job)
                continue
            size = part_path.stat().st_size
            if size > job.expected_size:
                self._cleanup_file(part_path)
                pending.append(job)
                continue
            stats.done_bytes += size
            if size == job.expected_size:
                stats.done_fragments += 1
            else:
                pending.append(job)

        if stats.done_bytes > 0:
            self.printer.message(
                f"{self._download_tag()} Resuming {output.name} with "
                f"{self.stdout_colors.notice(str(stats.done_fragments))} completed parts "
                f"and {self.stdout_colors.notice(self.printer._format_bytes(stats.done_bytes))} already downloaded"
            )
        self._write_resume_state(
            output,
            {
                "kind": "direct-range",
                "url": url,
                "output": str(output),
                "total_bytes": total_bytes,
                "completed_parts": stats.done_fragments,
                "done_bytes": stats.done_bytes,
            },
        )
        self._refresh_progress(stats)
        actual_workers = min(max(1, workers), max(len(jobs), 1))
        self.printer.message(
            f"{self._download_tag()} Using native parallel downloader with "
            f"{self.stdout_colors.notice(str(actual_workers))} connections"
        )

        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=actual_workers) as executor:
                future_map: Dict[concurrent.futures.Future[int], RangeJob] = {}
                while pending or future_map:
                    while pending and len(future_map) < actual_workers:
                        job = pending.popleft()
                        future = executor.submit(self._download_range_part, url, job, temp_dir, stats)
                        future_map[future] = job

                    if not future_map:
                        continue

                    done, _ = concurrent.futures.wait(
                        future_map,
                        return_when=concurrent.futures.FIRST_COMPLETED,
                    )
                    for future in done:
                        job = future_map.pop(future)
                        try:
                            future.result()
                        except Exception as exc:
                            raise DownloadError(
                                f"Failed to download {output.name} range {job.index + 1}: {exc}"
                            ) from exc
                        self._update_progress(stats, fragment_delta=1)

            with output.open("wb") as out_fh:
                for job in jobs:
                    part_path = temp_dir / f"{job.index:06d}.part"
                    if not part_path.exists():
                        raise DownloadError(f"Missing range part {job.index + 1}")
                    with part_path.open("rb") as in_fh:
                        while True:
                            chunk = in_fh.read(CHUNK_SIZE)
                            if not chunk:
                                break
                            out_fh.write(chunk)
        finally:
            self._cleanup_parts(temp_dir)

    def _download_http_file_single(
        self,
        url: str,
        output: Path,
        total_bytes: int,
        supports_ranges: bool,
        stats: DownloadStats,
    ) -> None:
        part_output = output.with_suffix(output.suffix + ".part")
        existing = part_output.stat().st_size if part_output.exists() else 0
        headers: Optional[Dict[str, str]] = None
        mode = "wb"

        if total_bytes > 0 and existing >= total_bytes:
            self.printer.message(
                f"{self._download_tag()} Found a complete partial file; finalizing {output.name}"
            )
            self._cleanup_file(output)
            part_output.replace(output)
            stats.done_bytes = total_bytes
            stats.total_bytes = total_bytes
            return

        if existing > 0 and supports_ranges and total_bytes > 0:
            headers = {
                "Range": f"bytes={existing}-",
                "Accept-Encoding": "identity",
            }
            mode = "ab"
            stats.done_bytes = existing
            self.printer.message(
                f"{self._download_tag()} Resuming {output.name} from "
                f"{self.stdout_colors.notice(self.printer._format_bytes(existing))}"
            )
        elif existing > 0:
            self.printer.message(
                f"{self._download_tag()} Discarding an old partial file because this server "
                f"does not support resume"
            )
            self._cleanup_file(part_output)
            existing = 0

        self._write_resume_state(
            output,
            {
                "kind": "direct",
                "url": url,
                "output": str(output),
                "total_bytes": total_bytes,
                "done_bytes": existing,
                "supports_ranges": supports_ranges,
            },
        )
        self._refresh_progress(stats)

        resp = self._request_with_retry("GET", url, stream=True, headers=headers)
        try:
            if headers and resp.status_code != 206:
                resp.close()
                self._cleanup_file(part_output)
                stats.done_bytes = 0
                self.printer.message(
                    f"{self._http_tag()} Server ignored the resume request; restarting from the beginning"
                )
                self._write_resume_state(
                    output,
                    {
                        "kind": "direct",
                        "url": url,
                        "output": str(output),
                        "total_bytes": total_bytes,
                        "done_bytes": 0,
                        "supports_ranges": supports_ranges,
                    },
                )
                resp = self._request_with_retry("GET", url, stream=True)
                mode = "wb"

            with part_output.open(mode) as fh:
                for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                    if not chunk:
                        continue
                    fh.write(chunk)
                    stats.done_bytes += len(chunk)
                    if not stats.total_bytes:
                        stats.total_bytes = max(stats.done_bytes, 1)
                    self.printer.progress(stats)
        finally:
            resp.close()

        self._cleanup_file(output)
        part_output.replace(output)

    def _download_http_file(self, url: str) -> Path:
        probe = self._probe_direct_download(url)
        output_name = probe.output_name or self._infer_output_name(probe.final_url)
        output = Path(output_name)
        output.parent.mkdir(parents=True, exist_ok=True)
        stats = DownloadStats(start_time=time.time(), total_bytes=probe.total_bytes)
        self.printer.message(f"{self._download_tag()} Saving to {output}")

        existing_state = self._load_resume_state(output)
        if existing_state:
            self.printer.message(
                f"{self._download_tag()} Found previous state for {output.name}; attempting to resume"
            )

        direct_workers = self._resolve_direct_workers(probe.total_bytes)
        try:
            if probe.supports_ranges and probe.total_bytes >= DIRECT_RANGE_MIN_SIZE and direct_workers > 1:
                try:
                    self._download_http_file_parallel(
                        probe.final_url,
                        output,
                        probe.total_bytes,
                        direct_workers,
                        stats,
                    )
                except DownloadError as exc:
                    self.printer.message(
                        f"{self._http_tag()} Parallel range download failed ({exc}); "
                        f"falling back to one connection"
                    )
                    stats = DownloadStats(start_time=time.time(), total_bytes=probe.total_bytes)
                    self._download_http_file_single(
                        probe.final_url,
                        output,
                        probe.total_bytes,
                        probe.supports_ranges,
                        stats,
                    )
            else:
                if probe.supports_ranges and direct_workers <= 1:
                    self.printer.message(
                        f"{self._download_tag()} Using single connection for this file size"
                    )
                elif not probe.supports_ranges:
                    self.printer.message(
                        f"{self._download_tag()} Server does not support byte ranges; falling back to one connection"
                    )
                self._download_http_file_single(
                    probe.final_url,
                    output,
                    probe.total_bytes,
                    probe.supports_ranges,
                    stats,
                )
        except Exception:
            raise
        else:
            self._delete_resume_state(output)

        if stats.total_bytes < stats.done_bytes:
            stats.total_bytes = stats.done_bytes
        self.printer.progress(stats)
        self.printer.finish()
        self.printer.message(f"{self._download_tag()} Finished: {output}")
        return output

    def _download_hls(self, manifest_url: str) -> Path:
        if not self.referer:
            self._set_runtime_referer(manifest_url)
        self.printer.message(f"{self._hls_tag()} Fetching HLS manifest")
        playlist_text, final_url, audio_playlist_url = self._fetch_playlist_with_variants(manifest_url)
        if audio_playlist_url is None:
            audio_playlist_url = self._infer_streamhls_audio_playlist(final_url)
        output = Path(self._infer_hls_output_name(manifest_url, final_url))
        output.parent.mkdir(parents=True, exist_ok=True)
        merged_output = output.with_name(output.name + ".ts")
        merged_audio_output = output.with_name(output.name + ".audio.ts")
        encoded_video_output = output.with_name(output.name + ".video.mp4")

        base_url = final_url
        segments, estimated_total = self._parse_media_playlist(playlist_text, base_url)
        stats = DownloadStats(
            start_time=time.time(),
            total_bytes=estimated_total,
            total_fragments=len(segments),
        )
        image_stream_kind = self._sniff_hls_segment_mode(segments)
        if audio_playlist_url is None:
            self._warn_if_playlist_appears_video_only(playlist_text, segments)

        temp_dir = output.with_suffix(output.suffix + ".parts")
        had_resume_state = self._load_resume_state(output) is not None
        temp_dir.mkdir(parents=True, exist_ok=True)
        self._write_resume_state(
            output,
            {
                "kind": "hls",
                "url": manifest_url,
                "final_url": final_url,
                "audio_playlist_url": audio_playlist_url,
                "output": str(output),
                "total_fragments": len(segments),
            },
        )
        if had_resume_state:
            self.printer.message(
                f"{self._hls_tag()} Found previous state for {output.name}; attempting to resume"
            )
        self.printer.message(
            f"{self._hls_tag()} Downloading {len(segments)} fragments to {output.name}"
        )
        audio_temp_dir = output.with_suffix(output.suffix + ".audio.parts")
        audio_segments: List[SegmentJob] = []

        try:
            image_stream_kind = self._download_segments(segments, temp_dir, stats) or image_stream_kind
            if audio_playlist_url:
                self.printer.message(
                    f"{self._hls_tag()} Fetching companion audio playlist"
                )
                if self._should_use_browser_transport(audio_playlist_url):
                    audio_playlist_text, _ = self._browser_fetch_text(audio_playlist_url)
                else:
                    audio_resp = self._request_with_retry("GET", audio_playlist_url, stream=False)
                    try:
                        audio_playlist_text = audio_resp.text
                    finally:
                        audio_resp.close()
                audio_segments, _ = self._parse_media_playlist(audio_playlist_text, audio_playlist_url)
                audio_temp_dir.mkdir(parents=True, exist_ok=True)
                audio_stats = DownloadStats(
                    start_time=time.time(),
                    total_fragments=len(audio_segments),
                )
                self.printer.message(
                    f"{self._hls_tag()} Downloading {len(audio_segments)} audio fragments"
                )
                self._download_segments(audio_segments, audio_temp_dir, audio_stats)
                self.printer.message(
                    f"{self._hls_tag()} Merging {len(audio_segments)} audio fragments into {merged_audio_output.name}"
                )
                self._merge_segments(audio_temp_dir, merged_audio_output, audio_stats)
            if image_stream_kind:
                self.printer.message(
                    f"{self._image_hls_tag()} Encoding {len(segments)} image fragments to {output.name}"
                )
                self._encode_image_hls(
                    temp_dir,
                    encoded_video_output if audio_playlist_url else output,
                    segments,
                )
            else:
                self.printer.message(
                    f"{self._hls_tag()} Merging {len(segments)} fragments into {merged_output.name}"
                )
                self._merge_segments(temp_dir, merged_output, stats)
        finally:
            self._cleanup_parts(temp_dir)
            self._cleanup_parts(audio_temp_dir)

        stats.total_bytes = max(stats.done_bytes, stats.total_bytes)
        self.printer.progress(stats)
        self.printer.finish()
        if image_stream_kind:
            if audio_playlist_url and merged_audio_output.exists():
                self._mux_hls_streams(encoded_video_output, merged_audio_output, output)
                self._cleanup_file(encoded_video_output)
                self._cleanup_file(merged_audio_output)
        else:
            if audio_playlist_url and merged_audio_output.exists():
                self._mux_hls_streams(merged_output, merged_audio_output, output)
                self._cleanup_file(merged_output)
                self._cleanup_file(merged_audio_output)
            else:
                self._fixup_m3u8_container(merged_output, output)
                self._cleanup_file(merged_output)
        self._delete_resume_state(output)
        self.printer.message(f"{self._download_tag()} Finished: {output}")
        return output

    def _fetch_playlist_with_variants(self, url: str) -> Tuple[str, str, Optional[str]]:
        visited = set()
        current = url
        audio_playlist_url: Optional[str] = None
        for _ in range(10):
            if current in visited:
                raise DownloadError("Playlist loop detected")
            visited.add(current)

            if self._should_use_browser_transport(current):
                text, _ = self._browser_fetch_text(current)
            else:
                resp = self._request_with_retry("GET", current, stream=False)
                try:
                    text = resp.text
                finally:
                    resp.close()
            if "#EXT-X-STREAM-INF" not in text:
                return text, current, audio_playlist_url

            audio_groups = self._parse_audio_renditions(text, current)
            variants = self._parse_variant_playlist(text, current)
            best = self._pick_hls_variant(variants)
            audio_group = self._find_variant_audio_group(text, current, best[2])
            audio_playlist_url = (
                (audio_groups.get(audio_group) or {}).get("url")
                if audio_group
                else None
            )
            self.printer.message(
                f"{self._hls_tag()} Selected variant {self.stdout_colors.notice(self._format_variant_label(best))}"
            )
            current = best[2]
        raise DownloadError("Too many nested playlists")

    def _parse_variant_playlist(self, text: str, base_url: str) -> List[Tuple[int, Optional[int], str]]:
        variants: List[Tuple[int, Optional[int], str]] = []
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        for idx, line in enumerate(lines):
            if not line.startswith("#EXT-X-STREAM-INF"):
                continue
            bandwidth_match = re.search(r"BANDWIDTH=(\d+)", line)
            resolution_match = re.search(r"RESOLUTION=(\d+)x(\d+)", line)
            bandwidth = int(bandwidth_match.group(1)) if bandwidth_match else 0
            height = int(resolution_match.group(2)) if resolution_match else None
            if idx + 1 >= len(lines):
                continue
            uri = lines[idx + 1]
            if uri.startswith("#"):
                continue
            variant_url = self._unwrap_proxy_media_url(urljoin(base_url, uri))
            variants.append((bandwidth, height, variant_url))
        return variants

    def _parse_audio_renditions(self, text: str, base_url: str) -> Dict[str, Dict[str, str]]:
        audio_by_group: Dict[str, Dict[str, str]] = {}
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        for line in lines:
            if not line.upper().startswith("#EXT-X-MEDIA"):
                continue
            if "TYPE=AUDIO" not in line.upper():
                continue
            group_match = re.search(r'GROUP-ID="([^"]+)"', line)
            uri_match = re.search(r'URI="([^"]+)"', line)
            if not group_match or not uri_match:
                continue
            group_id = group_match.group(1)
            is_default = "yes" if "DEFAULT=YES" in line.upper() else "no"
            if group_id in audio_by_group and is_default != "yes":
                continue
            name_match = re.search(r'NAME="([^"]+)"', line)
            language_match = re.search(r'LANGUAGE="([^"]+)"', line)
            audio_by_group[group_id] = {
                "url": urljoin(base_url, uri_match.group(1)),
                "name": name_match.group(1) if name_match else "",
                "language": language_match.group(1) if language_match else "",
                "is_default": is_default,
            }
        return audio_by_group

    def _find_variant_audio_group(
        self,
        text: str,
        base_url: str,
        variant_url: str,
    ) -> Optional[str]:
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        for idx, line in enumerate(lines):
            if not line.startswith("#EXT-X-STREAM-INF") or idx + 1 >= len(lines):
                continue
            uri = lines[idx + 1]
            if uri.startswith("#"):
                continue
            if urljoin(base_url, uri) != variant_url:
                continue
            audio_match = re.search(r'AUDIO="([^"]+)"', line)
            return audio_match.group(1) if audio_match else None
        return None

    def _infer_streamhls_audio_playlist(self, manifest_url: str) -> Optional[str]:
        parsed = urlparse(manifest_url)
        match = re.match(r"^/filesr2/([^/]+)/\d+/index$", parsed.path)
        if not match:
            return None
        audio_url = parsed._replace(path=f"/filesr2/{match.group(1)}/audio/audio_0").geturl()
        try:
            resp = self._request_with_retry("GET", audio_url, stream=False)
            try:
                text = resp.text
            finally:
                resp.close()
        except DownloadError:
            return None
        return audio_url if text.lstrip().upper().startswith("#EXTM3U") else None

    def _parse_media_playlist(self, text: str, base_url: str) -> Tuple[List[SegmentJob], int]:
        segments: List[SegmentJob] = []
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        current_extinf = 0.0
        current_byte_range: Optional[Tuple[int, int]] = None
        estimated_total = 0
        last_range_end = 0

        for line in lines:
            if line.startswith("#EXT-X-KEY"):
                method_match = re.search(r"METHOD=([^,]+)", line)
                keyformat_match = re.search(r'KEYFORMAT="([^"]+)"', line)
                method = (method_match.group(1).strip().upper() if method_match else "")
                keyformat = (keyformat_match.group(1).strip().lower() if keyformat_match else "")
                if method and method != "NONE":
                    if keyformat in {"", "identity"}:
                        raise DownloadError("Encrypted HLS is not supported")
                    raise DownloadError("DRM-protected HLS is not supported")
                continue

            if line.startswith("#EXT-X-MAP"):
                uri_match = re.search(r'URI="([^"]+)"', line)
                range_match = re.search(r'BYTERANGE="(\d+)(?:@(\d+))?"', line)
                if not uri_match:
                    continue
                init_range = None
                estimated_bytes = 0
                if range_match:
                    length = int(range_match.group(1))
                    start = int(range_match.group(2) or 0)
                    init_range = (start, start + length - 1)
                    estimated_bytes = length
                    estimated_total += length
                segments.append(
                    SegmentJob(
                        index=len(segments),
                        url=urljoin(base_url, uri_match.group(1)),
                        byte_range=init_range,
                        is_init=True,
                        estimated_bytes=estimated_bytes,
                    )
                )
                continue
            if line.startswith("#EXTINF"):
                value = line.split(":", 1)[1].split(",", 1)[0].strip()
                try:
                    current_extinf = float(value)
                except ValueError:
                    current_extinf = HLS_TARGET_DURATION_FALLBACK
                continue

            if line.startswith("#EXT-X-BYTERANGE"):
                raw = line.split(":", 1)[1].strip()
                if "@" in raw:
                    length_str, start_str = raw.split("@", 1)
                    start = int(start_str)
                else:
                    length_str = raw
                    start = last_range_end
                length = int(length_str)
                end = start + length - 1
                current_byte_range = (start, end)
                last_range_end = end + 1
                estimated_total += length
                continue

            if line.startswith("#"):
                continue

            seg_url = self._unwrap_proxy_media_url(urljoin(base_url, line))
            estimated_bytes = 0
            if current_byte_range is None:
                estimated_bytes = int(max(current_extinf, HLS_TARGET_DURATION_FALLBACK) * 250000)
            segments.append(
                SegmentJob(
                    index=len(segments),
                    url=seg_url,
                    extinf=current_extinf,
                    byte_range=current_byte_range,
                    estimated_bytes=estimated_bytes or (
                        current_byte_range[1] - current_byte_range[0] + 1
                        if current_byte_range is not None
                        else 0
                    ),
                )
            )
            if current_byte_range is None:
                estimated_total += estimated_bytes
            current_extinf = 0.0
            current_byte_range = None

        if not segments:
            raise DownloadError("No segments found in media playlist")
        return segments, estimated_total

    def _segment_headers(self, seg: SegmentJob) -> Dict[str, str]:
        headers: Dict[str, str] = {}
        if seg.byte_range is not None:
            start, end = seg.byte_range
            headers["Range"] = f"bytes={start}-{end}"
        return headers

    def _download_single_segment(self, seg: SegmentJob, temp_dir: Path) -> SegmentResult:
        part_path = temp_dir / f"{seg.index:06d}.part"
        if part_path.exists():
            return SegmentResult(seg.index, part_path.stat().st_size, seg.estimated_bytes)
        if self._should_use_browser_transport(seg.url):
            if seg.byte_range is not None:
                raise DownloadError("Browser transport does not support HLS byte-range segments")
            max_attempts = max(1, min(self.retries, FRAGMENT_REQUEST_RETRIES))
            last_exc: Optional[Exception] = None
            for attempt in range(1, max_attempts + 1):
                try:
                    body, headers = self._browser_fetch_bytes(seg.url)
                    content_type = self._content_type(headers.get("Content-Type", ""))
                    head = body[:64]
                    part_path.write_bytes(body)
                    image_kind = self._normalize_image_segment(part_path, head, content_type)
                    bytes_written = part_path.stat().st_size
                    return SegmentResult(
                        seg.index,
                        bytes_written,
                        seg.estimated_bytes,
                        image_kind=image_kind,
                    )
                except DownloadError as exc:
                    last_exc = exc
                    if attempt < max_attempts:
                        time.sleep(self._retry_delay(attempt, fragment=True))
                        continue
            return SegmentResult(
                seg.index,
                0,
                seg.estimated_bytes,
                retryable=True,
                error_message=str(last_exc) if last_exc else "browser transport failed",
            )

        merged_headers = dict(self.session.headers)
        merged_headers.update(self._segment_headers(seg))
        last_exc: Optional[Exception] = None
        server_error_retries = 0
        content_type = ""
        max_attempts = max(1, min(self.retries, FRAGMENT_REQUEST_RETRIES))

        for attempt in range(1, max_attempts + 1):
            resp: Optional[requests.Response] = None
            try:
                resp = self.session.request(
                    "GET",
                    seg.url,
                    headers=merged_headers,
                    stream=True,
                    timeout=self.timeout,
                    allow_redirects=True,
                )
                if resp.status_code in FRAGMENT_SKIP_STATUS_CODES and not seg.is_init:
                    resp.close()
                    return SegmentResult(seg.index, 0, seg.estimated_bytes, skipped=True)
                if resp.status_code >= 500:
                    server_error_retries += 1
                    delay = self._retry_delay(attempt, response=resp, fragment=True)
                    resp.close()
                    if attempt < max_attempts:
                        time.sleep(delay)
                        continue
                    return SegmentResult(
                        seg.index,
                        0,
                        seg.estimated_bytes,
                        server_error_retries=server_error_retries,
                        retryable=True,
                        error_message=f"HTTP Error {resp.status_code}: {resp.reason}",
                    )
                resp.raise_for_status()
                content_type = self._content_type(resp.headers.get("Content-Type", ""))

                bytes_written = 0
                head = b""
                with part_path.open("wb") as fh:
                    for chunk in resp.iter_content(chunk_size=CHUNK_SIZE):
                        if not chunk:
                            continue
                        if len(head) < 64:
                            head += chunk[: 64 - len(head)]
                        fh.write(chunk)
                        bytes_written += len(chunk)
                image_kind = self._normalize_image_segment(part_path, head, content_type)
                if image_kind:
                    bytes_written = part_path.stat().st_size
                return SegmentResult(
                    seg.index,
                    bytes_written,
                    seg.estimated_bytes,
                    server_error_retries=server_error_retries,
                    image_kind=image_kind,
                )
            except (requests.Timeout, requests.ConnectionError) as exc:
                last_exc = exc
                self._cleanup_file(part_path)
                if attempt < max_attempts:
                    time.sleep(self._retry_delay(attempt, fragment=True))
                    continue
                return SegmentResult(
                    seg.index,
                    0,
                    seg.estimated_bytes,
                    server_error_retries=server_error_retries,
                    retryable=True,
                    error_message=f"{exc.__class__.__name__}: {exc}",
                )
            except requests.HTTPError as exc:
                last_exc = exc
                self._cleanup_file(part_path)
                status = exc.response.status_code if exc.response is not None else 0
                if status in FRAGMENT_SKIP_STATUS_CODES and not seg.is_init:
                    return SegmentResult(seg.index, 0, seg.estimated_bytes, skipped=True)
                if attempt < max_attempts and status not in {403, 404}:
                    time.sleep(self._retry_delay(attempt, response=exc.response, fragment=True))
                    continue
                reason = exc.response.reason if exc.response is not None else str(exc)
                if status >= 500 or status == 0:
                    return SegmentResult(
                        seg.index,
                        0,
                        seg.estimated_bytes,
                        server_error_retries=max(server_error_retries, 1),
                        retryable=True,
                        error_message=f"HTTP Error {status}: {reason}",
                    )
                raise DownloadError(f"[download] Got error: HTTP Error {status}: {reason}.") from exc
            finally:
                if resp is not None:
                    resp.close()

        if last_exc:
            return SegmentResult(
                seg.index,
                0,
                seg.estimated_bytes,
                server_error_retries=server_error_retries,
                retryable=True,
                error_message=str(last_exc),
            )
        return SegmentResult(
            seg.index,
            0,
            seg.estimated_bytes,
            server_error_retries=server_error_retries,
            retryable=True,
            error_message=f"Failed to download fragment {seg.index + 1}",
        )

    def _download_segments(self, segments: List[SegmentJob], temp_dir: Path, stats: DownloadStats) -> str:
        completed: Dict[int, int] = {}
        requeue_counts: Dict[int, int] = {}
        image_stream_kind = ""
        for seg in segments:
            part_path = temp_dir / f"{seg.index:06d}.part"
            if part_path.exists():
                size = part_path.stat().st_size
                completed[seg.index] = size
                stats.done_fragments += 1
                stats.done_bytes += size
                stats.total_bytes += size - seg.estimated_bytes

        if completed:
            self.printer.message(
                f"{self._hls_tag()} Resuming from "
                f"{self.stdout_colors.notice(str(len(completed)))} existing fragments "
                f"and {self.stdout_colors.notice(self.printer._format_bytes(stats.done_bytes))} already downloaded"
            )
        self.printer.progress(stats)
        pending = [seg for seg in segments if seg.index not in completed]
        if not pending:
            stats.total_bytes = max(stats.done_bytes, stats.total_bytes)
            return image_stream_kind

        effective_workers = self._resolve_hls_workers(pending)
        host_windows = self._build_host_windows(pending, effective_workers=effective_workers)
        hosts = list(host_windows)
        pending_by_host: Dict[str, Deque[SegmentJob]] = {
            host: deque() for host in hosts
        }
        for seg in pending:
            pending_by_host[self._segment_host(seg)].append(seg)

        if len(hosts) > 1:
            per_host_limit = next(iter(host_windows.values())).configured_limit
            self.printer.message(
                f"{self._hls_tag()} Using up to {self.stdout_colors.notice(str(effective_workers))} "
                f"workers across {self.stdout_colors.notice(str(len(hosts)))} hosts "
                f"({self.stdout_colors.notice(str(per_host_limit))} per host)"
            )
        else:
            initial_limit = next(iter(host_windows.values())).active_limit
            self.printer.message(
                f"{self._hls_tag()} Using up to {self.stdout_colors.notice(str(effective_workers))} "
                f"workers on {self.stdout_colors.notice(hosts[0])} "
                f"(starting at {self.stdout_colors.notice(str(initial_limit))})"
            )

        with concurrent.futures.ThreadPoolExecutor(max_workers=effective_workers) as executor:
            future_map: Dict[concurrent.futures.Future[SegmentResult], Tuple[SegmentJob, str]] = {}
            active_by_host: Dict[str, int] = {host: 0 for host in hosts}

            while any(pending_by_host[host] for host in hosts) or future_map:
                scheduled = True
                while len(future_map) < effective_workers and scheduled:
                    scheduled = False
                    for host in hosts:
                        if len(future_map) >= effective_workers:
                            break
                        if not pending_by_host[host]:
                            continue
                        if time.time() < host_windows[host].cooldown_until:
                            continue
                        if active_by_host[host] >= host_windows[host].active_limit:
                            continue

                        seg = pending_by_host[host].popleft()
                        future = executor.submit(self._download_single_segment, seg, temp_dir)
                        future_map[future] = (seg, host)
                        active_by_host[host] += 1
                        scheduled = True

                if not future_map:
                    next_ready = min(
                        (
                            host_windows[host].cooldown_until
                            for host in hosts
                            if pending_by_host[host]
                        ),
                        default=0.0,
                    )
                    delay = max(0.05, min(next_ready - time.time(), 0.5))
                    time.sleep(delay)
                    continue

                done, _ = concurrent.futures.wait(
                    future_map,
                    return_when=concurrent.futures.FIRST_COMPLETED,
                )
                for future in done:
                    seg, host = future_map.pop(future)
                    active_by_host[host] -= 1
                    try:
                        result = future.result()
                    except Exception as exc:
                        raise DownloadError(f"Failed to download fragment {seg.index + 1}: {exc}") from exc

                    if result.image_kind:
                        if not image_stream_kind:
                            image_stream_kind = result.image_kind
                            self.printer.message(
                                f"{self._image_hls_tag()} Detected image-based HLS fragments "
                                f"({self.stdout_colors.notice(image_stream_kind)})"
                            )
                        elif image_stream_kind != result.image_kind:
                            raise DownloadError(
                                "Mixed HLS fragment types detected; cannot build a stable output"
                            )

                    if result.retryable:
                        self._note_fragment_backpressure(
                            host,
                            host_windows[host],
                            result.server_error_retries or 1,
                            timeout_related=result.server_error_retries <= 0,
                        )
                        requeue_count = requeue_counts.get(seg.index, 0) + 1
                        requeue_counts[seg.index] = requeue_count
                        if requeue_count <= FRAGMENT_REQUEUE_LIMIT:
                            pending_by_host[host].append(seg)
                            continue
                        detail = result.error_message or "transient network error"
                        raise DownloadError(
                            f"Failed to download fragment {seg.index + 1} after repeated transient "
                            f"errors: {detail}"
                        )

                    if result.server_error_retries > 0:
                        self._note_fragment_backpressure(
                            host,
                            host_windows[host],
                            result.server_error_retries,
                        )
                    self._note_fragment_success(
                        host,
                        host_windows[host],
                        result.skipped,
                    )

                    if result.skipped:
                        self.printer.message(
                            f"{self._skip_tag()} fragment not found; "
                            f"{self.stdout_colors.warning(f'Skipping fragment {seg.index + 1} ...')}"
                        )
                    stats.done_fragments += 1
                    stats.done_bytes += result.size
                    stats.total_bytes += result.size - result.estimated_bytes
                    stats.total_bytes = max(stats.done_bytes, stats.total_bytes)
                    self.printer.progress(stats)

        stats.total_bytes = max(stats.done_bytes, stats.total_bytes)
        return image_stream_kind

    def _merge_segments(self, temp_dir: Path, output: Path, stats: DownloadStats) -> None:
        part_files = sorted(temp_dir.glob("*.part"))
        if not part_files:
            raise DownloadError("No segment parts were downloaded")

        with output.open("wb") as out_fh:
            for part in part_files:
                with part.open("rb") as in_fh:
                    while True:
                        chunk = in_fh.read(CHUNK_SIZE)
                        if not chunk:
                            break
                        out_fh.write(chunk)

    @staticmethod
    def _find_embedded_ts_payload(data: bytes) -> int:
        candidates: List[int] = []
        if data.startswith(b"\x89PNG\r\n\x1a\n"):
            marker = b"IEND\xaeB`\x82"
            end = data.find(marker)
            if end >= 0:
                candidates.append(end + len(marker))
        if data.startswith(b"\xff\xd8\xff"):
            end = data.rfind(b"\xff\xd9")
            if end >= 0:
                candidates.append(end + 2)
        if data.startswith((b"GIF87a", b"GIF89a")):
            end = data.rfind(b"\x3b")
            if end >= 0:
                candidates.append(end + 1)
        if data.startswith(b"RIFF") and data[8:12] == b"WEBP" and len(data) >= 8:
            candidates.append(min(int.from_bytes(data[4:8], "little") + 8, len(data)))

        for base in candidates:
            search_end = min(len(data), base + 512)
            for pos in range(base, search_end):
                if data[pos] != 0x47:
                    continue
                hits = 1
                for idx in range(1, 4):
                    packet_pos = pos + 188 * idx
                    if packet_pos < len(data) and data[packet_pos] == 0x47:
                        hits += 1
                if hits >= 3:
                    return pos
        return -1

    @classmethod
    def _normalize_image_segment(cls, part_path: Path, head: bytes, content_type: str) -> str:
        image_kind = ""
        cleaned: Optional[bytes] = None
        full = b""

        if head.startswith(b"\x89PNG\r\n\x1a\n"):
            full = part_path.read_bytes()
            marker = b"IEND\xaeB`\x82"
            end = full.find(marker)
            if end >= 0:
                cleaned = full[: end + len(marker)]
                image_kind = "png"
        elif head.startswith(b"\xff\xd8\xff"):
            full = part_path.read_bytes()
            end = full.rfind(b"\xff\xd9")
            if end >= 0:
                cleaned = full[: end + 2]
                image_kind = "jpeg"
        elif head.startswith((b"GIF87a", b"GIF89a")):
            full = part_path.read_bytes()
            end = full.rfind(b"\x3b")
            if end >= 0:
                cleaned = full[: end + 1]
                image_kind = "gif"
        elif head.startswith(b"RIFF") and head[8:12] == b"WEBP":
            full = part_path.read_bytes()
            if len(full) >= 8:
                size = int.from_bytes(full[4:8], "little") + 8
                cleaned = full[:size]
                image_kind = "webp"

        if cleaned is None:
            return ""

        payload_start = cls._find_embedded_ts_payload(full)
        if payload_start >= 0 and payload_start < len(full):
            part_path.write_bytes(full[payload_start:])
            return ""

        if cleaned != full:
            part_path.write_bytes(cleaned)
        return image_kind

    def _encode_image_hls(self, temp_dir: Path, output: Path, segments: List[SegmentJob]) -> None:
        part_files = sorted(temp_dir.glob("*.part"))
        if not part_files:
            raise DownloadError("No image fragments were downloaded")

        ffmpeg_path = shutil.which("ffmpeg")
        if not ffmpeg_path:
            raise DownloadError("ffmpeg is required to encode image-based HLS streams")
        try:
            from PIL import Image
        except ImportError as exc:
            raise DownloadError(
                "Pillow is required to encode image-based HLS streams"
            ) from exc

        first_existing = next(
            (temp_dir / f"{seg.index:06d}.part" for seg in segments if (temp_dir / f"{seg.index:06d}.part").exists()),
            None,
        )
        if first_existing is None:
            raise DownloadError("No image fragments were available to encode")

        with Image.open(first_existing) as first_image:
            width, height = first_image.size

        frame_rate = 10
        command = [
            ffmpeg_path,
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "rawvideo",
            "-pixel_format",
            "rgb24",
            "-video_size",
            f"{width}x{height}",
            "-framerate",
            str(frame_rate),
            "-i",
            "-",
            "-an",
            "-c:v",
            "libx264",
            "-preset",
            "veryfast",
            "-crf",
            "18",
            "-pix_fmt",
            "yuv420p",
            "-movflags",
            "+faststart",
            str(output),
        ]
        process = subprocess.Popen(
            command,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        total_target_frames = 0.0
        frames_written = 0
        try:
            assert process.stdin is not None
            for seg in segments:
                part_path = temp_dir / f"{seg.index:06d}.part"
                if not part_path.exists():
                    continue

                with Image.open(part_path) as image:
                    frame = image.convert("RGB")
                    if frame.size != (width, height):
                        frame = frame.resize((width, height))
                    frame_bytes = frame.tobytes()

                total_target_frames += max(seg.extinf, 0.04) * frame_rate
                repeat = max(1, round(total_target_frames) - frames_written)
                for _ in range(repeat):
                    process.stdin.write(frame_bytes)
                frames_written += repeat

            process.stdin.close()
            _, stderr = process.communicate()
        except Exception:
            if process.stdin is not None and not process.stdin.closed:
                process.stdin.close()
            process.kill()
            process.wait()
            self._cleanup_file(output)
            raise

        if process.returncode != 0:
            self._cleanup_file(output)
            details = (stderr.decode("utf-8", errors="ignore") or "unknown ffmpeg error").strip()
            raise DownloadError(f"Failed to encode image-based HLS stream: {details}")

    def _mux_hls_streams(self, video_path: Path, audio_path: Path, output_path: Path) -> None:
        ffmpeg_path = shutil.which("ffmpeg")
        if not ffmpeg_path:
            raise DownloadError("ffmpeg is required to mux HLS audio and video streams")

        self.printer.message(
            f'{self._fixup_tag()} Muxing companion audio into "{output_path.name}"'
        )

        command = [
            ffmpeg_path,
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            str(video_path),
            "-i",
            str(audio_path),
            "-map",
            "0:v:0",
            "-map",
            "1:a:0",
            "-c",
            "copy",
            "-movflags",
            "+faststart",
            str(output_path),
        ]
        result = subprocess.run(command, capture_output=True, text=True)
        if result.returncode != 0:
            self._cleanup_file(output_path)
            details = (result.stderr or result.stdout or "unknown ffmpeg error").strip()
            raise DownloadError(f"Failed to mux HLS audio/video streams: {details}")

    def _fixup_m3u8_container(self, source_path: Path, output_path: Path) -> None:
        ffmpeg_path = shutil.which("ffmpeg")
        if not ffmpeg_path:
            raise DownloadError("ffmpeg is required to remux HLS downloads into MP4")

        self.printer.message(
            f'{self._fixup_tag()} Fixing MPEG-TS in MP4 container of "{output_path.name}"'
        )

        command = [
            ffmpeg_path,
            "-y",
            "-hide_banner",
            "-loglevel",
            "error",
            "-i",
            str(source_path),
            "-c",
            "copy",
            "-movflags",
            "+faststart",
            str(output_path),
        ]
        result = subprocess.run(command, capture_output=True, text=True)
        if result.returncode != 0:
            self._cleanup_file(output_path)
            details = (result.stderr or result.stdout or "unknown ffmpeg error").strip()
            raise DownloadError(f"Failed to remux HLS download to MP4: {details}")

    def _cleanup_parts(self, temp_dir: Path) -> None:
        if not temp_dir.exists():
            return
        for part in temp_dir.glob("*.part"):
            try:
                part.unlink()
            except OSError:
                pass
        try:
            temp_dir.rmdir()
        except OSError:
            pass

    @staticmethod
    def _cleanup_file(path: Path) -> None:
        try:
            path.unlink()
        except OSError:
            pass
