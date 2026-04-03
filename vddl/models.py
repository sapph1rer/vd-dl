import threading
from dataclasses import dataclass, field
from typing import Dict, Optional, Tuple


@dataclass
class SegmentJob:
    index: int
    url: str
    extinf: float = 0.0
    byte_range: Optional[Tuple[int, int]] = None
    is_init: bool = False
    estimated_bytes: int = 0


@dataclass
class DownloadStats:
    start_time: float
    total_bytes: int = 0
    done_bytes: int = 0
    total_fragments: int = 0
    done_fragments: int = 0


@dataclass
class SegmentResult:
    index: int
    size: int
    estimated_bytes: int = 0
    skipped: bool = False
    server_error_retries: int = 0
    image_kind: str = ""
    retryable: bool = False
    error_message: str = ""


@dataclass
class ProbeResult:
    kind: str
    final_url: str
    content_type: str = ""
    preview_text: str = ""


@dataclass
class BrowserResource:
    url: str
    status_code: int
    headers: Dict[str, str]
    body: bytes


@dataclass
class DirectDownloadProbe:
    final_url: str
    total_bytes: int = 0
    supports_ranges: bool = False
    output_name: str = ""


@dataclass
class RangeJob:
    index: int
    start: int
    end: int

    @property
    def expected_size(self) -> int:
        return self.end - self.start + 1


@dataclass
class WorkerWindowState:
    configured_limit: int
    active_limit: int
    consecutive_server_errors: int = 0
    success_streak: int = 0
    cooldown_until: float = 0.0
    lock: threading.Lock = field(default_factory=threading.Lock)


@dataclass
class FormatOption:
    index: int
    bandwidth: int
    height: Optional[int]
    url: str
    audio_label: str = ""
    is_direct: bool = False

    @property
    def quality_value(self) -> str:
        if self.height:
            return str(self.height)
        return "best"


@dataclass
class EpisodeOption:
    index: int
    title: str
    url: str
