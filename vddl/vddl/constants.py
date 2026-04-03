DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/145.0.0.0 Safari/537.36"
    ),
    "Accept": "*/*",
    "Accept-Language": "en-US,en;q=0.9",
}

CHUNK_SIZE = 1024 * 1024
MAX_STATUS_ERRORS = 10
MAX_TIMEOUT_ERRORS = 10
HLS_TARGET_DURATION_FALLBACK = 6.0
PROBE_READ_SIZE = 4096
FRAGMENT_SKIP_STATUS_CODES = {404, 410}
FRAGMENT_500_REDUCTION_THRESHOLD = 8
FRAGMENT_WORKER_COOLDOWN_SECONDS = 4.0
FRAGMENT_REQUEUE_LIMIT = 8
FRAGMENT_REQUEST_RETRIES = 2
DIRECT_RANGE_MIN_SIZE = 8 * 1024 * 1024
DIRECT_RANGE_PART_SIZE_MIN = 4 * 1024 * 1024
DIRECT_RANGE_PART_SIZE_MAX = 32 * 1024 * 1024
DIRECT_RANGE_RETRIES = 4
GENERIC_HLS_STEMS = {
    "chunk",
    "default",
    "index",
    "main",
    "manifest",
    "master",
    "media",
    "playlist",
    "stream",
    "video",
}
DIRECT_MEDIA_SUFFIXES = {
    ".aac",
    ".flac",
    ".m4a",
    ".m4v",
    ".mkv",
    ".mov",
    ".mp3",
    ".mp4",
    ".mpeg",
    ".mpg",
    ".ogg",
    ".opus",
    ".ts",
    ".wav",
    ".webm",
}
HLS_CONTENT_TYPES = {
    "application/apple-vnd.mpegurl",
    "application/mpegurl",
    "application/vnd.apple.mpegurl",
    "application/x-mpegurl",
    "audio/mpegurl",
    "audio/x-mpegurl",
}
DASH_CONTENT_TYPES = {
    "application/dash+xml",
}
