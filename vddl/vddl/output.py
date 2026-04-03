import math
import os
import sys
import time
from collections import deque
from typing import Deque, Optional, Tuple

from .models import DownloadStats

ANSI_RESET = "\033[0m"
ANSI_BOLD = "\033[1m"
ANSI_BLUE = "\033[34m"
ANSI_BRIGHT_BLUE = "\033[94m"
ANSI_BRIGHT_WHITE = "\033[97m"
ANSI_CYAN = "\033[36m"
ANSI_GREEN = "\033[32m"
ANSI_MAGENTA = "\033[35m"
ANSI_RED = "\033[91m"
ANSI_YELLOW = "\033[93m"


def _enable_windows_ansi(stream: object) -> bool:
    if os.name != "nt":
        return True

    try:
        import ctypes

        kernel32 = ctypes.windll.kernel32
        handle_map = {
            sys.stdout: -11,
            sys.stderr: -12,
        }
        std_handle = kernel32.GetStdHandle(handle_map.get(stream, -11))
        if std_handle in (0, -1):
            return False

        mode = ctypes.c_uint()
        if kernel32.GetConsoleMode(std_handle, ctypes.byref(mode)) == 0:
            return False

        enable_vt = 0x0004
        if mode.value & enable_vt:
            return True
        return kernel32.SetConsoleMode(std_handle, mode.value | enable_vt) != 0
    except Exception:
        return False


def _supports_color(stream: object) -> bool:
    if os.environ.get("NO_COLOR") is not None:
        return False

    force_color = os.environ.get("FORCE_COLOR")
    if force_color and force_color != "0":
        return _enable_windows_ansi(stream)

    if not hasattr(stream, "isatty") or not stream.isatty():
        return False

    return _enable_windows_ansi(stream)


class Colorizer:
    def __init__(self, enabled: bool) -> None:
        self.enabled = enabled

    def wrap(self, text: str, *codes: str) -> str:
        if not self.enabled or not codes:
            return text
        return f"{''.join(codes)}{text}{ANSI_RESET}"

    def tag(self, text: str) -> str:
        return self.wrap(text, ANSI_BRIGHT_WHITE)

    def percent(self, text: str) -> str:
        return self.wrap(text, ANSI_BLUE)

    def speed(self, text: str) -> str:
        return self.wrap(text, ANSI_GREEN)

    def eta(self, text: str) -> str:
        return self.wrap(text, ANSI_YELLOW)

    def fragment(self, text: str) -> str:
        return self.wrap(text, ANSI_CYAN)

    def notice(self, text: str) -> str:
        return self.wrap(text, ANSI_BRIGHT_BLUE)

    def warning(self, text: str) -> str:
        return self.wrap(text, ANSI_MAGENTA)

    def error(self, text: str) -> str:
        return self.wrap(text, ANSI_BOLD, ANSI_RED)


class ProgressPrinter:
    def __init__(self, colors: Optional[Colorizer] = None, *, screen_mode: bool = False) -> None:
        self.colors = colors or Colorizer(False)
        self.screen_mode = screen_mode
        self._last_line_length = 0
        self._history: Deque[Tuple[float, int]] = deque(maxlen=100)
        self._messages: Deque[str] = deque(maxlen=12)
        self._status_line = ""

    @staticmethod
    def _format_bar(percent: float, width: int = 18) -> str:
        clamped = max(0.0, min(percent, 100.0))
        filled = int(round((clamped / 100.0) * width))
        filled = max(0, min(filled, width))
        return "[" + ("=" * filled) + ("-" * (width - filled)) + "]"

    @staticmethod
    def _format_bytes(num: float) -> str:
        if num <= 0:
            return "0.00B"
        units = ["B", "KiB", "MiB", "GiB", "TiB"]
        idx = min(int(math.log(num, 1024)), len(units) - 1)
        value = num / (1024 ** idx)
        return f"{value:6.2f}{units[idx]}"

    @staticmethod
    def _format_eta(seconds: float) -> str:
        if seconds < 0 or math.isinf(seconds) or math.isnan(seconds):
            return "--:--"
        minutes, sec = divmod(int(seconds), 60)
        hours, minutes = divmod(minutes, 60)
        if hours > 0:
            return f"{hours:02d}:{minutes:02d}:{sec:02d}"
        return f"{minutes:02d}:{sec:02d}"

    def _calc_speed(self, now: float, done_bytes: int) -> float:
        self._history.append((now, done_bytes))
        if len(self._history) < 2:
            return 0.0
        t0, b0 = self._history[0]
        t1, b1 = self._history[-1]
        dt = max(t1 - t0, 1e-6)
        return max((b1 - b0) / dt, 0.0)

    def progress(self, stats: DownloadStats) -> None:
        now = time.time()
        speed = self._calc_speed(now, stats.done_bytes)
        elapsed = max(now - stats.start_time, 0.0)
        pct = 0.0
        eta = float("inf")
        if stats.total_bytes > 0:
            pct = min((stats.done_bytes / stats.total_bytes) * 100.0, 100.0)
            if speed > 0:
                eta = max((stats.total_bytes - stats.done_bytes) / speed, 0.0)

        total_text = self._format_bytes(stats.total_bytes) if stats.total_bytes else "unknown"
        done_text = self._format_bytes(stats.done_bytes)
        speed_text = self._format_bytes(speed) + "/s"
        elapsed_text = self._format_eta(elapsed)
        bar_text = self._format_bar(pct) if stats.total_bytes > 0 else "[..................]"
        frag_text = ""
        frag_text_colored = ""
        if stats.total_fragments > 0:
            frag_text = f" (frag {stats.done_fragments}/{stats.total_fragments})"
            frag_text_colored = " " + self.colors.fragment(
                f"(frag {stats.done_fragments}/{stats.total_fragments})"
            )

        plain_line = (
            f"[download] {bar_text} {pct:5.1f}% {done_text:>10}/{total_text:>10} "
            f"{speed_text:>11} ETA {self._format_eta(eta)} Elapsed {elapsed_text}{frag_text}"
        )
        line = (
            f"{self.colors.tag('[download]')} {self.colors.notice(bar_text)} "
            f"{self.colors.percent(f'{pct:5.1f}%')} "
            f"{done_text:>10}/{total_text:>10} "
            f"{self.colors.speed(f'{speed_text:>11}')} "
            f"ETA {self.colors.eta(self._format_eta(eta))} "
            f"Elapsed {elapsed_text}{frag_text_colored}"
        )
        if self.screen_mode:
            self._status_line = line
            self._render_screen()
            return
        sys.stdout.write("\r" + line + " " * max(self._last_line_length - len(plain_line), 0))
        sys.stdout.flush()
        self._last_line_length = len(plain_line)

    def message(self, text: str) -> None:
        if self.screen_mode:
            self._messages.append(text)
            self._render_screen()
            return
        sys.stdout.write("\r" + " " * self._last_line_length + "\r")
        sys.stdout.write(text + "\n")
        sys.stdout.flush()
        self._last_line_length = 0

    def finish(self) -> None:
        if self.screen_mode:
            self._render_screen()
            return
        sys.stdout.write("\n")
        sys.stdout.flush()
        self._last_line_length = 0

    def _render_screen(self) -> None:
        lines = [
            self.colors.notice("vd-dl"),
            "Interactive download view",
            "",
        ]
        if self._messages:
            lines.extend(self._messages)
        else:
            lines.append("Preparing download...")
        lines.extend(
            [
                "",
                self._status_line or f"{self.colors.tag('[download]')} Waiting for progress...",
            ]
        )
        sys.stdout.write("\033[H\033[2J" + "\n".join(lines))
        sys.stdout.flush()
