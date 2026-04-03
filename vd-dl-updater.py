#!/usr/bin/env python3

import argparse
import hashlib
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional

import requests


def _format_bytes(num: float) -> str:
    if num <= 0:
        return "0.00B"
    units = ["B", "KiB", "MiB", "GiB", "TiB"]
    idx = 0
    value = float(num)
    while value >= 1024 and idx < len(units) - 1:
        value /= 1024.0
        idx += 1
    return f"{value:6.2f}{units[idx]}"


def _format_eta(seconds: float) -> str:
    if seconds < 0 or seconds == float("inf"):
        return "--:--"
    minutes, sec = divmod(int(seconds), 60)
    hours, minutes = divmod(minutes, 60)
    if hours > 0:
        return f"{hours:02d}:{minutes:02d}:{sec:02d}"
    return f"{minutes:02d}:{sec:02d}"


def _progress_bar(percent: float, width: int = 18) -> str:
    clamped = max(0.0, min(percent, 100.0))
    filled = int(round((clamped / 100.0) * width))
    filled = max(0, min(width, filled))
    return "[" + ("=" * filled) + ("-" * (width - filled)) + "]"


def _build_progress_line(prefix: str, done: int, total: int, elapsed: float) -> str:
    speed = done / max(elapsed, 1e-6)
    if total > 0:
        percent = min((done / total) * 100.0, 100.0)
        eta = (total - done) / speed if speed > 0 else float("inf")
        return (
            f"{prefix} {_progress_bar(percent)} {percent:5.1f}% "
            f"{_format_bytes(done):>10}/{_format_bytes(total):>10} "
            f"{_format_bytes(speed):>9}/s ETA {_format_eta(eta)}"
        )
    return (
        f"{prefix} {_progress_bar(0.0)} {'??.?%':>5} "
        f"{_format_bytes(done):>10}/{'unknown':>10} "
        f"{_format_bytes(speed):>9}/s ETA --:--"
    )


def _download_file(url: str, dest: Path, timeout: float, *, prefix: str = "[updater]") -> None:
    last_len = 0
    with requests.get(url, stream=True, timeout=timeout) as response:
        response.raise_for_status()
        total_size = int(response.headers.get("Content-Length") or 0)
        start_time = time.time()
        last_render = 0.0
        done_bytes = 0
        print(f"{prefix} Downloading {url}")
        with dest.open("wb") as out:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if not chunk:
                    continue
                out.write(chunk)
                done_bytes += len(chunk)
                now = time.time()
                if (now - last_render) >= 0.15:
                    line = _build_progress_line(prefix, done_bytes, total_size, now - start_time)
                    sys.stdout.write("\r" + line + (" " * max(last_len - len(line), 0)))
                    sys.stdout.flush()
                    last_len = len(line)
                    last_render = now
        final_line = _build_progress_line(prefix, done_bytes, total_size, max(time.time() - start_time, 1e-6))
        sys.stdout.write("\r" + final_line + (" " * max(last_len - len(final_line), 0)) + "\n")
        sys.stdout.flush()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest().lower()


def _verify_sha256(path: Path, expected: Optional[str]) -> None:
    if not expected:
        return
    actual = _sha256_file(path)
    if actual != expected.lower():
        raise RuntimeError("Checksum mismatch for downloaded executable.")


def _is_pid_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        result = subprocess.run(
            ["tasklist", "/FI", f"PID eq {pid}"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        output = (result.stdout or "").lower()
        if "no tasks are running" in output:
            return False
        return str(pid) in output
    except Exception:
        try:
            os.kill(pid, 0)
        except OSError:
            return False
        return True


def _wait_for_pid_exit(pid: int, timeout: float = 180.0) -> bool:
    if pid <= 0:
        return True
    deadline = time.time() + max(timeout, 1.0)
    while time.time() < deadline:
        if not _is_pid_running(pid):
            return True
        time.sleep(0.5)
    return not _is_pid_running(pid)


def _replace_target(target: Path, downloaded: Path, backup: Path) -> None:
    moved_to_backup = False
    if backup.exists():
        backup.unlink()
    if target.exists():
        os.replace(str(target), str(backup))
        moved_to_backup = True

    try:
        os.replace(str(downloaded), str(target))
    except Exception:
        if moved_to_backup and backup.exists() and not target.exists():
            os.replace(str(backup), str(target))
        raise


def _schedule_self_cleanup(
    updater_path: Path,
    downloaded: Path,
    backup: Path,
    *,
    delete_backup: bool,
) -> None:
    script_path = Path(tempfile.gettempdir()) / f"vddl_updater_cleanup_{os.getpid()}.cmd"
    lines = [
        "@echo off",
        "setlocal",
        "timeout /t 2 /nobreak >nul",
        f'if exist "{downloaded}" del /f /q "{downloaded}" >nul 2>nul',
    ]
    if delete_backup:
        lines.append(f'if exist "{backup}" del /f /q "{backup}" >nul 2>nul')
    lines.extend(
        [
            f'if exist "{updater_path}" del /f /q "{updater_path}" >nul 2>nul',
            'del /f /q "%~f0" >nul 2>nul',
        ]
    )
    script_path.write_text("\n".join(lines), encoding="utf-8")
    flags = getattr(subprocess, "CREATE_NO_WINDOW", 0) | getattr(subprocess, "DETACHED_PROCESS", 0)
    subprocess.Popen(["cmd.exe", "/c", str(script_path)], creationflags=flags, close_fds=True)


def _launch_target(target: Path) -> None:
    create_console = getattr(subprocess, "CREATE_NEW_CONSOLE", 0)
    subprocess.Popen([str(target)], cwd=str(target.parent), creationflags=create_console, close_fds=True)


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="vd-dl external updater")
    parser.add_argument("--target", required=True, help="Path to vd-dl executable to replace")
    parser.add_argument("--source-url", required=True, help="URL of the new vd-dl executable")
    parser.add_argument("--expected-sha256", default="", help="Expected SHA256 of the new executable")
    parser.add_argument("--wait-pid", type=int, default=0, help="PID to wait for before replacing")
    parser.add_argument("--timeout", type=float, default=120.0, help="Download timeout in seconds")
    parser.add_argument("--version", default="", help="Version label for log output")
    parser.add_argument("--keep-backup", action="store_true", help="Keep .bak file after success")
    parser.add_argument("--no-launch", action="store_true", help="Do not launch target after replacing")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    target = Path(args.target).resolve()
    if not target.parent.exists():
        print(f"[updater] Target directory does not exist: {target.parent}")
        return 1

    downloaded = target.with_name(f"{target.stem}.download{target.suffix}")
    backup = target.with_name(f"{target.stem}.bak{target.suffix}")
    updater_path = Path(sys.executable).resolve() if getattr(sys, "frozen", False) else Path(__file__).resolve()

    try:
        if args.wait_pid > 0:
            print(f"[updater] Waiting for process {args.wait_pid} to exit...")
            if not _wait_for_pid_exit(args.wait_pid):
                raise RuntimeError("Timed out waiting for running process to exit.")

        _download_file(args.source_url, downloaded, args.timeout, prefix="[updater]")
        print("[updater] Verifying package integrity...")
        _verify_sha256(downloaded, args.expected_sha256 or None)
        print("[updater] Replacing executable...")
        _replace_target(target, downloaded, backup)

        if not args.no_launch:
            print("[updater] Starting updated application...")
            _launch_target(target)

        _schedule_self_cleanup(
            updater_path,
            downloaded,
            backup,
            delete_backup=not args.keep_backup,
        )
        version_text = f" {args.version}" if args.version else ""
        print(f"[updater] Update{version_text} complete.")
        return 0
    except Exception as exc:
        print(f"[updater] Failed: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
