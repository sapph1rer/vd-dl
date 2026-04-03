import hashlib
import json
import os
import shutil
import subprocess
import sys
import tempfile
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple

import requests

from .errors import UpdateError

UPDATE_MANIFEST_ENV = "VDDL_UPDATE_MANIFEST"


@dataclass
class UpdateInfo:
    version: str
    notes: str
    manifest_url: str
    script_url: Optional[str]
    script_sha256: Optional[str]
    exe_url: Optional[str]
    exe_sha256: Optional[str]


@dataclass
class UpdateApplyResult:
    message: str
    restart_required: bool


def resolve_manifest_url(explicit: Optional[str] = None) -> Optional[str]:
    value = (explicit or "").strip()
    if value:
        return value
    env_value = (os.environ.get(UPDATE_MANIFEST_ENV) or "").strip()
    return env_value or None


def _normalize_version(version: str) -> Tuple[int, ...]:
    clean = (version or "").strip()
    if not clean:
        return (0,)
    parts = []
    for token in clean.replace("-", ".").split("."):
        digits = "".join(ch for ch in token if ch.isdigit())
        parts.append(int(digits or 0))
    while parts and parts[-1] == 0:
        parts.pop()
    return tuple(parts or [0])


def is_newer_version(remote_version: str, current_version: str) -> bool:
    return _normalize_version(remote_version) > _normalize_version(current_version)


def _read_manifest_payload(payload: dict, manifest_url: str) -> UpdateInfo:
    version = str(payload.get("version") or "").strip()
    if not version:
        raise UpdateError("Update manifest is missing 'version'.")
    notes = str(payload.get("notes") or payload.get("changelog") or "").strip()

    script_cfg = payload.get("script") if isinstance(payload.get("script"), dict) else {}
    exe_cfg = payload.get("exe") if isinstance(payload.get("exe"), dict) else {}

    script_url = str(
        payload.get("script_url")
        or script_cfg.get("url")
        or ""
    ).strip() or None
    script_sha256 = str(
        payload.get("script_sha256")
        or script_cfg.get("sha256")
        or ""
    ).strip().lower() or None
    exe_url = str(
        payload.get("exe_url")
        or exe_cfg.get("url")
        or ""
    ).strip() or None
    exe_sha256 = str(
        payload.get("exe_sha256")
        or exe_cfg.get("sha256")
        or ""
    ).strip().lower() or None

    return UpdateInfo(
        version=version,
        notes=notes,
        manifest_url=manifest_url,
        script_url=script_url,
        script_sha256=script_sha256,
        exe_url=exe_url,
        exe_sha256=exe_sha256,
    )


def fetch_update_info(manifest_url: str, timeout: float = 20.0) -> UpdateInfo:
    try:
        response = requests.get(manifest_url, timeout=timeout)
        response.raise_for_status()
    except requests.RequestException as exc:
        raise UpdateError(f"Could not fetch update manifest: {exc}") from exc

    try:
        payload = response.json()
    except json.JSONDecodeError as exc:
        raise UpdateError("Update manifest is not valid JSON.") from exc
    if not isinstance(payload, dict):
        raise UpdateError("Update manifest must be a JSON object.")
    return _read_manifest_payload(payload, manifest_url)


def check_for_update(
    *,
    current_version: str,
    manifest_url: Optional[str] = None,
    timeout: float = 20.0,
) -> Tuple[UpdateInfo, bool]:
    resolved = resolve_manifest_url(manifest_url)
    if not resolved:
        raise UpdateError(
            "Update manifest URL is not configured. "
            "Set VDDL_UPDATE_MANIFEST or pass --manifest-url."
        )
    info = fetch_update_info(resolved, timeout=timeout)
    return info, is_newer_version(info.version, current_version)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest().lower()


def _download_file(url: str, dest: Path, timeout: float) -> None:
    try:
        with requests.get(url, stream=True, timeout=timeout) as response:
            response.raise_for_status()
            with dest.open("wb") as out:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        out.write(chunk)
    except requests.RequestException as exc:
        raise UpdateError(f"Download failed: {exc}") from exc


def _verify_sha256(path: Path, expected: Optional[str]) -> None:
    if not expected:
        return
    actual = _sha256_file(path)
    if actual != expected.lower():
        raise UpdateError("Checksum mismatch while applying update.")


def _pick_extracted_root(extract_dir: Path) -> Path:
    children = list(extract_dir.iterdir())
    if len(children) == 1 and children[0].is_dir():
        return children[0]
    return extract_dir


def _copy_tree(src_root: Path, dst_root: Path) -> None:
    for src in src_root.rglob("*"):
        rel = src.relative_to(src_root)
        dst = dst_root / rel
        if src.is_dir():
            dst.mkdir(parents=True, exist_ok=True)
            continue
        dst.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, dst)


def _apply_script_update(info: UpdateInfo, timeout: float) -> UpdateApplyResult:
    if not info.script_url:
        raise UpdateError("No script update URL in manifest.")
    project_root = Path(__file__).resolve().parent.parent
    with tempfile.TemporaryDirectory(prefix="vddl_script_update_") as tmp_dir:
        tmp_path = Path(tmp_dir)
        archive_path = tmp_path / "update.zip"
        extract_dir = tmp_path / "extract"
        _download_file(info.script_url, archive_path, timeout)
        _verify_sha256(archive_path, info.script_sha256)
        with zipfile.ZipFile(archive_path, "r") as archive:
            archive.extractall(extract_dir)
        root = _pick_extracted_root(extract_dir)
        _copy_tree(root, project_root)
    return UpdateApplyResult(
        message=f"Updated scripts to {info.version}. Please run the command again.",
        restart_required=False,
    )


def _apply_exe_update(info: UpdateInfo, timeout: float) -> UpdateApplyResult:
    if not info.exe_url:
        raise UpdateError("No exe update URL in manifest.")
    current_exe = Path(sys.executable).resolve()
    staged_exe = current_exe.with_name(f"{current_exe.stem}.new{current_exe.suffix}")
    download_target = current_exe.with_name(f"{current_exe.stem}.download{current_exe.suffix}")
    _download_file(info.exe_url, download_target, timeout)
    _verify_sha256(download_target, info.exe_sha256)
    if staged_exe.exists():
        staged_exe.unlink()
    download_target.replace(staged_exe)

    script_body = "\n".join(
        [
            "@echo off",
            "setlocal",
            f'set "TARGET={current_exe}"',
            f'set "STAGED={staged_exe}"',
            f"set PID={os.getpid()}",
            ":wait",
            'tasklist /FI "PID eq %PID%" | find "%PID%" >nul',
            "if not errorlevel 1 (",
            "  timeout /t 1 /nobreak >nul",
            "  goto wait",
            ")",
            'move /Y "%STAGED%" "%TARGET%" >nul',
            'start "" "%TARGET%"',
            'del "%~f0"',
        ]
    )
    script_path = Path(tempfile.gettempdir()) / f"vddl_update_{os.getpid()}.cmd"
    script_path.write_text(script_body, encoding="utf-8")
    create_console = getattr(subprocess, "CREATE_NEW_CONSOLE", 0)
    subprocess.Popen(["cmd.exe", "/c", str(script_path)], creationflags=create_console)
    return UpdateApplyResult(
        message=(
            f"Update {info.version} downloaded. vd-dl will restart with the new version."
        ),
        restart_required=True,
    )


def apply_self_update(info: UpdateInfo, timeout: float = 120.0) -> UpdateApplyResult:
    is_frozen = bool(getattr(sys, "frozen", False))
    if is_frozen:
        return _apply_exe_update(info, timeout)
    return _apply_script_update(info, timeout)
