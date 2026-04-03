import argparse
import os
import sys
from dataclasses import dataclass
from typing import Optional

from .downloader import Downloader
from .errors import DownloadError, UpdateError
from .output import Colorizer, _supports_color
from .updater import apply_self_update, check_for_update, resolve_manifest_url
from .version import __version__


@dataclass
class InteractiveConfig:
    url: str
    output: Optional[str]
    retries: int
    timeout: float
    workers: int
    referer: Optional[str]
    quality: str
    list_formats: bool
    episode_source_url: Optional[str] = None
    update_manifest: Optional[str] = None


MENU_DOWNLOAD = "1"
MENU_LIST_FORMATS = "2"
MENU_ADVANCED = "3"
MENU_UPDATE = "4"
MENU_EXIT = "5"
ALT_SCREEN_ENTER = "\033[?1049h\033[2J\033[H"
ALT_SCREEN_EXIT = "\033[?1049l"
PROFILE_BALANCED = "balanced"
PROFILE_FASTEST = "fastest"
PROFILE_SAFE = "safe"
PROFILE_CUSTOM = "custom"


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "CLI video downloader for direct media URLs and HLS manifests using the native downloader."
        )
    )
    parser.add_argument(
        "--version",
        action="store_true",
        help="Show vd-dl version and exit",
    )
    subparsers = parser.add_subparsers(dest="command", required=False)

    download_parser = subparsers.add_parser(
        "download",
        help="Download from a direct media URL or HLS manifest",
    )
    download_parser.add_argument("url", help="Video URL")
    download_parser.add_argument("-o", "--output", help="Output filename")
    download_parser.add_argument("--retries", type=int, default=10, help="Retry count (default: 10)")
    download_parser.add_argument("--timeout", type=float, default=45.0, help="Request timeout in seconds")
    download_parser.add_argument(
        "--workers",
        type=int,
        default=0,
        help="Parallel workers for native HLS fragments and range downloads (default: auto)",
    )
    download_parser.add_argument(
        "--quality",
        default="best",
        help="HLS quality selector: best, worst, or a height like 720",
    )
    download_parser.add_argument(
        "--list-formats",
        action="store_true",
        help="List available HLS variants and exit",
    )
    download_parser.add_argument("--referer", help="Optional Referer header")

    subparsers.add_parser(
        "interactive",
        help="Start interactive mode",
    )

    check_update_parser = subparsers.add_parser(
        "check-update",
        help="Check for a newer vd-dl version",
    )
    check_update_parser.add_argument(
        "--timeout",
        type=float,
        default=20.0,
        help="HTTP timeout for update checks in seconds",
    )

    self_update_parser = subparsers.add_parser(
        "self-update",
        help="Download and apply the latest version",
    )
    self_update_parser.add_argument(
        "--timeout",
        type=float,
        default=120.0,
        help="HTTP timeout for update download in seconds",
    )
    self_update_parser.add_argument(
        "-y",
        "--yes",
        action="store_true",
        help="Apply update without confirmation",
    )
    return parser


def _prompt(text: str, default: Optional[str] = None) -> str:
    suffix = f" [{default}]" if default not in (None, "") else ""
    try:
        value = input(f"{text}{suffix}: ").strip()
    except EOFError:
        return default or ""
    if value:
        return value
    return default or ""


def _prompt_int(text: str, default: int, minimum: int = 0) -> int:
    while True:
        raw = _prompt(text, str(default))
        try:
            value = int(raw)
            if value < minimum:
                raise ValueError
            return value
        except ValueError:
            print(f"Please enter an integer >= {minimum}.")


def _prompt_float(text: str, default: float, minimum: float = 0.0) -> float:
    while True:
        raw = _prompt(text, str(default))
        try:
            value = float(raw)
            if value < minimum:
                raise ValueError
            return value
        except ValueError:
            print(f"Please enter a number >= {minimum}.")


def _prompt_yes_no(text: str, default: bool = False) -> bool:
    default_label = "Y/n" if default else "y/N"
    while True:
        try:
            raw = input(f"{text} [{default_label}]: ").strip().lower()
        except EOFError:
            return default
        if not raw:
            return default
        if raw in {"y", "yes"}:
            return True
        if raw in {"n", "no"}:
            return False
        print("Please answer y or n.")


def _clear_screen() -> None:
    if not hasattr(sys.stdout, "isatty") or not sys.stdout.isatty():
        return
    os.system("cls" if os.name == "nt" else "clear")


def _use_alternate_screen() -> bool:
    return hasattr(sys.stdout, "isatty") and sys.stdout.isatty() and _supports_color(sys.stdout)


def _enter_alternate_screen() -> bool:
    if not _use_alternate_screen():
        return False
    sys.stdout.write(ALT_SCREEN_ENTER)
    sys.stdout.flush()
    return True


def _exit_alternate_screen(enabled: bool) -> None:
    if not enabled:
        return
    sys.stdout.write(ALT_SCREEN_EXIT)
    sys.stdout.flush()


def _pause() -> None:
    if hasattr(sys.stdin, "isatty") and sys.stdin.isatty():
        input("Press Enter to return to the main menu...")


def _detect_profile(session_config: InteractiveConfig) -> str:
    if (
        session_config.retries == 10
        and abs(session_config.timeout - 45.0) < 0.001
        and session_config.workers == 0
    ):
        return PROFILE_BALANCED
    if (
        session_config.retries == 6
        and abs(session_config.timeout - 30.0) < 0.001
        and session_config.workers == 12
    ):
        return PROFILE_FASTEST
    if (
        session_config.retries == 15
        and abs(session_config.timeout - 60.0) < 0.001
        and session_config.workers == 2
    ):
        return PROFILE_SAFE
    return PROFILE_CUSTOM


def _apply_profile(session_config: InteractiveConfig, profile: str) -> InteractiveConfig:
    retries = session_config.retries
    timeout = session_config.timeout
    workers = session_config.workers
    if profile == PROFILE_BALANCED:
        retries, timeout, workers = 10, 45.0, 0
    elif profile == PROFILE_FASTEST:
        retries, timeout, workers = 6, 30.0, 12
    elif profile == PROFILE_SAFE:
        retries, timeout, workers = 15, 60.0, 2

    return InteractiveConfig(
        url="",
        output=None,
        retries=retries,
        timeout=timeout,
        workers=workers,
        referer=session_config.referer,
        quality=session_config.quality,
        list_formats=False,
        update_manifest=session_config.update_manifest,
    )


def _print_header(session_config: InteractiveConfig) -> None:
    workers_text = "auto" if session_config.workers == 0 else str(session_config.workers)
    referer_text = session_config.referer or "none"
    profile = _detect_profile(session_config)
    print(f"vd-dl {__version__}")
    print("Simple video downloader")
    print()
    print(f"Profile: {profile} | quality {session_config.quality} | workers {workers_text}")
    print(f"Referer: {referer_text}")
    print()


def _prompt_menu_choice(text: str, valid_choices: set[str], default: Optional[str] = None) -> str:
    while True:
        choice = _prompt(text, default).strip()
        if choice in valid_choices:
            return choice
        valid_text = ", ".join(sorted(valid_choices))
        print(f"Please choose {valid_text}")


def _resolve_quality_prompt(default_quality: str) -> str:
    print()
    print("Choose quality")
    print("1. Best")
    print("2. Worst")
    print("3. Enter manually")
    default_choice = "1"
    if default_quality == "worst":
        default_choice = "2"
    elif default_quality not in {"best", "worst"}:
        default_choice = "3"
    choice = _prompt_menu_choice("Select", {"1", "2", "3"}, default_choice)
    if choice == "1":
        return "best"
    if choice == "2":
        return "worst"
    custom_default = default_quality if default_quality not in {"best", "worst"} else "720"
    custom_quality = _prompt("Enter quality like 720", custom_default)
    return custom_quality or "best"


def _choose_quality_from_format_options(
    session_config: InteractiveConfig,
    url: str,
) -> str:
    inspector = Downloader(
        output=None,
        retries=session_config.retries,
        timeout=session_config.timeout,
        workers=session_config.workers,
        referer=session_config.referer,
        quality=session_config.quality,
        list_formats=False,
    )
    try:
        options = inspector.get_format_options(url)
    except Exception:
        options = []
    finally:
        inspector.close()

    if not options:
        return _resolve_quality_prompt(session_config.quality)

    print()
    print("Available formats")
    for option in options:
        height_text = f"{option.height}p" if option.height else "direct"
        audio_text = f" | audio: {option.audio_label}" if option.audio_label else ""
        print(f"{option.index}. {height_text}{audio_text}")
    manual_option = str(len(options) + 1)
    print(f"{manual_option}. Enter manually")

    choice = _prompt_menu_choice(
        "Choose quality",
        {str(option.index) for option in options} | {manual_option},
        "1",
    )
    if choice == manual_option:
        return _resolve_quality_prompt(session_config.quality)

    selected = next(option for option in options if str(option.index) == choice)
    return selected.quality_value


def _choose_episode_from_options(
    session_config: InteractiveConfig,
    url: str,
) -> tuple[str, Optional[str], bool]:
    inspector = Downloader(
        output=None,
        retries=session_config.retries,
        timeout=session_config.timeout,
        workers=session_config.workers,
        referer=session_config.referer,
        quality=session_config.quality,
        list_formats=False,
    )
    try:
        episodes = inspector.get_episode_options(url)
    except Exception:
        episodes = []
    finally:
        inspector.close()

    if len(episodes) <= 1:
        return url, None, False

    print()
    print("Available episodes")
    for episode in episodes:
        print(f"{episode.index}. {episode.title}")
    choice = _prompt_menu_choice(
        "Choose episode",
        {str(episode.index) for episode in episodes},
        "1",
    )
    selected = next(episode for episode in episodes if str(episode.index) == choice)
    print(f"Selected episode: {selected.title}")
    return selected.url, selected.title, True


def _collect_job_config(
    session_config: InteractiveConfig,
    *,
    list_formats: bool,
) -> Optional[InteractiveConfig]:
    title = "List available formats" if list_formats else "Download video"
    print(title)
    print()
    print("Step 1/3")
    source_url = _prompt("Paste a video or webpage URL")
    if not source_url:
        return None
    url, episode_title, has_episode_list = _choose_episode_from_options(session_config, source_url)

    output = None
    quality = session_config.quality
    if list_formats:
        print()
        print("The program will analyze the URL and show the available formats.")
    else:
        print()
        print("Step 2/3")
        quality = _choose_quality_from_format_options(session_config, url)
        print()
        print("Step 3/3")
        output_value = _prompt("Output filename (leave blank for automatic naming)", "")
        output = output_value or None
        print()
        print("Ready to download")
        print(f"- URL: {url}")
        if episode_title:
            print(f"- Episode: {episode_title}")
        print(f"- Quality: {quality}")
        print(f"- Filename: {output or 'automatic'}")
        print()
        if not _prompt_yes_no("Start now", default=True):
            return None

    return InteractiveConfig(
        url=url,
        output=output,
        retries=session_config.retries,
        timeout=session_config.timeout,
        workers=session_config.workers,
        referer=session_config.referer,
        quality=quality or "best",
        list_formats=list_formats,
        episode_source_url=source_url if has_episode_list and not list_formats else None,
        update_manifest=session_config.update_manifest,
    )


def _edit_advanced_settings(session_config: InteractiveConfig) -> InteractiveConfig:
    print("Advanced settings")
    print()
    print("Choose a profile first. Profiles set speed and retry defaults for you.")
    print()
    print("1. Balanced    Recommended default")
    print("2. Fastest     More aggressive concurrency")
    print("3. Safe        Lower speed, more conservative")
    print("4. Custom      Edit values manually")
    print()
    current_profile = _detect_profile(session_config)
    default_choice = {
        PROFILE_BALANCED: "1",
        PROFILE_FASTEST: "2",
        PROFILE_SAFE: "3",
        PROFILE_CUSTOM: "4",
    }[current_profile]
    profile_choice = _prompt_menu_choice("Choose profile", {"1", "2", "3", "4"}, default_choice)
    if profile_choice == "1":
        session_config = _apply_profile(session_config, PROFILE_BALANCED)
    elif profile_choice == "2":
        session_config = _apply_profile(session_config, PROFILE_FASTEST)
    elif profile_choice == "3":
        session_config = _apply_profile(session_config, PROFILE_SAFE)

    print()
    print("Press Enter to keep the current value.")
    print()
    retries = session_config.retries
    timeout = session_config.timeout
    workers = session_config.workers
    if profile_choice == "4":
        retries = _prompt_int("Retries", session_config.retries, minimum=1)
        timeout = _prompt_float("Timeout seconds", session_config.timeout, minimum=1.0)
        workers = _prompt_int("Workers (0 = auto)", session_config.workers, minimum=0)
    referer_value = _prompt("Referer", session_config.referer or "")
    default_quality = _prompt("Default quality (best, worst, 720)", session_config.quality or "best")

    return InteractiveConfig(
        url="",
        output=None,
        retries=retries,
        timeout=timeout,
        workers=workers,
        referer=referer_value or None,
        quality=default_quality or "best",
        list_formats=False,
        update_manifest=session_config.update_manifest,
    )


def _run_download(
    config: InteractiveConfig,
    stderr_colors: Colorizer,
    *,
    screen_mode: bool = False,
) -> int:
    downloader = Downloader(
        output=config.output,
        retries=config.retries,
        timeout=config.timeout,
        workers=config.workers,
        referer=config.referer,
        quality=config.quality,
        list_formats=config.list_formats,
        screen_mode=screen_mode,
    )
    try:
        downloader.download(config.url)
        return 0
    except KeyboardInterrupt:
        print(f"\n{stderr_colors.error('[download] Interrupted by user')}", file=sys.stderr)
        return 130
    except DownloadError as exc:
        print(f"{stderr_colors.error('[error]')} {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"{stderr_colors.error('[error]')} Unexpected error: {exc}", file=sys.stderr)
        return 1
    finally:
        downloader.close()


def _run_check_update(
    stderr_colors: Colorizer,
    *,
    timeout: float,
) -> int:
    print(f"[update] Checking updates from {resolve_manifest_url(None)}")
    try:
        info, available = check_for_update(
            current_version=__version__,
            manifest_url=None,
            timeout=timeout,
        )
    except UpdateError as exc:
        print(f"{stderr_colors.error('[error]')} {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"{stderr_colors.error('[error]')} Unexpected update error: {exc}", file=sys.stderr)
        return 1

    print(f"Current version: {__version__}")
    print(f"Latest version:  {info.version}")
    print(f"Manifest:        {info.manifest_url}")
    if available:
        print("Status:          update available")
    else:
        print("Status:          up to date")
    if info.notes:
        print()
        print("Release notes:")
        print(info.notes)
    return 0


def _run_self_update(
    stderr_colors: Colorizer,
    *,
    timeout: float,
    assume_yes: bool,
) -> int:
    print(f"[update] Checking updates from {resolve_manifest_url(None)}")
    try:
        info, available = check_for_update(
            current_version=__version__,
            manifest_url=None,
            timeout=min(timeout, 30.0),
        )
    except UpdateError as exc:
        print(f"{stderr_colors.error('[error]')} {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"{stderr_colors.error('[error]')} Unexpected update error: {exc}", file=sys.stderr)
        return 1

    if not available:
        print(f"vd-dl is already up to date ({__version__}).")
        return 0

    print(f"Current version: {__version__}")
    print(f"Latest version:  {info.version}")
    if info.notes:
        print()
        print("Release notes:")
        print(info.notes)
    print()

    if not assume_yes and not _prompt_yes_no("Apply update now", default=True):
        print("Update cancelled.")
        return 0

    try:
        result = apply_self_update(info, timeout=timeout)
    except UpdateError as exc:
        print(f"{stderr_colors.error('[error]')} {exc}", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"{stderr_colors.error('[error]')} Unexpected update error: {exc}", file=sys.stderr)
        return 1

    print(result.message)
    return 0


def _interactive_update_flow(
    session_config: InteractiveConfig,
    stderr_colors: Colorizer,
) -> tuple[InteractiveConfig, bool]:
    print("Check for updates")
    print()
    manifest_url = resolve_manifest_url(None)
    print(f"Manifest: {manifest_url}")
    print()

    try:
        info, available = check_for_update(
            current_version=__version__,
            manifest_url=None,
            timeout=20.0,
        )
    except UpdateError as exc:
        print(f"{stderr_colors.error('[error]')} {exc}")
        return InteractiveConfig(
            url=session_config.url,
            output=session_config.output,
            retries=session_config.retries,
            timeout=session_config.timeout,
            workers=session_config.workers,
            referer=session_config.referer,
            quality=session_config.quality,
            list_formats=session_config.list_formats,
            episode_source_url=session_config.episode_source_url,
            update_manifest=manifest_url,
        ), False

    print(f"Current version: {__version__}")
    print(f"Latest version:  {info.version}")
    if info.notes:
        print()
        print("Release notes:")
        print(info.notes)
    print()

    next_session = InteractiveConfig(
        url=session_config.url,
        output=session_config.output,
        retries=session_config.retries,
        timeout=session_config.timeout,
        workers=session_config.workers,
        referer=session_config.referer,
        quality=session_config.quality,
        list_formats=session_config.list_formats,
        episode_source_url=session_config.episode_source_url,
        update_manifest=manifest_url,
    )

    if not available:
        print("You are already on the latest version.")
        return next_session, False

    if not _prompt_yes_no("Update now", default=True):
        print("Update skipped.")
        return next_session, False

    try:
        result = apply_self_update(info, timeout=120.0)
    except UpdateError as exc:
        print(f"{stderr_colors.error('[error]')} {exc}")
        return next_session, False

    print(result.message)
    return next_session, result.restart_required


def _auto_check_updates_on_startup(stderr_colors: Colorizer) -> bool:
    if not hasattr(sys.stdin, "isatty") or not sys.stdin.isatty():
        return False

    print(f"[update] Auto check from {resolve_manifest_url(None)}")
    try:
        info, available = check_for_update(
            current_version=__version__,
            manifest_url=None,
            timeout=8.0,
        )
    except Exception:
        return False

    if not available:
        return False

    print(f"[update] New version available: {info.version} (current {__version__})")
    if not _prompt_yes_no("Update now", default=True):
        return False

    try:
        result = apply_self_update(info, timeout=120.0)
    except UpdateError as exc:
        print(f"{stderr_colors.error('[error]')} {exc}")
        return False
    except Exception as exc:
        print(f"{stderr_colors.error('[error]')} Unexpected update error: {exc}")
        return False

    print(result.message)
    return True


def _download_more_episodes_if_requested(
    session_config: InteractiveConfig,
    config: InteractiveConfig,
    stderr_colors: Colorizer,
) -> int:
    if config.list_formats or not config.episode_source_url:
        return 0

    while True:
        print()
        if not _prompt_yes_no("Download another episode from this series", default=False):
            return 0

        try:
            next_url, next_title, has_episode_list = _choose_episode_from_options(
                session_config,
                config.episode_source_url,
            )
        except KeyboardInterrupt:
            print()
            return 130

        try:
            next_output_value = _prompt(
                "Output filename for this episode (leave blank for automatic naming)",
                "",
            )
        except KeyboardInterrupt:
            print()
            return 130

        next_config = InteractiveConfig(
            url=next_url,
            output=next_output_value or None,
            retries=config.retries,
            timeout=config.timeout,
            workers=config.workers,
            referer=config.referer,
            quality=config.quality,
            list_formats=False,
            episode_source_url=config.episode_source_url if has_episode_list else None,
            update_manifest=config.update_manifest,
        )
        if next_title:
            print(f"Starting download: {next_title}")
        result = _run_download(next_config, stderr_colors, screen_mode=True)
        if result != 0:
            return result


def interactive_main(stderr_colors: Colorizer) -> int:
    if _auto_check_updates_on_startup(stderr_colors):
        return 0

    last_exit = 0
    alternate_screen = _enter_alternate_screen()
    session_config = InteractiveConfig(
        url="",
        output=None,
        retries=10,
        timeout=45.0,
        workers=0,
        referer=None,
        quality="best",
        list_formats=False,
        update_manifest=resolve_manifest_url(None),
    )
    try:
        try:
            while True:
                try:
                    _clear_screen()
                    _print_header(session_config)
                    print("1. Download video        Analyze a URL and start downloading")
                    print("2. List available formats Inspect resolutions before downloading")
                    print("3. Download settings     Speed profile, retries, referer, default quality")
                    print("4. Check updates         Check and install new versions")
                    print("5. Exit                  Close vd-dl")
                    print()
                    choice = _prompt_menu_choice(
                        "Choose an option",
                        {MENU_DOWNLOAD, MENU_LIST_FORMATS, MENU_ADVANCED, MENU_UPDATE, MENU_EXIT},
                        MENU_DOWNLOAD,
                    )
                except KeyboardInterrupt:
                    print()
                    return 130

                if choice == MENU_EXIT:
                    return last_exit
                if choice == MENU_UPDATE:
                    _clear_screen()
                    _print_header(session_config)
                    session_config, should_exit_now = _interactive_update_flow(
                        session_config,
                        stderr_colors,
                    )
                    print()
                    if should_exit_now:
                        return 0
                    _pause()
                    continue
                if choice == MENU_ADVANCED:
                    _clear_screen()
                    _print_header(session_config)
                    session_config = _edit_advanced_settings(session_config)
                    print()
                    print("Advanced settings saved.")
                    _pause()
                    continue

                try:
                    _clear_screen()
                    _print_header(session_config)
                    config = _collect_job_config(
                        session_config,
                        list_formats=choice == MENU_LIST_FORMATS,
                    )
                except KeyboardInterrupt:
                    print()
                    return 130

                if config is None:
                    continue

                print()
                last_exit = _run_download(config, stderr_colors, screen_mode=True)
                if choice == MENU_DOWNLOAD and last_exit == 0:
                    followup_exit = _download_more_episodes_if_requested(
                        session_config,
                        config,
                        stderr_colors,
                    )
                    if followup_exit != 0:
                        last_exit = followup_exit
                print()
                _pause()
        except KeyboardInterrupt:
            print()
            return 130
    finally:
        _exit_alternate_screen(alternate_screen)


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    stderr_colors = Colorizer(_supports_color(sys.stderr))

    if args.version:
        print(f"vd-dl {__version__}")
        return 0

    if (len(sys.argv) == 1 and sys.stdin.isatty()) or args.command == "interactive":
        return interactive_main(stderr_colors)

    if args.command == "download":
        return _run_download(
            InteractiveConfig(
                url=args.url,
                output=args.output,
                retries=args.retries,
                timeout=args.timeout,
                workers=args.workers,
                referer=args.referer,
                quality=args.quality,
                list_formats=args.list_formats,
            ),
            stderr_colors,
        )
    if args.command == "check-update":
        return _run_check_update(
            stderr_colors,
            timeout=args.timeout,
        )
    if args.command == "self-update":
        return _run_self_update(
            stderr_colors,
            timeout=args.timeout,
            assume_yes=args.yes,
        )

    parser.print_help()
    return 1
