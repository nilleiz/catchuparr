"""ffmpeg option helper for fixtures that must decode without frame dropping."""
import subprocess
from functools import lru_cache


@lru_cache(maxsize=1)
def passthrough_args():
    """Return ``-fps_mode passthrough``, or ``-vsync 0`` for ffmpeg builds before 5.1.

    ``-vsync`` was removed in ffmpeg 9, so the newer option is preferred.
    """
    try:
        help_text = subprocess.run(
            ["ffmpeg", "-hide_banner", "-h", "full"],
            check=False, capture_output=True, text=True, timeout=30,
        ).stdout
    except (OSError, subprocess.SubprocessError):
        help_text = ""
    return ["-fps_mode", "passthrough"] if "-fps_mode" in help_text else ["-vsync", "0"]
