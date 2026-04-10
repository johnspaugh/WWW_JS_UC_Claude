import os
import re
import subprocess
from datetime import datetime
from typing import Dict, Any, List

from s3_client import get_client, INTERNAL_BUCKET


class FFmpegError(Exception):
    pass


def download_from_s3(s3_key: str, local_path: str) -> None:
    """Download a file from the internal S3 bucket to a local path."""
    os.makedirs(os.path.dirname(local_path) or ".", exist_ok=True)
    print(f"    [S3] Downloading s3://{INTERNAL_BUCKET}/{s3_key} → {local_path}")
    get_client().download_file(INTERNAL_BUCKET, s3_key, local_path)


def upload_to_s3(local_path: str, s3_key: str) -> str:
    """Upload a local file to the internal S3 bucket. Returns the S3 URI."""
    print(f"    [S3] Uploading {local_path} → s3://{INTERNAL_BUCKET}/{s3_key}")
    get_client().upload_file(local_path, INTERNAL_BUCKET, s3_key)
    return f"s3://{INTERNAL_BUCKET}/{s3_key}"


def build_command(source_path: str, output_path: str, profile: Dict[str, Any]) -> List[str]:
    """Build an FFmpeg command list from a profile dict. Pure function, no side effects."""
    video_bitrate = f"{profile['video_bitrate'] // 1000}k"
    audio_bitrate = f"{profile['audio_bitrate'] // 1000}k"
    scale = f"{profile['width']}:{profile['height']}"

    return [
        "ffmpeg",
        "-i", source_path,
        "-c:v", "libx264",
        "-b:v", video_bitrate,
        "-vf", f"scale={scale}",
        "-c:a", "aac",
        "-b:a", audio_bitrate,
        "-y", output_path,
    ]


def encode(source_path: str, profile: Dict[str, Any], output_dir: str = ".") -> Dict[str, Any]:
    """
    Encode source_path using the given profile dict.

    source_path may be:
      - a local file path  (used directly)
      - an S3 key          (downloaded first, output uploaded after)

    Returns a result dict: {'path', 's3_uri', 'profile', 'size', 'created_at'}

    Raises FFmpegError on non-zero FFmpeg exit.
    """
    is_s3 = not os.path.exists(source_path)
    local_source = source_path

    if is_s3:
        local_source = os.path.join(output_dir, os.path.basename(source_path))
        download_from_s3(source_path, local_source)

    basename = os.path.splitext(os.path.basename(local_source))[0]
    output_filename = f"{basename}_{profile['name']}.mp4"
    output_path = os.path.join(output_dir, output_filename)

    os.makedirs(output_dir, exist_ok=True)

    cmd = build_command(local_source, output_path, profile)
    print(f"    Running: {' '.join(cmd)}")

    process = subprocess.Popen(
        cmd,
        stderr=subprocess.PIPE,
        universal_newlines=True,
    )

    stderr_lines = []
    time_pattern = re.compile(r"time=(\d+:\d+:\d+)")

    for line in process.stderr:
        line = line.rstrip()
        stderr_lines.append(line)
        # Keep a rolling window to limit memory use
        if len(stderr_lines) > 50:
            stderr_lines.pop(0)

        match = time_pattern.search(line)
        if match:
            print(f"    [{profile['name']}] progress: {match.group(1)}", end="\r")

    process.wait()
    print()  # newline after carriage-return progress output

    if process.returncode != 0:
        tail = "\n".join(stderr_lines[-10:])
        raise FFmpegError(
            f"FFmpeg failed for profile '{profile['name']}' (exit {process.returncode}):\n{tail}"
        )

    s3_uri = None
    if is_s3:
        s3_key = f"encoded/{output_filename}"
        s3_uri = upload_to_s3(output_path, s3_key)

    return {
        "path": output_path,
        "s3_uri": s3_uri,
        "profile": profile,
        "size": os.path.getsize(output_path),
        "created_at": datetime.now().isoformat(),
    }
