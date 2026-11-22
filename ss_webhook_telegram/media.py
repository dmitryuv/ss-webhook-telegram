"""ffmpeg helpers: one decode pass produces the clip and the photo frames.

The service decodes the H.265 recording itself, because Surveillance Station
stops server-side H.265 processing in DSM 7.2.2. Since the clip needs a full
decode of the fragment anyway, the photo frames are cut from the same decode
(``split`` + a ``trim``/``fps`` chain), so photos cost no extra decode - they
only pick exact moments of the recording.

Decoding runs on the GPU (VAAPI) when the host exposes one (``/dev/dri`` and a
driver that works); otherwise, or when a hardware pass fails, it falls back to
software decoding. Both drivers are tried at startup - iHD for modern kernels,
i965 for the older Synology kernels - and the chosen path is logged once.
"""
from __future__ import annotations

import logging
import os
import shutil
import subprocess

DEFAULT_VIDEO_MAX_HEIGHT = 480
DEFAULT_VIDEO_PRESET = "veryfast"
DEFAULT_PHOTO_MAX_SIDE = 1280
DEFAULT_PHOTO_QUALITY = 2
X264_PRESETS = {"ultrafast", "superfast", "veryfast", "faster", "fast",
                "medium", "slow", "slower", "veryslow", "placebo"}
FFMPEG_TIMEOUT_SECS = 180
PROBE_TIMEOUT_SECS = 30
STDERR_TAIL_LINES = 20
HWACCEL_DEVICE = "/dev/dri/renderD128"
HWACCEL_PROBE_TIMEOUT_SECS = 20
HWDOWNLOAD_PREFIX = "hwdownload,format=nv12,"
HWACCEL_MODES = {"auto", "vaapi", "none"}
# Tried in order. libva would also fall back on its own, but probing both makes
# the choice explicit and lets the startup log name the driver actually used.
VAAPI_DRIVERS = ("iHD", "i965")

# None = software (also the state before configure_hwaccel is called).
_hwaccel = None
_vaapi_driver = None


class MediaError(Exception):
    """ffmpeg is missing, timed out or returned a non-zero code."""


def check_ffmpeg():
    path = shutil.which("ffmpeg")
    if not path:
        raise MediaError("ffmpeg not found in PATH")
    return path


def vaapi_driver_name():
    """The VAAPI driver in use ("iHD"/"i965"), or None in software mode."""
    return _vaapi_driver


def _probe_vaapi_driver(device, driver):
    """Can ffmpeg open `device` through the VAAPI driver `driver`?"""
    env = dict(os.environ)
    env["LIBVA_DRIVER_NAME"] = driver
    args = ["ffmpeg", "-hide_banner", "-loglevel", "error",
            "-init_hw_device", "vaapi=hw:" + device,
            "-f", "lavfi", "-i", "color=black:s=64x64",
            "-frames:v", "1", "-f", "null", "-"]
    try:
        result = subprocess.run(args, capture_output=True, text=True,
                                timeout=HWACCEL_PROBE_TIMEOUT_SECS, env=env)
    except (subprocess.TimeoutExpired, OSError) as e:
        logging.debug("VAAPI probe (%s) failed: %s", driver, e)
        return False
    if result.returncode == 0:
        return True
    tail = (result.stderr or "").strip().splitlines()[-2:]
    # Debug only: an unavailable driver is normal, the startup log names the
    # one that was picked.
    logging.debug("VAAPI probe (%s) failed: %s", driver,
                  " / ".join(tail) or "ffmpeg exited with code %i" % result.returncode)
    return False


def probe_hwaccel(device=HWACCEL_DEVICE):
    """Return the VAAPI driver that works on `device`, or None."""
    if not os.path.exists(device):
        return None
    for driver in VAAPI_DRIVERS:
        if _probe_vaapi_driver(device, driver):
            return driver
    return None


def configure_hwaccel(mode="auto"):
    """Pick the HEVC decode path and remember it. Returns "vaapi" or None.

    auto  - hardware when the device works, otherwise software;
    vaapi - require hardware (MediaError at startup when it is unavailable);
    none  - software only.
    """
    global _hwaccel, _vaapi_driver
    if mode == "none":
        _hwaccel = None
        _vaapi_driver = None
        return None
    found = probe_hwaccel()
    if found is None and mode == "vaapi":
        raise MediaError("ffmpeg_hwaccel=vaapi, but VAAPI is not available "
                         "on %s" % HWACCEL_DEVICE)
    _vaapi_driver = found
    _hwaccel = "vaapi" if found else None
    return _hwaccel


def _hwaccel_input_args(output_format=False):
    """Input options for hardware HEVC decode (empty in software mode)."""
    if _hwaccel != "vaapi":
        return []
    args = ["-hwaccel", "vaapi", "-hwaccel_device", HWACCEL_DEVICE]
    if output_format:
        # Keep frames on the GPU; the filter chain must download them before
        # software filters (see _hwdownload_prefix).
        args += ["-hwaccel_output_format", "vaapi"]
    return args


def _hwdownload_prefix():
    return HWDOWNLOAD_PREFIX if _hwaccel == "vaapi" else ""


def normalize_height(value, default=DEFAULT_VIDEO_MAX_HEIGHT):
    """Keep the height even (yuv420p) and at least 2."""
    try:
        height = int(value)
    except (TypeError, ValueError):
        logging.warning("Invalid max height %r, using %i", value, default)
        height = default
    if height <= 0:
        logging.warning("Invalid max height %r, using %i", value, default)
        height = default
    return max(2, height // 2 * 2)


def normalize_side(value, default=DEFAULT_PHOTO_MAX_SIDE):
    """Clamp a long-side limit to at least 2 pixels."""
    try:
        side = int(value)
    except (TypeError, ValueError):
        logging.warning("Invalid photo_max_side %r, using %i", value, default)
        side = default
    if side <= 0:
        logging.warning("Invalid photo_max_side %r, using %i", value, default)
        side = default
    return max(2, side)


def normalize_quality(value, default=DEFAULT_PHOTO_QUALITY):
    try:
        quality = int(value)
    except (TypeError, ValueError):
        quality = default
    return min(max(quality, 2), 31)


def normalize_preset(value, default=DEFAULT_VIDEO_PRESET):
    if value not in X264_PRESETS:
        logging.warning("Invalid video_preset %r, using %s", value, default)
        return default
    return value


def normalize_interval(value, default=1.5):
    """A positive spacing; an fps filter cannot divide by zero."""
    try:
        interval = float(value)
    except (TypeError, ValueError):
        return default
    if interval <= 0:
        return default
    return interval


def _video_scale(height):
    # Width follows the aspect ratio (-2), height is capped and never upscaled
    # by min(ih,H). The comma inside min() must be escaped for the ffmpeg
    # filter parser, not for a shell (we never use one).
    return "scale=-2:min(ih\\,{}):flags=bilinear".format(height)


def _photo_fit(max_side):
    """Fit the photo inside max_side x max_side, keep AR, never upscale.

    Telegram recompresses sent photos capping the *long* side (~1280), so
    scaling to that same box avoids a second, lossy rescale on their side.
    """
    return ("scale=w=min(iw\\,{0}):h=min(ih\\,{0}):"
            "force_original_aspect_ratio=decrease:force_divisible_by=2:"
            "flags=bilinear").format(max_side)


def _video_branch(height, fps, skip_secs):
    """Clip branch: start `skip_secs` after the event, CFR, 480p, yuv420p."""
    return "trim=start={},setpts=PTS-STARTPTS,fps={},{},format=yuv420p".format(
        skip_secs, fps, _video_scale(height))


def _frames_branch(interval_secs, skip_secs, max_side):
    """Photo branch: first frame at/after `skip_secs`, then every interval.

    Uses ``select`` rather than ``fps``: the ``fps`` filter emits nothing when
    less than one interval of material is left after the seek (a follow-up
    window ends at the event), while this selects the first frame immediately.
    """
    expr = ("gte(t\\,{skip})*isnan(prev_selected_t)"
            "+gte(t-prev_selected_t\\,{interval})").format(
                skip=skip_secs, interval=interval_secs)
    return "select='{}',{}".format(expr, _photo_fit(max_side))


def build_clip_args(input_path, output_path, *, seek_secs, video_skip_secs,
                    video_length_secs, video_max_height, video_fps,
                    video_preset, video_format, frames_pattern=None,
                    burst_skip_secs=0.0, burst_interval_secs=1.5,
                    burst_photos=0, photo_max_side=DEFAULT_PHOTO_MAX_SIDE,
                    photo_quality=DEFAULT_PHOTO_QUALITY):
    """One ffmpeg invocation: the clip, plus (optionally) the photo frames.

    ``seek_secs`` positions the input at the event; the clip and the frames are
    then trimmed relative to it. When ``frames_pattern`` is set, a ``split``
    feeds two filter chains, so the input is decoded only once.
    """
    height = normalize_height(video_max_height)
    preset = normalize_preset(video_preset)
    video_branch = _video_branch(height, video_fps, video_skip_secs)
    want_frames = bool(frames_pattern) and burst_photos and burst_photos > 0

    if want_frames:
        frames_branch = _frames_branch(normalize_interval(burst_interval_secs),
                                       burst_skip_secs,
                                       normalize_side(photo_max_side))
        filter_complex = "[0:v]split=2[v][p];[v]%s[vo];[p]%s[po]" % (
            video_branch, frames_branch)
    else:
        filter_complex = "[0:v]%s[vo]" % video_branch

    args = [
        "ffmpeg", "-loglevel", "warning", "-y",
    ] + _hwaccel_input_args() + [
        "-ss", str(seek_secs),
        "-i", str(input_path),
        "-filter_complex", filter_complex,
        "-map", "[vo]", "-t", str(video_length_secs),
    ]
    if video_format == "gif":
        args += ["-f", "gif", "-loop", "0", str(output_path)]
    else:
        args += ["-c:v", "libx264", "-preset", preset, "-crf", "30",
                 "-pix_fmt", "yuv420p", "-an", "-movflags", "+faststart",
                 str(output_path)]
    if want_frames:
        args += ["-map", "[po]", "-fps_mode", "passthrough",
                 "-frames:v", str(burst_photos),
                 "-q:v", str(normalize_quality(photo_quality)),
                 "-start_number", "0", str(frames_pattern)]
    return args


def build_keyframes_args(input_path, frames_pattern, *, seek_secs, count,
                         photo_max_side=DEFAULT_PHOTO_MAX_SIDE,
                         photo_quality=DEFAULT_PHOTO_QUALITY):
    """Key-frame-only extraction for a follow-up event.

    ``-skip_frame nokey`` decodes only I-frames, so the pass is a few times
    cheaper than a full decode; the frames are the camera key frames, exactly
    like the old live snapshots were. A fragment always begins on a key frame,
    so the first `count` frames need no timestamp selection.
    """
    vf = _hwdownload_prefix() + _photo_fit(normalize_side(photo_max_side))
    return [
        "ffmpeg", "-loglevel", "warning", "-y",
    ] + _hwaccel_input_args(output_format=True) + [
        "-ss", str(seek_secs),
        "-skip_frame", "nokey",
        "-i", str(input_path),
        "-vf", vf,
        "-fps_mode", "passthrough",
        "-frames:v", str(count),
        "-q:v", str(normalize_quality(photo_quality)),
        "-start_number", "0",
        str(frames_pattern),
    ]


def build_photo_resize_args(input_path, output_path, *,
                            photo_max_side=DEFAULT_PHOTO_MAX_SIDE,
                            photo_quality=DEFAULT_PHOTO_QUALITY):
    """Downscale a JPEG snapshot to fit photo_max_side (long side, no upscale)."""
    return [
        "ffmpeg", "-loglevel", "warning", "-y",
        "-i", str(input_path),
        "-vf", _photo_fit(normalize_side(photo_max_side)),
        "-q:v", str(normalize_quality(photo_quality)),
        str(output_path),
    ]


def probe_keyframe_gap(path, *, min_gap=0.5, max_gap=12.0):
    """Largest key-frame interval of a fragment, clamped, or None.

    Scene changes insert extra key frames, so the *largest* gap estimates the
    camera's scheduled GOP (which is what the follow-up window must cover).
    """
    args = ["ffprobe", "-v", "error", "-select_streams", "v",
            "-skip_frame", "nokey", "-show_entries", "frame=pts_time",
            "-of", "csv=p=0", str(path)]
    try:
        result = subprocess.run(args, capture_output=True, text=True,
                                timeout=PROBE_TIMEOUT_SECS)
    except (subprocess.TimeoutExpired, OSError) as e:
        logging.warning("ffprobe key frames failed for %s: %s", path, e)
        return None
    times = []
    for line in (result.stdout or "").splitlines():
        try:
            times.append(float(line.strip().rstrip(",")))
        except ValueError:
            continue
    gaps = [b - a for a, b in zip(times, times[1:]) if b > a]
    if not gaps:
        return None
    gap = max(gaps)
    if gap < min_gap:
        return None
    return min(gap, max_gap)


def _software_args(args):
    """The same command with the hardware-decode options removed."""
    out = []
    i = 0
    while i < len(args):
        arg = args[i]
        if arg in ("-hwaccel", "-hwaccel_device", "-hwaccel_output_format"):
            i += 2
            continue
        if arg in ("-vf", "-filter_complex") and i + 1 < len(args):
            out += [arg, args[i + 1].replace(HWDOWNLOAD_PREFIX, "", 1)]
            i += 2
            continue
        out.append(arg)
        i += 1
    return out


def _disable_hwaccel(stderr):
    """A hardware pass failed: decode in software until the next start."""
    global _hwaccel, _vaapi_driver
    _hwaccel = None
    _vaapi_driver = None
    tail = " / ".join((stderr or "").strip().splitlines()[-3:])
    logging.warning("Hardware decode failed (%s); falling back to software "
                    "decode for the rest of this run", tail or "unknown error")


def _ffmpeg_env():
    """Force libva to the probed driver; None leaves the environment alone."""
    if _hwaccel != "vaapi" or not _vaapi_driver:
        return None
    env = dict(os.environ)
    env["LIBVA_DRIVER_NAME"] = _vaapi_driver
    return env


def _run_ffmpeg(args):
    try:
        return subprocess.run(args, capture_output=True, text=True,
                              timeout=FFMPEG_TIMEOUT_SECS, env=_ffmpeg_env())
    except subprocess.TimeoutExpired:
        raise MediaError("ffmpeg timed out after %i s" % FFMPEG_TIMEOUT_SECS)
    except OSError as e:
        raise MediaError("cannot run ffmpeg: %s" % e)


def convert(args):
    """Run ffmpeg, raising MediaError with the tail of stderr on failure.

    A failed hardware-decode pass is retried once in software (and hardware
    decode stays off for the rest of the run), so one unsupported stream or a
    busy iGPU does not cost the event.
    """
    logging.debug("ffmpeg %s", " ".join(args[1:]))
    result = _run_ffmpeg(args)
    if result.returncode != 0 and _hwaccel == "vaapi":
        _disable_hwaccel(result.stderr)
        args = _software_args(args)
        logging.debug("ffmpeg (software retry) %s", " ".join(args[1:]))
        result = _run_ffmpeg(args)
    if result.returncode != 0:
        tail = "\n".join((result.stderr or "").strip().splitlines()[-STDERR_TAIL_LINES:])
        raise MediaError("ffmpeg exited with code %i\n%s" % (result.returncode, tail))
    if result.stderr:
        logging.debug("ffmpeg stderr: %s", result.stderr.strip())


def probe_duration(path):
    """Duration of a media file in seconds, or None if it cannot be read."""
    args = ["ffprobe", "-v", "error", "-show_entries", "format=duration",
            "-of", "default=nw=1:nk=1", str(path)]
    try:
        result = subprocess.run(args, capture_output=True, text=True,
                                timeout=PROBE_TIMEOUT_SECS)
    except (subprocess.TimeoutExpired, OSError) as e:
        logging.warning("ffprobe failed for %s: %s", path, e)
        return None
    if result.returncode != 0:
        logging.warning("ffprobe exited with code %i for %s",
                        result.returncode, path)
        return None
    try:
        return float(result.stdout.strip())
    except (TypeError, ValueError):
        return None
