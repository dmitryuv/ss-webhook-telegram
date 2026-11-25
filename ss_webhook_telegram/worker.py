"""Event processing: dedup, episodes, and recording-derived photos + clip.

Flow for one webhook event:

1. resolve the camera by name (Camera.List cache);
2. dedup on (camera, event-second);
3. the first event of an episode downloads one recording window and a single
   ffmpeg pass turns it into the photo frames and the clip; later events take a
   few follow-up frames at their own moment, never faster than the cooldown;
4. the download and the Telegram sends happen in the camera's own thread; the
   CPU-heavy ffmpeg pass runs on a small shared pool (see ``processing_threads``)
   because decoding H.265 is CPU bound.

Photos are frames of the recording: the burst shares the clip's decode, and a
follow-up frame is one `SnapShot/Take?time=` request for the event second (the
recording key frames are its fallback), so the service needs no extra decode for
photos and does not depend on live snapshots, which stop working for H.265 in
DSM 7.2.2.
"""
from __future__ import annotations

import logging
import queue
import threading
import time
import unicodedata
from concurrent.futures import Future
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import media
from .config import Config
from .store import Episode, EventStore
from .synology import (Event, SurveillanceClient, SynologyApiError, SynologyError,
                       SOURCE_DUAL, SOURCE_MAIN)

# The download starts at the event, not earlier: nothing before it is used (the
# burst and the clip both start at or after the event), and the recording holds
# whatever the camera's own pre-record setting kept. Recording.Download still
# snaps the start back to the previous key frame; that offset is measured as
# `snap` from the fragment duration, so the cut stays exact.
# The recording index (stop_time) lags real time, but Recording.Download serves
# the material as it is written. Wait for the wall-clock moment when the window
# end exists, then verify the downloaded fragment instead of trusting the index.
VIDEO_READY_BUFFER_SECS = 1.0
SNAP_MISS_TOLERANCE_SECS = 0.35
SNAP_MAX_SECS = 30.0
# Surveillance Station cuts every continuous run into `recordTime` chunks
# (1800 s measured on the live host) and Recording.Download never crosses that
# boundary: it returns a fragment truncated at the end of the chunk. A window
# that crosses a seam is therefore stitched from consecutive chunks; a hole
# larger than the tolerance is a real gap (retention, a camera restart) and is
# not retried, because the material will never appear.
SEAM_TOLERANCE_SECS = 0.5
SEGMENT_MIN_SECS = 0.3
MAX_SEGMENTS = 3
# The recording index lags the writer by ~6 s, so the chunk that follows a
# just-closed one shows up a little later; wait for it before cutting short.
SEAM_WAIT_SECS = 2.0
MAX_SEAM_WAITS = 3
# A fragment pays a tail padding (measured 0.92..1.40 s), and the requested
# window may start up to a second before the event.
FRAGMENT_PAD_SECS = 1.0
# Retry budget per material: a clip can wait, a follow-up photo cannot - it is
# already up to EVENT_PHOTO_MAX_AGE_SECS old and holds the camera's thread.
MAX_ATTEMPTS = {"ep": 8, "fu": 3}
RETRY_DEFAULT_SECS = {"ep": 2.0, "fu": 1.0}
# A follow-up photo is one recorded frame taken by exact time
# (SnapShot/Take?time=), so it shows the event itself and not the latest key
# frame, which can be a whole GOP before it. A request for "now" would return
# the live profile instead of a recorded frame, so the requested second must be
# at least this far in the past; and SS answers 400 when the second is not
# covered by a recording yet (the index lags the writer by a few seconds),
# hence the retries with a growing pause.
FOLLOWUP_SNAPSHOT_LAG_SECS = 1.0
FOLLOWUP_SNAPSHOT_RETRIES = 4
FOLLOWUP_SNAPSHOT_RETRY_SECS = 0.5        # multiplied by the attempt number
# A single event can be unlucky, so a camera is switched to recording frames
# only after a run of failed photos, and the run resets on the first frame.
# The switch lives until the process restarts or the camera list is re-read:
# a host that cannot serve a frame by time will never start doing so.
SNAPSHOT_FAIL_STREAK = 5
# 400 with no recording covering the second and the next chunk starting later
# than this is a real hole, not a lagging index.
SNAPSHOT_GAP_SECS = 2.0
# Follow-up photos from the recording (the fallback when the Snapshot API is
# unavailable) are the camera key frames, so the window must cover this many
# key-frame intervals. The real interval is measured from the burst fragment
# and remembered per camera; this is only the fallback until the first episode
# of that camera.
DEFAULT_GOP_SECS = 4.0
# One queue per camera: a burst of another camera must never delay this one.
CAMERA_QUEUE_SIZE = 8
# A safety net for garbage camera names (the webhook may be unauthenticated).
MAX_CAMERA_CHANNELS = 32
# A photo burst for an event older than this is no longer useful; the clip is
# still produced.
EVENT_PHOTO_MAX_AGE_SECS = 60


def _norm(name):
    return unicodedata.normalize("NFC", (name or "").strip())


@dataclass(frozen=True)
class CameraInfo:
    id: int
    name: str
    ds_id: int
    src_type: int


class CameraResolver:
    """Name -> CameraInfo cache, refreshed on a miss."""

    def __init__(self, client: SurveillanceClient):
        self._client = client
        self._lock = threading.Lock()
        self._by_name = {}
        self._fold = {}
        self._generation = 0

    @property
    def generation(self):
        """Bumped every time Camera.List is re-read (settings may have changed)."""
        return self._generation

    def _refresh(self):
        """Read Camera.List and return (by_name, casefolded) maps."""
        by_name = {}
        fold = {}
        for info in self._client.cameras():
            name = _norm(info.get("name"))
            if not name:
                continue
            try:
                cam_id = int(info["id"])
            except (KeyError, TypeError, ValueError):
                continue
            raw_ds = info.get("ownerDsId")
            if raw_ds is None:
                raw_ds = info.get("dsId")
            try:
                ds_id = int(raw_ds or 0)
            except (TypeError, ValueError):
                ds_id = 0
            cam = CameraInfo(cam_id, name, ds_id, 2 if ds_id > 0 else 0)
            by_name[name] = cam
            fold.setdefault(name.casefold(), cam)
        return by_name, fold

    def resolve(self, name):
        key = _norm(name)
        with self._lock:
            cam = self._by_name.get(key) or self._fold.get(key.casefold())
        if cam is not None:
            return cam
        # Camera.List is a network call: do it outside the lock so a miss for
        # one camera does not freeze the others.
        try:
            by_name, fold = self._refresh()
        except SynologyError as e:
            logging.warning("Cannot refresh the camera list: %s", e)
            return None
        with self._lock:
            self._by_name = by_name
            self._fold = fold
            self._generation += 1
        return by_name.get(key) or fold.get(key.casefold())


# -- recording source ------------------------------------------------------

def _source_name(source):
    return "dual" if source == SOURCE_DUAL else "main"


def _stream_ok(width, height, settings):
    """Does the stream keep photo_max_side (no upscale) and the clip height?"""
    if not width or not height:
        return False
    return (max(width, height) >= settings.photo_max_side
            and min(width, height) >= settings.video_max_height)


class SourceResolver:
    """Which Recording.List feeds a camera: main, dual, or the cheaper one.

    `auto` picks the cheapest stream that still covers photo_max_side and
    video_max_height. The low-profile (dual) chunks of a continuous recording
    are written around the clock and are two to four times lighter than the
    main stream, which only holds event recordings and the holes between them.

    The choice is made on the first event of a camera, not at startup: events
    do not come from every camera, so startup must not pay for them.
    """

    def __init__(self, client: SurveillanceClient):
        self._client = client
        self._lock = threading.Lock()
        self._cache = {}

    def peek(self, cam):
        """The source already chosen for the camera, or None."""
        with self._lock:
            entry = self._cache.get(cam.name)
        return entry[0] if entry else None

    def invalidate(self, cam):
        with self._lock:
            self._cache.pop(cam.name, None)

    def source_for(self, cam, settings):
        mode = settings.recording_stream
        if mode == "main":
            return SOURCE_MAIN
        cached = self.peek(cam)
        if cached is not None:
            return cached
        chosen = self._choose(cam, settings)
        with self._lock:
            self._cache[cam.name] = chosen
        return chosen[0]

    def _choose(self, cam, settings):
        """(source, width, height, codec, Mbit/s) plus one log line."""
        mode = settings.recording_stream
        wanted = (SOURCE_MAIN, SOURCE_DUAL) if mode == "auto" else (SOURCE_DUAL,)
        found = []
        for source in wanted:
            try:
                events, _total = self._client.list_recordings(
                    cam.id, cam.src_type, cam.ds_id, source=source, limit=3)
            except SynologyError as e:
                logging.warning("%s: cannot list %s recordings: %s", cam.name,
                                _source_name(source), e)
                continue
            if not events:
                continue
            newest = max(events, key=lambda event: event.start_time)
            mbit = (newest.size_bytes * 8.0 / newest.duration / 1e6
                    if newest.duration > 0 and newest.size_bytes else 0.0)
            found.append((source, newest.width, newest.height, newest.codec, mbit))

        if not found:
            logging.debug("%s: no recordings in the %s list(s)", cam.name,
                          "main/dual" if mode == "auto" else _source_name(wanted[0]))
        else:
            # Built only when DEBUG is on: _stream_ok() would otherwise run for
            # every candidate on every resolve just to be thrown away.
            if logging.getLogger().isEnabledFor(logging.DEBUG):
                logging.debug(
                    "%s: candidates %s", cam.name,
                    ", ".join("%s %sx%s %s %.1f Mbit/s%s" % (
                        _source_name(src), width or "?", height or "?",
                        (codec or "?").lower(), mbit,
                        "" if _stream_ok(width, height, settings) else " (below limits)")
                        for src, width, height, codec, mbit in found))

        if mode == "dual" and not any(entry[0] == SOURCE_DUAL for entry in found):
            logging.warning("%s: no dual recordings; using the main list", cam.name)
            chosen, reason = (SOURCE_MAIN, 0, 0, "", 0.0), "no dual recordings"
        elif not found:
            chosen, reason = (SOURCE_MAIN, 0, 0, "", 0.0), "no recordings yet"
        else:
            fitting = [entry for entry in found
                       if _stream_ok(entry[1], entry[2], settings)]
            chosen = min(fitting or found, key=lambda entry: entry[4])
            reason = "continuous" if chosen[0] == SOURCE_DUAL else "event recordings"
            skipped = [entry for entry in found if entry not in fitting]
            if not fitting:
                logging.warning(
                    "%s: no recording stream covers photo_max_side=%i / "
                    "video_max_height=%i; using the lightest one", cam.name,
                    settings.photo_max_side, settings.video_max_height)
                reason = "below photo_max_side"
            elif skipped:
                # The lighter stream exists but does not meet the limits. A
                # freshly created chunk may simply not have its dimensions in
                # the index yet, which is worth seeing: "?x?" here means the
                # metadata was not ready when the camera was first seen.
                logging.warning(
                    "%s: %s (%sx%s) does not cover photo_max_side=%i / "
                    "video_max_height=%i; using %s", cam.name,
                    _source_name(skipped[0][0]), skipped[0][1] or "?",
                    skipped[0][2] or "?", settings.photo_max_side,
                    settings.video_max_height, _source_name(chosen[0]))
        source, width, height, codec, mbit = chosen
        logging.info("Recording streams (%s): %s %s %sx%s %s %.1f Mbit/s (%s)",
                     mode, cam.name, _source_name(source), width or "?",
                     height or "?", (codec or "?").lower(), mbit, reason)
        return chosen


# -- per-event settings ----------------------------------------------------

@dataclass
class Settings:
    enabled: bool
    episode_gap_secs: float
    cooldown_secs: float
    max_photos_per_episode: int
    burst_photos: int
    burst_interval_secs: float
    burst_skip_first_secs: float
    subsequent_photos: int
    photo_max_side: int
    photo_quality: int
    video_skip_first_n_secs: float
    video_max_length_secs: float
    video_max_height: int
    video_fps: int | str
    video_format: str
    video_preset: str
    use_snapshot_api: str
    recording_stream: str

    def burst_window_end(self):
        """Seconds after the event the last photo frame lives at."""
        if self.burst_photos <= 0:
            return 0.0
        return (self.burst_skip_first_secs
                + (self.burst_photos - 1) * self.burst_interval_secs)

    def video_window_end(self):
        return self.video_skip_first_n_secs + self.video_max_length_secs

    def window_end(self, with_frames=True):
        """How much material after the event the download must cover."""
        if with_frames:
            return max(self.video_window_end(), self.burst_window_end())
        return self.video_window_end()


def _coerce(raw, default, kind, name):
    if raw is None:
        return default
    try:
        if kind is bool:
            if isinstance(raw, bool):
                return raw
            if isinstance(raw, (int, float)):
                return bool(raw)
            return str(raw).strip().lower() in {"1", "true", "yes"}
        if kind is int:
            return int(float(raw))
        if kind is float:
            return float(str(raw).strip())
        if kind is str:
            return str(raw).strip().lower()
    except (TypeError, ValueError):
        logging.warning("payload config %s=%r is invalid, using %r", name, raw, default)
        return default
    return default


def _snapshot_mode(value, default):
    """Normalise use_snapshot_api to one of auto / true / false."""
    if value is None:
        return default
    if isinstance(value, bool):
        return "true" if value else "false"
    text = str(value).strip().lower()
    if text in {"auto", "true", "false"}:
        return text
    if text in {"1", "yes", "on"}:
        return "true"
    if text in {"0", "no", "off"}:
        return "false"
    logging.warning("payload config use_snapshot_api=%r is invalid, using %r",
                    value, default)
    return default


def _stream_mode(value, default):
    """Normalise recording_stream to one of auto / main / dual."""
    if value is None:
        return default
    text = str(value).strip().lower()
    if text in {"auto", "main", "dual"}:
        return text
    logging.warning("payload config recording_stream=%r is invalid, using %r",
                    value, default)
    return default


def _fps_mode(value, default):
    """Normalise video_fps to a positive whole number or "pass".

    "pass" keeps the recording's own timestamps, which is the only way to keep
    a stream with an uneven cadence smooth (a 20 fps grid with every fourth
    frame missing averages 15 fps and CFR 15 makes it stutter).
    """
    if value is None:
        return default
    if isinstance(value, str) and value.strip().lower() == media.FPS_PASSTHROUGH:
        return media.FPS_PASSTHROUGH
    try:
        fps = int(float(value))
    except (TypeError, ValueError):
        fps = 0
    if fps <= 0:
        logging.warning("payload config video_fps=%r is invalid, using %r",
                        value, default)
        return default
    return fps


def build_settings(config: Config, overrides):
    """Merge the global defaults with the per-rule ``config`` object.

    Override keys are exactly the tunable key names (see config-sample.toml);
    an unknown or invalid value is ignored with a warning.
    """
    overrides = overrides if isinstance(overrides, dict) else {}

    def num(key, default):
        return _coerce(overrides.get(key), default, float, key)

    def whole(key, default):
        return _coerce(overrides.get(key), default, int, key)

    fmt = _coerce(overrides.get("video_format"), config.video_format, str, "video_format")
    if fmt not in {"mp4", "gif"}:
        fmt = config.video_format
    fps = _fps_mode(overrides.get("video_fps"), config.video_fps)
    if fmt == "gif" and fps == media.FPS_PASSTHROUGH:
        logging.warning("payload config video_fps=pass is not available for gif, "
                        "using %i fps", media.DEFAULT_VIDEO_FPS)
        fps = media.DEFAULT_VIDEO_FPS

    return Settings(
        use_snapshot_api=_snapshot_mode(overrides.get("use_snapshot_api"),
                                        config.use_snapshot_api),
        recording_stream=_stream_mode(overrides.get("recording_stream"),
                                      config.recording_stream),
        enabled=_coerce(overrides.get("enabled"), True, bool, "enabled"),
        episode_gap_secs=num("episode_gap_secs", config.episode_gap_secs),
        cooldown_secs=num("cooldown_secs", config.cooldown_secs),
        max_photos_per_episode=whole("max_photos_per_episode", config.max_photos_per_episode),
        burst_photos=whole("burst_photos", config.burst_photos),
        burst_interval_secs=_coerce(overrides.get("burst_interval_secs"),
                                    config.burst_interval_secs, float, "burst_interval_secs"),
        burst_skip_first_secs=num("burst_skip_first_secs", config.burst_skip_first_secs),
        subsequent_photos=whole("subsequent_photos", config.subsequent_photos),
        photo_max_side=whole("photo_max_side", config.photo_max_side),
        photo_quality=whole("photo_quality", config.photo_quality),
        video_skip_first_n_secs=num("video_skip_first_n_secs", config.video_skip_first_n_secs),
        video_max_length_secs=num("video_max_length_secs", config.video_max_length_secs),
        video_max_height=whole("video_max_height", config.video_max_height),
        video_fps=fps,
        video_format=fmt,
        video_preset=_coerce(overrides.get("video_preset"), config.video_preset, str, "video_preset"),
    )


def parse_event_time(value, tz: ZoneInfo):
    """%EVENT_TIME% (naive, server local time) -> epoch seconds, or None."""
    if not value:
        return None
    value = str(value).strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
        try:
            return datetime.strptime(value[:19], fmt).replace(tzinfo=tz).timestamp()
        except ValueError:
            continue
    try:
        return float(value)
    except ValueError:
        return None


def _unlink(path):
    if not path:
        return
    try:
        Path(path).unlink(missing_ok=True)
    except OSError as e:
        logging.warning("Cannot remove %s: %s", path, e)


# -- recording window ------------------------------------------------------

@dataclass(frozen=True)
class Segment:
    """A piece of one recording: `take` seconds starting at `offset`."""

    recording: Event
    offset: float
    take: float


@dataclass(frozen=True)
class PlanWait:
    """The material is not there yet; try planning again after `delay`."""

    delay: float
    seam: bool = False


@dataclass(frozen=True)
class PlanGap:
    """A hole in the recording: cut whatever `segments` already cover."""

    segments: tuple
    at: float
    next_start: float | None


class _SnapshotGap(Exception):
    """No recording covers the second, so no frame can ever appear."""


# -- processing pool -------------------------------------------------------

class ProcessingPool:
    """A small pool that serialises the CPU-bound ffmpeg passes.

    Downloads and Telegram sends run in the per-camera threads; only decoding
    and encoding run here, ``processing_threads`` of them at a time (1 by
    default). A full queue blocks the caller, which is the intended
    backpressure: a camera waits, it does not pile up encodes.
    """

    def __init__(self, threads):
        self._threads_count = max(1, int(threads))
        self._queue = queue.Queue(maxsize=max(2, self._threads_count * 2))
        self._stop = threading.Event()
        self._threads = []

    @property
    def threads(self):
        return self._threads_count

    def start(self):
        for index in range(self._threads_count):
            thread = threading.Thread(target=self._run, name="proc-%i" % index,
                                      daemon=True)
            thread.start()
            self._threads.append(thread)

    def submit(self, fn, *args, **kwargs):
        future = Future()
        self._queue.put((future, fn, args, kwargs))
        return future

    def _run(self):
        while not self._stop.is_set():
            try:
                future, fn, args, kwargs = self._queue.get(timeout=1)
            except queue.Empty:
                continue
            try:
                if future.set_running_or_notify_cancel():
                    try:
                        future.set_result(fn(*args, **kwargs))
                    except Exception as e:  # surfaced to the waiting camera thread
                        future.set_exception(e)
            except Exception:
                logging.exception("Processing pool job failed")
            finally:
                self._queue.task_done()

    def stop(self):
        self._stop.set()


# -- event worker ----------------------------------------------------------

class EventWorker(threading.Thread):
    """Dispatcher: one thread per camera, so a burst never blocks another camera.

    The webhook receiver drops payloads into a shared queue; this thread fans
    them out to a per-camera queue served by a dedicated thread. Events of the
    same camera keep their order (episodes, cooldown), events of different
    cameras are processed in parallel. The heavy ffmpeg work is handed to the
    shared :class:`ProcessingPool`.
    """

    def __init__(self, config: Config, client: SurveillanceClient, store: EventStore,
                 notifier, resolver: CameraResolver, pool: ProcessingPool,
                 event_queue, stop_event, dry_run=False):
        super().__init__(name="event-worker", daemon=True)
        self._config = config
        self._client = client
        self._store = store
        self._notifier = notifier
        self._resolver = resolver
        self._sources = SourceResolver(client)
        self._pool = pool
        self._queue = event_queue
        self._stop = stop_event
        self._dry_run = dry_run
        self._tz = ZoneInfo(config.timezone)
        self._workdir = Path(config.ffmpeg_working_folder)
        self._channels = {}
        self._channels_lock = threading.Lock()
        # Cameras whose episode burst is being produced right now (channel keys).
        self._bursts = set()
        # Snapshot bookkeeping, in memory only: a camera may be switched to
        # recording frames after a run of failures, and the switch is dropped
        # when the camera list is re-read or the process restarts.
        self._snapshot_disabled = {}
        self._snapshot_fail_streak = {}
        self._snapshot_generation = {}

    def run(self):
        while not self._stop.is_set():
            try:
                payload = self._queue.get(timeout=1)
            except queue.Empty:
                continue
            name = _norm(payload.get("camera")) or "?"
            self._dispatch(name, payload)

    def _dispatch(self, name, payload):
        key = name.casefold()
        with self._channels_lock:
            busy = key in self._bursts
            channel = self._channels.get(key)
            if channel is None:
                if len(self._channels) >= MAX_CAMERA_CHANNELS:
                    logging.warning("%s: too many cameras (%i channels); dropping "
                                    "the event", name, MAX_CAMERA_CHANNELS)
                    return
                events = queue.Queue(maxsize=CAMERA_QUEUE_SIZE)
                thread = threading.Thread(target=self._drain, args=(name, events),
                                          name="cam-%s" % name[:20], daemon=True)
                channel = (events, thread)
                self._channels[key] = channel
                thread.start()
        if busy:
            # The burst is being produced right now; by the time it finished this
            # event would be within the cooldown anyway, so drop it here instead
            # of parking it in the camera queue. Debug level: it duplicates the
            # `within cooldown` line the event would have produced, and the
            # batch is exactly what an operator sees as silence.
            settings = build_settings(self._config, payload.get("config"))
            if settings.cooldown_secs > 0 or settings.subsequent_photos <= 0:
                logging.debug("%s: burst in flight, dropping the event (%s)",
                              name, payload.get("time"))
                return
        events = channel[0]
        payload["_channel"] = key
        try:
            events.put_nowait(payload)
        except queue.Full:
            logging.warning("%s: camera queue is full (%i events); dropping the "
                            "newest event (%s)", name, CAMERA_QUEUE_SIZE,
                            payload.get("time"))
            return
        depth = events.qsize()
        if depth > 2:
            logging.warning("%s: %i events waiting for this camera", name, depth)

    def _drain(self, name, events):
        while not self._stop.is_set():
            try:
                payload = events.get(timeout=1)
            except queue.Empty:
                continue
            try:
                self.process(payload)
            except Exception:
                logging.exception("Failed to process webhook event; continuing")

    def _stamp(self, t_ev):
        return datetime.fromtimestamp(t_ev, self._tz).strftime("%H:%M:%S")

    def _label(self, cam, t_ev):
        stamp = datetime.fromtimestamp(t_ev, self._tz).strftime("%Y-%m-%d %H:%M:%S")
        return "%s @%s" % (cam.name, stamp)

    def process(self, payload):
        name = payload.get("camera")
        settings = build_settings(self._config, payload.get("config"))
        if not settings.enabled:
            logging.info("Event for %r ignored (enabled=false)", name)
            return
        cam = self._resolver.resolve(name)
        if cam is None:
            logging.warning("Event for unknown camera %r; skipping", name)
            return

        t_ev = parse_event_time(payload.get("time"), self._tz)
        if t_ev is None:
            t_ev = time.time()
            logging.warning("Event for %r has no parsable time, using now", cam.name)

        key = "%s|%i" % (cam.name, int(t_ev))
        if not self._dry_run and not self._store.mark_seen(key):
            logging.info("Event %s already seen; skipping", key)
            return

        delay = time.time() - t_ev
        if delay > 2:
            received = payload.get("_received_at")
            if received:
                logging.info("%s: event %s handled %.1fs late "
                             "(SS delivery %.1fs, our queue %.1fs)",
                             cam.name, self._stamp(t_ev), delay,
                             received - t_ev, time.time() - received)
            else:
                logging.info("%s: event %s handled %.1fs late",
                             cam.name, self._stamp(t_ev), delay)

        chat = self._config.chat(payload.get("chat"))
        self._handle_episode(cam, chat, settings, t_ev, payload.get("_channel"))

    def _handle_episode(self, cam, chat, settings, t_ev, channel=None):
        now = time.time()
        last = self._store.get_episode(cam.name)
        new_episode = last is None or (t_ev - last.last_event) > settings.episode_gap_secs
        stale = now - t_ev > EVENT_PHOTO_MAX_AGE_SECS

        if new_episode:
            self._run_episode(cam, chat, settings, t_ev, stale, channel)
            return

        # subsequent event within the same episode
        last.last_event = t_ev
        if settings.subsequent_photos > 0:
            room = None
            if settings.max_photos_per_episode > 0:
                room = settings.max_photos_per_episode - last.photos_sent
            if room is not None and room <= 0:
                logging.info("Episode %s: photo limit reached, skipping", cam.name)
            elif now - last.last_photo < settings.cooldown_secs:
                logging.info("Episode %s: within cooldown (%.0fs), skipping photo",
                             cam.name, settings.cooldown_secs)
            elif stale:
                logging.info("Episode %s: event %s is %.0fs old, skipping photo",
                             cam.name, self._stamp(t_ev), now - t_ev)
            else:
                count = settings.subsequent_photos
                if room is not None:
                    count = min(count, room)
                sent = self._run_followup(cam, chat, settings, t_ev, count)
                last.photos_sent += sent
                if sent:
                    last.last_photo = time.time()
                logging.info("Episode %s: +%i photo(s) for a follow-up event",
                             cam.name, sent)
        if not self._dry_run:
            self._store.save_episode(last)

    def _set_burst(self, channel, active):
        if channel is None:
            return
        with self._channels_lock:
            if active:
                self._bursts.add(channel)
            else:
                self._bursts.discard(channel)

    def _run_episode(self, cam, chat, settings, t_ev, stale, channel=None):
        """First event of an episode: one download -> frames + clip.

        While this runs the camera is marked busy, so events of the same camera
        are dropped at dispatch instead of waiting in the queue to be skipped by
        the cooldown.
        """
        self._set_burst(channel, True)
        parts = []
        try:
            label = self._label(cam, t_ev)
            # Paired with the `delivered` line below: an episode that never
            # reaches it died in the download, which is otherwise invisible.
            logging.info("New episode: %s @%s", cam.name, self._stamp(t_ev))
            send_frames = settings.burst_photos > 0 and not stale
            if stale and settings.burst_photos > 0:
                logging.warning("%s: event %s is %.0fs old, skipping the photo burst",
                                cam.name, self._stamp(t_ev), time.time() - t_ev)
            window = settings.window_end(with_frames=send_frames)
            fragment = self._download_segments(cam, t_ev, window, "ep", settings)
            if fragment is None:
                return

            parts = [part["path"] for part in fragment["segments"]]
            # The key-frame interval only sizes the *next* follow-up window, so
            # it is measured in the background, in parallel with the clip pass
            # and the sends. The fragments stay on disk until the end of the
            # episode for that probe (and for the clip pass, which may still be
            # waiting in the processing pool). Use the longest piece: at a seam
            # the first one can be a short tail of the previous chunk.
            self._remember_gop_async(
                cam, max(fragment["segments"], key=lambda p: p["keep"])["path"])
            clip_path, frame_paths = self._process_clip(
                cam, settings, fragment, send_frames)
            if self._dry_run:
                logging.info("[dry-run] would send %i photo(s) and a clip for %s",
                             len(frame_paths), label)
                for path in frame_paths:
                    _unlink(path)
                _unlink(clip_path)
                return

            sent_photos = 0
            if frame_paths:
                if self._notifier.send_photos(chat.tele_bot_token, chat.tele_chat_id,
                                              frame_paths, cam.name, cam.name,
                                              wait=True):
                    sent_photos = len(frame_paths)
            self._send_clip(cam, chat, settings, clip_path, label)

            episode = Episode(
                camera=cam.name, episode_start=t_ev, last_event=t_ev,
                photos_sent=sent_photos,
                last_photo=time.time() if sent_photos else 0.0,
            )
            self._store.save_episode(episode)
            logging.info("Episode %s @%s delivered: %i photo(s), clip=%s",
                         cam.name, self._stamp(t_ev), sent_photos,
                         settings.video_format)
        finally:
            self._set_burst(channel, False)
            self._unlink_parts(parts)

    def _run_followup(self, cam, chat, settings, t_ev, count):
        """A later event of an episode: a few photos from its own moment.

        Photos are recorded frames taken by exact time through the Snapshot API
        when `use_snapshot_api` allows it (`true`, or `auto` and the camera has
        not failed a run of events), otherwise the camera key frames of a tiny
        recording window. A camera is switched to the frames only after
        SNAPSHOT_FAIL_STREAK failed events: one unlucky second must not cost
        the exact-time frame for the rest of the run.
        """
        self._check_snapshot_capability(cam)
        mode = settings.use_snapshot_api
        if mode != "false" and (mode == "true"
                                or not self._snapshot_disabled.get(cam.name, False)):
            try:
                return self._followup_snapshots(cam, chat, settings, t_ev, count)
            except _SnapshotGap as e:
                logging.info("%s: follow-up from recording frames (%s)",
                             cam.name, e)
            except (SynologyError, media.MediaError) as e:
                if mode == "true":
                    logging.error("%s: snapshot API failed (%s: %s); "
                                  "use_snapshot_api=true, not falling back",
                                  cam.name, type(e).__name__, e)
                    return 0
                self._snapshot_failed(cam, e)
        return self._followup_frames(cam, chat, settings, t_ev, count)

    def _check_snapshot_capability(self, cam):
        """Drop a latched switch when the camera list was re-read.

        Camera.List is refreshed on a name miss, and its settings (codec,
        streams) may have changed since the switch was made. This is not a
        timer: a host that cannot serve a frame by time is left alone until
        the process restarts or something actually re-reads the camera list.
        """
        generation = getattr(self._resolver, "generation", 0)
        if self._snapshot_generation.get(cam.name) == generation:
            return
        if self._snapshot_disabled.get(cam.name):
            logging.info("%s: camera list re-read; re-evaluating the snapshot API",
                         cam.name)
        self._snapshot_generation[cam.name] = generation
        self._snapshot_disabled.pop(cam.name, None)
        self._snapshot_fail_streak.pop(cam.name, None)

    def _snapshot_failed(self, cam, error):
        """Count one failed photo; switch the camera after a run of them."""
        streak = self._snapshot_fail_streak.get(cam.name, 0) + 1
        self._snapshot_fail_streak[cam.name] = streak
        if streak < SNAPSHOT_FAIL_STREAK:
            logging.warning("%s: snapshot API failed (%s: %s); using recording "
                            "frames", cam.name, type(error).__name__, error)
            return
        self._snapshot_disabled[cam.name] = True
        logging.warning(
            "%s: snapshot API disabled: %i follow-up events in a row without a "
            "frame (last: %s: %s); photos come from the recording until the "
            "camera list is re-read or the process restarts",
            cam.name, streak, type(error).__name__, error)

    def _followup_snapshots(self, cam, chat, settings, t_ev, count):
        """Recorded frames at the event moments (SnapShot/Take?time=).

        Asking for a time makes SS return the frame of the recording at that
        second (from the camera's snapshot stream), so the photo shows the
        event itself - a live snapshot would be the latest key frame, up to a
        whole GOP before it, where the motion may not have started yet.
        """
        tag = self._tag(cam, t_ev, "fu")
        paths = []
        for index in range(count):
            if self._stop.is_set():
                break
            at = t_ev + index * settings.burst_interval_secs
            raw = self._take_snapshot_at(cam, at)
            if raw is None:
                break
            self._snapshot_fail_streak.pop(cam.name, None)
            logging.info("%s: follow-up snapshot @%s (%i KB)", cam.name,
                         self._stamp(at), len(raw) // 1024)
            tmp = self._workdir / ("%s_snap%02d.jpg" % (tag, index))
            out = self._workdir / ("%s_%02d.jpg" % (tag, index))
            try:
                tmp.write_bytes(raw)
                args = media.build_photo_resize_args(
                    str(tmp), str(out), photo_max_side=settings.photo_max_side,
                    photo_quality=settings.photo_quality)
                self._run_ffmpeg(args, cam, t_ev)
                paths.append(str(out))
            except OSError as e:
                logging.warning("%s: cannot store the snapshot: %s", cam.name, e)
                _unlink(out)
            finally:
                _unlink(tmp)
        return self._deliver_followup(cam, chat, paths)

    def _take_snapshot_at(self, cam, at):
        """The recorded frame at `at` (epoch seconds), or None on stop.

        SS answers 400 both when the second is not covered by a recording and
        when the recording index has not caught up with a fresh event (it lags
        the writer by a few seconds), so the two are told apart: a real hole
        raises _SnapshotGap immediately (no frames will ever be there), a
        lagging index is retried with a growing pause. Any other API error is
        raised to the caller, which falls back to the recording.
        """
        ready_at = at + FOLLOWUP_SNAPSHOT_LAG_SECS
        now = time.time()
        if now < ready_at:
            logging.info("%s: waiting %.1fs for the follow-up frame (%s)",
                         cam.name, ready_at - now, self._stamp(ready_at))
            if self._wait(ready_at - now):
                return None
        memo = {}
        for attempt in range(1, FOLLOWUP_SNAPSHOT_RETRIES + 1):
            if self._stop.is_set():
                return None
            try:
                return self._client.take_snapshot(cam.id, at=at)
            except SynologyApiError as e:
                if e.code != 400:
                    raise
                if self._is_snapshot_gap(cam, at, memo):
                    raise _SnapshotGap("no recording covers %s" % self._stamp(at)) from e
                if attempt == FOLLOWUP_SNAPSHOT_RETRIES:
                    raise
                logging.info("%s: snapshot at %s not in a recording yet "
                             "(attempt %i/%i)", cam.name, self._stamp(at),
                             attempt, FOLLOWUP_SNAPSHOT_RETRIES)
                if self._wait(FOLLOWUP_SNAPSHOT_RETRY_SECS * attempt):
                    return None
        return None

    def _is_snapshot_gap(self, cam, at, memo):
        """True when no recording covers `at` and the next chunk is not imminent.

        A lagging index looks the same for a moment, so anything doubtful is
        treated as a lag and retried rather than declared a hole.
        """
        try:
            source = self._sources.peek(cam) or SOURCE_MAIN
            if self._client.find_record_at(cam.id, cam.src_type, cam.ds_id, at,
                                           source=source, memo=memo) is not None:
                return False
            nxt = self._client.find_record_after(cam.id, cam.src_type, cam.ds_id,
                                                 at, source=source, memo=memo)
        except SynologyError as e:
            logging.debug("%s: cannot check the recording index: %s", cam.name, e)
            return False
        if nxt is None:
            return False
        return nxt.start_time - at > SNAPSHOT_GAP_SECS

    def _followup_frames(self, cam, chat, settings, t_ev, count):
        """Camera key frames from a tiny recording window (no clip to share)."""
        gop = self._store.gop(cam.name, DEFAULT_GOP_SECS)
        window = (count - 1) * gop + 1.0
        fragment = self._download_segments(cam, t_ev, window, "fu", settings)
        if fragment is None:
            return 0
        # A photo window is at most a couple of seconds, so the event is in the
        # first chunk even when the window crosses a seam: the key frame at or
        # before the event is all a follow-up needs.
        first = fragment["segments"][0]
        if len(fragment["segments"]) > 1:
            logging.debug("%s: follow-up window crosses a chunk seam; using the "
                          "first chunk", cam.name)
        tag = self._tag(cam, t_ev, "fu")
        pattern = str(self._workdir / (tag + "_%02d.jpg"))
        paths = []
        try:
            # Take the fragment's first key frame(s): seeking to the event would
            # discard the key frame at or before it, and the next one can be a
            # whole GOP past the tiny window. On the key frame <= the event is
            # fine for a follow-up.
            args = media.build_keyframes_args(
                first["path"], pattern, seek_secs=0.0, count=count,
                photo_max_side=settings.photo_max_side,
                photo_quality=settings.photo_quality)
            self._run_ffmpeg(args, cam, t_ev)
            paths = [str(p) for p in sorted(self._workdir.glob(tag + "_*.jpg"))]
        finally:
            self._unlink_parts([part["path"] for part in fragment["segments"]])
        return self._deliver_followup(cam, chat, paths)

    def _deliver_followup(self, cam, chat, paths):
        if not paths:
            logging.warning("%s: no photos for the follow-up event", cam.name)
            return 0
        if self._dry_run:
            logging.info("[dry-run] would send %i follow-up photo(s) for %s",
                         len(paths), cam.name)
            for path in paths:
                _unlink(path)
            return len(paths)
        if not self._notifier.send_photos(chat.tele_bot_token, chat.tele_chat_id,
                                          paths, cam.name, cam.name):
            return 0
        return len(paths)

    def _tag(self, cam, t_ev, kind):
        return "cam%i_%s%i" % (cam.id, kind, int(t_ev))

    def _process_clip(self, cam, settings, fragment, send_frames):
        """Run the single decode pass; return (clip_path, frame_paths)."""
        tag = self._tag(cam, fragment["episode_start"], "ep")
        suffix = ".gif" if settings.video_format == "gif" else "_clip.mp4"
        clip_path = str(self._workdir / (tag + suffix))
        pattern = str(self._workdir / (tag + "_%02d.jpg")) if send_frames else None
        args = media.build_clip_args(
            [(part["path"], part["seek"], part["keep"])
             for part in fragment["segments"]],
            clip_path,
            video_skip_secs=settings.video_skip_first_n_secs,
            video_length_secs=settings.video_max_length_secs,
            video_max_height=settings.video_max_height, video_fps=settings.video_fps,
            video_preset=settings.video_preset, video_format=settings.video_format,
            frames_pattern=pattern, burst_skip_secs=settings.burst_skip_first_secs,
            burst_interval_secs=settings.burst_interval_secs,
            burst_photos=settings.burst_photos,
            photo_max_side=settings.photo_max_side,
            photo_quality=settings.photo_quality)
        self._run_ffmpeg(args, cam, fragment["episode_start"])
        frame_paths = [str(p) for p in sorted(self._workdir.glob(tag + "_*.jpg"))]
        self._log_clip(cam, settings, fragment, frame_paths)
        return clip_path, frame_paths

    def _run_ffmpeg(self, args, cam, t_ev):
        future = self._pool.submit(media.convert, args)
        try:
            future.result()
        except media.MediaError:
            raise
        except Exception as e:  # re-raise with the camera context
            raise media.MediaError("%s: %s" % (type(e).__name__, e)) from e

    def _send_clip(self, cam, chat, settings, clip_path, label):
        if settings.video_format == "gif":
            ok = self._notifier.send_animation(chat.tele_bot_token, chat.tele_chat_id,
                                               clip_path, cam.name, label)
        else:
            ok = self._notifier.send_video(chat.tele_bot_token, chat.tele_chat_id,
                                           clip_path, cam.name, label)
        if not ok:
            logging.error("Clip %s: Telegram rejected the message", label)
            _unlink(clip_path)

    def _log_clip(self, cam, settings, fragment, frame_paths):
        extra = "lead %.0fs, snap %.1fs" % (fragment["lead"], fragment["snap"])
        if fragment["fragment"] is not None:
            extra += ", fragment %.1fs" % fragment["fragment"]
        segments = fragment["segments"]
        if len(segments) > 1:
            extra += ", segments %i (%s, rec %s)" % (
                len(segments),
                " + ".join("%.1fs" % part["keep"] for part in segments),
                " -> ".join(str(rid) for rid in fragment["recording_ids"]))
        if settings.video_fps != getattr(self._config, "video_fps", None):
            # A per-rule override leaves no other trace: the startup log only
            # knows the global default. Log it only when it is not the default,
            # so the line stays short for the usual case.
            extra += ", fps=%s" % (settings.video_fps,)
        extra += ", %i frame(s)" % len(frame_paths)
        logging.info("Clip %s: %s", self._label(cam, fragment["episode_start"]), extra)

    def _remember_gop(self, cam, source):
        """Measure the key-frame interval on the burst fragment and keep it."""
        gap = media.probe_keyframe_gap(source)
        if gap is None:
            return
        if not self._dry_run:
            self._store.set_gop(cam.name, gap)
        logging.info("%s: key-frame interval %.1fs", cam.name, gap)

    def _remember_gop_async(self, cam, source):
        """Run :meth:`_remember_gop` off the delivery path.

        The probe is a few hundred milliseconds of ffprobe; nothing on this
        event needs its result, so it must not sit between the clip and the
        first Telegram message. Errors are logged and swallowed: a missing
        measurement only keeps the previous (or the default) value.
        """
        def run():
            try:
                self._remember_gop(cam, source)
            except Exception:
                logging.exception("%s: key-frame probe failed", cam.name)

        threading.Thread(target=run, name="gop-%s" % cam.id, daemon=True).start()

    def _wait(self, secs):
        """Sleep up to `secs`, honouring shutdown. True when we were stopped."""
        if secs <= 0:
            return self._stop.is_set()
        return self._stop.wait(min(secs, 30.0))

    def plan_window(self, cam, t_ev, window, *, memo=None, allow_seam_wait=True,
                    source=SOURCE_MAIN):
        """Pick the recording chunks that cover [event, event + window].

        One chunk is the normal case. A chunk boundary (the next chunk starts
        exactly where the previous one stopped) is stitched, not skipped:
        Recording.Download stops at the end of a chunk, so an event near a seam
        needs material from both. A hole larger than SEAM_TOLERANCE_SECS is a
        real gap: PlanGap tells the caller to cut what the collected segments
        cover instead of retrying material that will never appear.
        """
        segments = []
        t = t_ev
        remaining = window
        for _ in range(MAX_SEGMENTS):
            recording = self._client.find_record_at(
                cam.id, cam.src_type, cam.ds_id, t, source=source, memo=memo)
            start = None
            if recording is None:
                nxt = self._client.find_record_after(
                    cam.id, cam.src_type, cam.ds_id, t, source=source, memo=memo)
                if nxt is None:
                    # The index has not caught up with a chunk that is being
                    # written right now.
                    return PlanWait(1.0)
                hole = nxt.start_time - t
                if hole > SEAM_TOLERANCE_SECS:
                    return PlanGap(tuple(segments), t, nxt.start_time)
                # A torn seam: a few frames are missing between the chunks.
                recording = nxt
                start = nxt.start_time
                remaining -= max(0.0, hole)
                if remaining <= SEGMENT_MIN_SECS:
                    return segments
            if start is None:
                start = max(t, recording.start_time)
            live = recording.recording or not recording.complete
            take = remaining if live else min(remaining, recording.stop_time - start)
            if take <= SEGMENT_MIN_SECS:
                if live:
                    return PlanWait(1.0)
                if recording.stop_time - start > SEGMENT_MIN_SECS:
                    # `remaining` is already tiny: nothing more to collect.
                    break
                # The event sits at the very end of a closed chunk: the rest of
                # the window is in the next one.
                remaining -= max(0.0, recording.stop_time - t)
                t = recording.stop_time + 0.001
                continue
            segments.append(Segment(recording, start - recording.start_time, take))
            remaining -= take
            t = start + take
            if remaining <= SEGMENT_MIN_SECS:
                return segments
            nxt = self._client.find_record_after(
                cam.id, cam.src_type, cam.ds_id, t, source=source, memo=memo)
            if nxt is None:
                if live:
                    return PlanWait(max(0.5, t + remaining
                                        + VIDEO_READY_BUFFER_SECS - time.time()))
                if allow_seam_wait:
                    return PlanWait(SEAM_WAIT_SECS, seam=True)
                return PlanGap(tuple(segments), t, None)
            if nxt.start_time - t > SEAM_TOLERANCE_SECS:
                return PlanGap(tuple(segments), t, nxt.start_time)
        if remaining > SEGMENT_MIN_SECS:
            logging.warning("%s: window still not covered after %i chunks "
                            "(%.1fs short)", cam.name, MAX_SEGMENTS, remaining)
        return segments

    def _download_segments(self, cam, t_ev, window, kind, settings):
        """Download [event, event + window], stitching chunks at a seam.

        Nothing before the event is used: the burst starts at
        ``event + burst_skip_first_secs`` and the clip at
        ``event + video_skip_first_n_secs``, so the request begins at the
        event. Waits until the material should exist (wall clock), plans the
        chunks, downloads them and verifies every fragment: the API snaps the
        start back to the previous key frame, so the measured duration reveals
        how far before the requested offset the fragment began (``snap``).
        Returns a dict of pieces to cut, or None on stop / a persistent failure.
        """
        ready_at = t_ev + window + VIDEO_READY_BUFFER_SECS
        now = time.time()
        if now < ready_at:
            logging.info("%s: waiting %.1fs for the recording (%s)",
                         cam.name, ready_at - now, self._stamp(ready_at))
            if self._wait(ready_at - now):
                return None

        attempts = MAX_ATTEMPTS[kind]
        retry_secs = RETRY_DEFAULT_SECS[kind]
        source = self._sources.source_for(cam, settings)
        seam_waits = 0
        for attempt in range(1, attempts + 1):
            if self._stop.is_set():
                return None
            # One memo per pass: find_record_at/find_record_after share the
            # first page, and a retry always sees a fresh index.
            memo = {}
            try:
                plan = self.plan_window(
                    cam, t_ev, window, memo=memo, source=source,
                    allow_seam_wait=seam_waits < MAX_SEAM_WAITS)
            except SynologyError as e:
                logging.warning("%s: cannot list recordings: %s", cam.name, e)
                if self._wait(retry_secs):
                    return None
                continue

            if isinstance(plan, PlanWait):
                if plan.seam:
                    seam_waits += 1
                if self._wait(plan.delay):
                    return None
                continue

            if isinstance(plan, PlanGap):
                if not plan.segments:
                    logging.warning(
                        "%s: no recording at %s, next chunk starts %s; "
                        "nothing to cut", cam.name, self._stamp(plan.at),
                        self._stamp(plan.next_start) if plan.next_start else "later")
                    return None
                logging.warning(
                    "%s: recording gap at %s (%.0fs, next chunk starts %s); "
                    "cutting what exists", cam.name, self._stamp(plan.at),
                    (plan.next_start - plan.at) if plan.next_start else 0.0,
                    self._stamp(plan.next_start) if plan.next_start else "later")
                result = self._download_plan(cam, t_ev, plan.segments, kind,
                                             attempt, attempts)
                return result if isinstance(result, dict) else None

            if not plan:
                if self._wait(retry_secs):
                    return None
                continue

            result = self._download_plan(cam, t_ev, plan, kind, attempt, attempts)
            if isinstance(result, dict):
                return result
            if isinstance(result, PlanWait):
                if self._wait(result.delay):
                    return None
                continue
            if self._wait(retry_secs):
                return None

        logging.error("%s: giving up on the recording after %i attempts",
                      cam.name, attempts)
        # The chosen stream may have stopped being written: re-evaluate it on
        # the next event instead of retrying the same list for ever.
        self._sources.invalidate(cam)
        return None

    def _download_plan(self, cam, t_ev, segments, kind, attempt, attempts):
        """Download and verify the planned chunks.

        Returns the result dict, ``PlanWait`` when a fragment is shorter than
        requested (the rest may still be written), or None on a download error.
        """
        tag = self._tag(cam, t_ev, kind)
        parts = []
        current = None
        snap = 0.0
        try:
            for index, seg in enumerate(segments):
                current = self._workdir / ("%s_%02d.mp4" % (tag, index))
                if index == 0:
                    start_offset = float(int(seg.offset))
                    event_rel = seg.offset - start_offset
                    play = float(max(1, int(event_rel + seg.take)))
                else:
                    start_offset = 0.0
                    event_rel = 0.0
                    play = float(max(1, int(seg.take)))
                self._client.download(seg.recording, int(start_offset * 1000),
                                      int(play * 1000), current)
                duration = media.probe_duration(current)
                need = (play if index == 0 else seg.take) + FRAGMENT_PAD_SECS
                if duration is not None and duration < need - SNAP_MISS_TOLERANCE_SECS:
                    logging.info("%s: segment %i not written yet (rec %i, need "
                                 "%.1fs, got %.1fs); retry %i/%i", cam.name, index,
                                 seg.recording.id, need, duration, attempt, attempts)
                    self._unlink_parts([p["path"] for p in parts] + [current])
                    return PlanWait(max(0.5, need - duration + 0.5))

                if index == 0:
                    if duration is None:
                        logging.warning("%s: cannot measure the fragment; cutting "
                                        "from the requested point", cam.name)
                        seek = event_rel
                        keep = seg.take
                    else:
                        snap = duration - (play + FRAGMENT_PAD_SECS)
                        if snap < 0.0:
                            snap = 0.0
                        elif snap > SNAP_MAX_SECS:
                            logging.warning("%s: unexpected key-frame offset %.1fs; "
                                            "cutting from the requested point",
                                            cam.name, snap)
                            snap = 0.0
                        seek = event_rel + snap
                        keep = max(0.0, min(seg.take, duration - seek))
                elif duration is None:
                    logging.warning("%s: cannot measure chunk %i; retry %i/%i",
                                    cam.name, index, attempt, attempts)
                    self._unlink_parts([p["path"] for p in parts] + [current])
                    return PlanWait(RETRY_DEFAULT_SECS[kind])
                else:
                    seek = 0.0
                    keep = min(seg.take, duration)
                parts.append({"path": str(current), "seek": seek, "keep": keep})

            if len(parts) > 1:
                sizes = [media.probe_size(p["path"]) for p in parts]
                if len(set(sizes)) > 1:
                    logging.warning(
                        "%s: recording chunks differ in size (%s); using the "
                        "first chunk only", cam.name,
                        ", ".join("%sx%s" % s if s else "?" for s in sizes))
                    self._unlink_parts([p["path"] for p in parts[1:]])
                    parts = parts[:1]

            return {
                "segments": parts,
                "lead": max(0.0, t_ev - segments[0].recording.start_time),
                "snap": snap,
                "fragment": sum(part["keep"] for part in parts),
                "recording_ids": [seg.recording.id for seg in segments],
                "episode_start": t_ev,
            }
        except (SynologyError, OSError) as e:
            logging.warning("%s: cannot download the recording: %s", cam.name, e)
            self._unlink_parts([p["path"] for p in parts] + [current])
            return None

    def _unlink_parts(self, paths):
        for path in paths:
            _unlink(path)
