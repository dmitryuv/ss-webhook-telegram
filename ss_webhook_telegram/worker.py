"""Event processing: dedup, episodes, and recording-derived photos + clip.

Flow for one webhook event:

1. resolve the camera by name (Camera.List cache);
2. dedup on (camera, event-second);
3. the first event of an episode downloads one recording window and a single
   ffmpeg pass turns it into the photo frames and the clip; later events send a
   few follow-up frames from their own moment, never faster than the cooldown;
4. the download and the Telegram sends happen in the camera's own thread; the
   CPU-heavy ffmpeg pass runs on a small shared pool (see ``processing_threads``)
   because decoding H.265 is CPU bound.

Photos are frames of the recording, so the service no longer depends on
Surveillance Station snapshots (which stop working for H.265 in DSM 7.2.2).
"""
from __future__ import annotations

import logging
import queue
import threading
import time
import unicodedata
import zlib
from concurrent.futures import Future
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import media
from .config import Config
from .store import Episode, EventStore
from .synology import SurveillanceClient, SynologyError

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
MAX_ATTEMPTS = 8
RETRY_DEFAULT_SECS = 2.0
# Follow-up photos are the camera key frames (like the old live snapshots), so
# the window must cover this many key-frame intervals. The real interval is
# measured from the burst fragment and remembered per camera; this is only the
# fallback until the first episode of that camera.
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
        return by_name.get(key) or fold.get(key.casefold())


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
    video_fps: int
    video_format: str
    video_preset: str
    use_snapshot_api: str

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

    return Settings(
        use_snapshot_api=_snapshot_mode(overrides.get("use_snapshot_api"),
                                        config.use_snapshot_api),
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
        video_fps=whole("video_fps", config.video_fps),
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
        # In memory only: a camera may switch codec, and a restart re-evaluates.
        self._snapshot_ok = {}

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
            # of parking it in the camera queue.
            settings = build_settings(self._config, payload.get("config"))
            if settings.cooldown_secs > 0 or settings.subsequent_photos <= 0:
                logging.info("%s: burst in flight, dropping the event (a follow-up "
                             "would be within cooldown)", name)
                return
        events = channel[0]
        payload["_channel"] = key
        try:
            events.put_nowait(payload)
        except queue.Full:
            logging.warning("%s: camera queue is full (%i events); dropping the "
                            "newest event", name, CAMERA_QUEUE_SIZE)
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
        source = None
        try:
            label = self._label(cam, t_ev)
            send_frames = settings.burst_photos > 0 and not stale
            if stale and settings.burst_photos > 0:
                logging.warning("%s: event %s is %.0fs old, skipping the photo burst",
                                cam.name, self._stamp(t_ev), time.time() - t_ev)
            window = settings.window_end(with_frames=send_frames)
            fragment = self._download_ready(cam, t_ev, window, "ep")
            if fragment is None:
                return

            source = fragment["path"]
            clip_path, frame_paths = self._process_clip(
                cam, settings, fragment, send_frames)
            self._remember_gop(cam, source)
            _unlink(source)
            source = None
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
            logging.info("New episode: %s (%i photo(s), clip=%s)",
                         cam.name, sent_photos, settings.video_format)
        finally:
            self._set_burst(channel, False)
            _unlink(source)

    def _run_followup(self, cam, chat, settings, t_ev, count):
        """A later event of an episode: a few photos from its own moment.

        Photos come from the live Snapshot API when `use_snapshot_api` allows it
        (`true`, or `auto` and it has not failed yet for this camera), otherwise
        from the camera key frames of a tiny recording window.
        """
        mode = settings.use_snapshot_api
        if mode != "false" and (mode == "true" or self._snapshot_ok.get(cam.name, True)):
            try:
                return self._followup_snapshots(cam, chat, settings, t_ev, count)
            except (SynologyError, media.MediaError) as e:
                if mode == "true":
                    logging.error("%s: snapshot API failed (%s: %s); "
                                  "use_snapshot_api=true, not falling back",
                                  cam.name, type(e).__name__, e)
                    return 0
                logging.warning("%s: snapshot API unavailable (%s: %s); using "
                                "recording frames", cam.name, type(e).__name__, e)
                self._snapshot_ok[cam.name] = False
        return self._followup_frames(cam, chat, settings, t_ev, count)

    def _followup_snapshots(self, cam, chat, settings, t_ev, count):
        """Live snapshots (cheap JPEGs) - the pre-DSM-7.2.2 photo source."""
        tag = self._tag(cam, t_ev, "fu")
        paths = []
        seen = set()
        for index in range(count):
            if index and settings.burst_interval_secs > 0:
                if self._stop.wait(settings.burst_interval_secs):
                    break
            if self._stop.is_set():
                break
            raw = self._client.take_snapshot(cam.id)
            digest = "%08x" % (zlib.crc32(raw) & 0xFFFFFFFF)
            if digest in seen:  # snapshots inside one key-frame interval repeat
                continue
            seen.add(digest)
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

    def _followup_frames(self, cam, chat, settings, t_ev, count):
        """Camera key frames from a tiny recording window (no clip to share)."""
        gop = self._store.gop(cam.name, DEFAULT_GOP_SECS)
        window = (count - 1) * gop + 1.0
        fragment = self._download_ready(cam, t_ev, window, "fu")
        if fragment is None:
            return 0
        source = fragment["path"]
        tag = self._tag(cam, t_ev, "fu")
        pattern = str(self._workdir / (tag + "_%02d.jpg"))
        try:
            # Take the fragment's first key frame(s): seeking to the event would
            # discard the key frame at or before it, and the next one can be a
            # whole GOP past the tiny window. On the key frame <= the event is
            # fine for a follow-up.
            args = media.build_keyframes_args(
                source, pattern, seek_secs=0.0, count=count,
                photo_max_side=settings.photo_max_side,
                photo_quality=settings.photo_quality)
            self._run_ffmpeg(args, cam, t_ev)
            paths = [str(p) for p in sorted(self._workdir.glob(tag + "_*.jpg"))]
        finally:
            _unlink(source)
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
            fragment["path"], clip_path, seek_secs=fragment["seek"],
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
        self._log_clip(cam, fragment, clip_path, frame_paths)
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

    def _log_clip(self, cam, fragment, clip_path, frame_paths):
        clip_secs = media.probe_duration(clip_path)
        extra = "lead %.0fs, snap %.1fs" % (fragment["lead"], fragment["snap"])
        if fragment["fragment"] is not None:
            extra += ", fragment %.1fs" % fragment["fragment"]
        if clip_secs is not None:
            extra += ", clip %.1fs" % clip_secs
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

    def _wait(self, secs):
        """Sleep up to `secs`, honouring shutdown. True when we were stopped."""
        if secs <= 0:
            return self._stop.is_set()
        return self._stop.wait(min(secs, 30.0))

    def _download_ready(self, cam, t_ev, window, kind):
        """Download [event, event + window] of the recording covering the event.

        Nothing before the event is used: the burst starts at
        ``event + burst_skip_first_secs`` and the clip at
        ``event + video_skip_first_n_secs``, so the request begins at the event.
        Waits until the material should exist (wall clock), downloads the
        fragment, and verifies its duration: the API snaps the start back to the
        previous key frame, so the measured duration reveals how far before the
        requested offset the fragment began (``snap``). Returns a dict, or None
        on stop / a persistent failure.
        """
        ready_at = t_ev + window + VIDEO_READY_BUFFER_SECS
        now = time.time()
        if now < ready_at:
            logging.info("%s: waiting %.1fs for the recording (%s)",
                         cam.name, ready_at - now, self._stamp(ready_at))
            if self._wait(ready_at - now):
                return None

        for attempt in range(1, MAX_ATTEMPTS + 1):
            if self._stop.is_set():
                return None
            try:
                recording = self._client.find_recording(
                    cam.id, cam.src_type, cam.ds_id, t_ev)
            except SynologyError as e:
                logging.warning("%s: cannot list recordings: %s", cam.name, e)
                if self._wait(RETRY_DEFAULT_SECS):
                    return None
                continue
            if recording is None:
                if self._wait(1.0):
                    return None
                continue

            lead = max(0.0, t_ev - recording.start_time)
            start_offset = float(int(lead))
            event_rel = lead - start_offset
            play_secs = float(max(1, int(event_rel + window)))
            fragment_end = play_secs + 1.0
            source = self._workdir / (self._tag(cam, t_ev, kind) + ".mp4")
            try:
                self._client.download(recording, int(start_offset * 1000),
                                      int(play_secs * 1000), source)
            except (SynologyError, OSError) as e:
                logging.warning("%s: cannot download the recording: %s", cam.name, e)
                _unlink(source)
                if self._wait(RETRY_DEFAULT_SECS):
                    return None
                continue

            snap = 0.0
            duration = media.probe_duration(source)
            if duration is None:
                logging.warning("%s: cannot measure the fragment; cutting from the "
                                "requested point", cam.name)
            else:
                snap = duration - fragment_end
                if snap < -SNAP_MISS_TOLERANCE_SECS:
                    _unlink(source)
                    if self._wait((-snap) + 0.5):
                        return None
                    continue
                if snap < 0.0:
                    snap = 0.0
                elif snap > SNAP_MAX_SECS:
                    logging.warning("%s: unexpected keyframe offset %.1fs; cutting "
                                    "from the requested point", cam.name, snap)
                    snap = 0.0

            return {
                "path": str(source), "seek": max(0.0, event_rel + snap),
                "lead": lead, "snap": snap,
                "fragment": duration, "episode_start": t_ev, "recording_id": recording.id,
            }

        logging.error("%s: giving up on the recording after %i attempts",
                      cam.name, MAX_ATTEMPTS)
        return None
