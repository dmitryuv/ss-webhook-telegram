"""Entry point: config, logging, camera check, HTTP receiver and workers."""
from __future__ import annotations

import argparse
import logging
import queue
import signal
import sys
import threading
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from . import media, telegram, webhook
from . import __version__
from .config import ConfigError, load
from .store import EventStore
from .synology import SynologyError, SurveillanceClient
from .worker import CameraResolver, EventWorker, ProcessingPool

LOG_FORMAT = "[%(asctime)s] [%(levelname)s] (%(threadName)-10s) %(message)s"
LOG_DATEFMT = "%Y-%m-%d %H:%M:%S"
WORKDIR_MAX_AGE_SECS = 3600
DEDUP_PRUNE_INTERVAL_SECS = 3600


class TZFormatter(logging.Formatter):
    """Log timestamps in the configured timezone (the container runs in UTC)."""

    def __init__(self, fmt, datefmt, tz):
        super().__init__(fmt, datefmt)
        self._tz = tz

    def formatTime(self, record, datefmt=None):
        moment = datetime.fromtimestamp(record.created, self._tz)
        return moment.strftime(datefmt) if datefmt else moment.isoformat()


def _use_timezone(name):
    try:
        tz = ZoneInfo(name)
    except Exception as e:
        logging.warning("Cannot use timezone %r (%s); logging in server time",
                        name, e)
        return
    formatter = TZFormatter(LOG_FORMAT, LOG_DATEFMT, tz)
    for handler in logging.getLogger().handlers:
        handler.setFormatter(formatter)


def _cleanup_workdir(folder, max_age_secs=WORKDIR_MAX_AGE_SECS):
    """Drop leftovers from a previous run (async sends can outlive a restart)."""
    now = time.time()
    removed = 0
    for path in Path(folder).iterdir():
        try:
            if path.is_file() and now - path.stat().st_mtime > max_age_secs:
                path.unlink()
                removed += 1
        except OSError:
            continue
    if removed:
        logging.info("Removed %i stale file(s) from %s", removed, folder)


def parse_args(argv):
    parser = argparse.ArgumentParser(
        prog="ss_webhook_telegram",
        description="Send Surveillance Station webhook events to Telegram.",
    )
    parser.add_argument("config", help="path to config.json")
    parser.add_argument("--dry-run", action="store_true",
                        help="do everything but sending; do not persist state")
    parser.add_argument("--list-cameras", action="store_true",
                        help="print the Camera.List ({name, id, dsId}) and exit")
    parser.add_argument("--log-level", choices=("DEBUG", "INFO", "WARNING", "ERROR"),
                        help="override log_level from the config")
    return parser.parse_args(argv)


def _prepare_folder(folder, label):
    path = Path(folder)
    try:
        path.mkdir(parents=True, exist_ok=True)
        probe = path / ".write-test"
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
    except OSError as e:
        raise ConfigError("%s is not writable: %s (%s)" % (label, folder, e))


def _decode_description():
    """The decode path for the startup log, e.g. "vaapi (iHD, /dev/dri/...)"."""
    driver = media.vaapi_driver_name()
    if not driver:
        return "software"
    return "vaapi (%s, %s)" % (driver, media.HWACCEL_DEVICE)


def _prune_dedup(store, config, dry_run=False):
    """Drop dedup keys nobody can match any more.

    A repeated delivery arrives within seconds, and once an event is older than
    an episode gap it starts a new episode anyway, so keeping the keys longer
    only grows the table.
    """
    if dry_run:
        return
    try:
        removed = store.prune_dedup(config.episode_gap_secs)
    except Exception:
        logging.exception("Cannot prune the dedup table")
        return
    if removed:
        logging.info("Pruned %i old event key(s)", removed)


def _install_signal_handlers(stop_event):
    def handler(signum, _frame):
        logging.info("Received %s, stopping", signal.Signals(signum).name)
        stop_event.set()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


def _log_cameras(infos):
    logging.info("Surveillance Station cameras:")
    for info in infos:
        ds = info.get("ownerDsId")
        if ds is None:
            ds = info.get("dsId")
        logging.info("  id=%s name=%s host=%s dsId=%s",
                     info.get("id"), info.get("name"), info.get("host"), ds)


def _log_config(config):
    logging.info("Config: timezone=%s burst=%i@%.1fs skip=%.1fs subsequent=%i "
                 "cooldown=%.0fs gap=%.0fs",
                 config.timezone, config.burst_photos, config.burst_interval_secs,
                 config.burst_skip_first_secs, config.subsequent_photos,
                 config.cooldown_secs, config.episode_gap_secs)
    logging.info("Photos: max_side=%ipx quality=%i snapshot_api=%s "
                 "(burst frames come from the recording)",
                 config.photo_max_side, config.photo_quality,
                 config.use_snapshot_api)
    logging.info("Recording stream: %s (auto picks the lightest stream that "
                 "covers photo_max_side and video_max_height)",
                 config.recording_stream)
    logging.info("Video: start=+%.1fs length=%.1fs max_height=%ip fps=%s format=%s",
                 config.video_skip_first_n_secs, config.video_max_length_secs,
                 config.video_max_height,
                 "pass (source timestamps)" if config.video_fps == media.FPS_PASSTHROUGH
                 else config.video_fps, config.video_format)
    logging.info("Concurrency: camera queues, %i processing thread(s), "
                 "%i telegram senders", config.processing_threads,
                 telegram.SEND_WORKERS)
    logging.info("Webhook: listen=%s path=%s auth=%s",
                 config.webhook_listen, config.webhook_path,
                 "on" if config.webhook_auth else "off")
    logging.info("Chats: %s (default=%s)", ", ".join(sorted(config.chats)),
                 config.default_chat)


def main(argv=None):
    args = parse_args(argv)
    logging.basicConfig(level=logging.INFO, format=LOG_FORMAT, datefmt=LOG_DATEFMT)

    try:
        config = load(args.config)
    except ConfigError as e:
        logging.error("Config error: %s", e)
        return 2

    logging.getLogger().setLevel(args.log_level or config.log_level)
    _use_timezone(config.timezone)
    # urllib3 logs full request URLs at DEBUG, including the session id in the
    # query string, so keep it quiet even in DEBUG mode.
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    if not config.syno_verify_ssl:
        import urllib3
        from urllib3.exceptions import InsecureRequestWarning
        urllib3.disable_warnings(InsecureRequestWarning)

    logging.info("Starting (%s mode), version %s",
                 "dry-run" if args.dry_run else "normal", __version__)
    _log_config(config)

    client = SurveillanceClient(
        config.synology_base_api_url,
        config.synology_user,
        config.synology_password,
        connect_timeout=config.syno_timeout_connect,
        read_timeout=config.syno_timeout_read,
        verify=config.syno_verify_ssl,
    )

    if args.list_cameras:
        try:
            _log_cameras(client.cameras())
        except SynologyError as e:
            logging.error("Cannot read the camera list: %s", e)
            return 1
        finally:
            client.logout()
        return 0

    try:
        media.check_ffmpeg()
        media.configure_hwaccel(config.ffmpeg_hwaccel)
        logging.info("FFmpeg: decode=%s, encode=%s",
                     _decode_description(),
                     "libx264" if config.video_format == "mp4" else "gif")
        _prepare_folder(config.data_folder, "data_folder")
        _prepare_folder(config.ffmpeg_working_folder, "ffmpeg_working_folder")
        _cleanup_workdir(config.ffmpeg_working_folder)
    except (media.MediaError, ConfigError) as e:
        logging.error("%s", e)
        return 2

    store = EventStore(config.data_folder)
    resolver = CameraResolver(client)
    _prune_dedup(store, config, args.dry_run)
    try:
        resolver.resolve("")  # trigger the first Camera.List refresh
        _log_cameras(client.cameras())
    except SynologyError as e:
        logging.error("Cannot read the camera list: %s", e)
        store.close()
        return 1

    url_pool = telegram.setup(config)
    notifier = telegram.Notifier(config, url_pool=url_pool)
    stop_event = threading.Event()
    event_queue = queue.Queue()
    pool = ProcessingPool(config.processing_threads)

    worker = EventWorker(config, client, store, notifier, resolver, pool,
                         event_queue, stop_event, dry_run=args.dry_run)

    try:
        httpd = webhook.serve(config, event_queue.put)
    except (OSError, ValueError) as e:
        logging.error("Cannot start the webhook server on %s: %s",
                      config.webhook_listen, e)
        client.logout()
        store.close()
        return 2

    _install_signal_handlers(stop_event)

    pool.start()
    worker.start()

    http_thread = threading.Thread(target=httpd.serve_forever, name="webhook-http",
                                   daemon=True)
    http_thread.start()
    logging.info("Listening on %s%s", config.webhook_listen, config.webhook_path)

    try:
        next_prune = time.time() + DEDUP_PRUNE_INTERVAL_SECS
        while not stop_event.is_set():
            stop_event.wait(1)
            if time.time() >= next_prune:
                _prune_dedup(store, config, args.dry_run)
                next_prune = time.time() + DEDUP_PRUNE_INTERVAL_SECS
    finally:
        logging.info("Stopping")
        httpd.shutdown()
        httpd.server_close()
        # Keep the total below Docker's 10s stop timeout: an interrupted clip is
        # simply lost (the event was already claimed by dedup).
        worker.join(timeout=4)
        pool.stop()
        notifier.close(timeout=6)
        client.logout()
        store.close()
        logging.info("Stopped")
    return 0


if __name__ == "__main__":
    sys.exit(main())
