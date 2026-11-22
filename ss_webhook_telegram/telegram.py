"""Telegram delivery with bounded retries and a small upload queue.

Uploads are queued and sent by a few worker threads: one stuck upload (read
timeout + retries, up to a few minutes) occupies a single worker instead of
blocking every camera. telebot keeps one ``requests.Session`` per thread, so a
single ``TeleBot`` per token is safe to share between the senders.

The caller hands over ownership of the files: after a send* method returns
True the notifier removes them once the upload finishes (or is dropped as too
old). When a method returns False nothing was queued and the files are already
removed.
"""
from __future__ import annotations

import logging
import queue
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import urlsplit

import requests
import telebot
from telebot import apihelper
from telebot.apihelper import ApiException, ApiTelegramException
from telebot.types import InputMediaPhoto

TELEGRAM_RETRY_AFTER_CAP_SECS = 60
TELEGRAM_CHAT_ACTION_TIMEOUT_SECS = 10

SEND_WORKERS = 3
SEND_QUEUE_SIZE = 64
QUEUE_WARN_DEPTH = 8
PHOTO_MAX_QUEUE_AGE_SECS = 120
VIDEO_MAX_QUEUE_AGE_SECS = 600
ALBUM_MAX_ITEMS = 10


def mask_proxy(proxy):
    """Hide proxy credentials before logging the URL."""
    try:
        parsed = urlsplit(proxy)
        port = ":%i" % parsed.port if parsed.port else ""
    except ValueError:
        return "***"
    if parsed.username or parsed.password:
        return "%s://***@%s%s" % (parsed.scheme, parsed.hostname or "", port)
    return proxy


class _UrlTemplate:
    """Stand-in for telebot's API_URL: resolves the URL per thread.

    telebot reads the module-level ``apihelper.API_URL`` and calls ``format``
    on it for every request, so an object with ``format`` lets each sender
    thread use its own endpoint without touching the global.
    """

    def __init__(self, pool):
        self._pool = pool

    def format(self, token, method_name):
        return self._pool.url_for_request(token, method_name)


class BaseUrlPool:
    """Several interchangeable Telegram API base URLs.

    The active URL is sticky: a successful send keeps it, the first failure
    moves to the next one, and every retry rotates again. Requests of one
    attempt always use the index chosen at its start, so concurrent senders do
    not fight over a global.
    """

    def __init__(self, urls):
        self._urls = tuple(urls)
        self._active = 0
        self._lock = threading.Lock()
        self._local = threading.local()

    def __len__(self):
        return len(self._urls)

    def template(self):
        return _UrlTemplate(self)

    def begin(self):
        """Index for the requests of the current attempt in this thread."""
        with self._lock:
            index = self._active
        self._local.index = index
        return index

    def current(self):
        with self._lock:
            return self._urls[self._active]

    def host(self, index=None):
        url = self.current() if index is None else self._urls[index]
        try:
            return urlsplit(url).hostname or url
        except ValueError:
            return url

    def confirm(self, index):
        """The send succeeded through this URL: keep it active."""
        with self._lock:
            self._active = index

    def rotate(self, index):
        with self._lock:
            self._active = (index + 1) % len(self._urls)
            return self._active

    def url_for_request(self, token, method_name):
        index = getattr(self._local, "index", None)
        with self._lock:
            if index is None or not 0 <= index < len(self._urls):
                index = self._active
            base = self._urls[index]
        return "%s/bot%s/%s" % (base, token, method_name)


def setup(config):
    """Apply process-wide telebot settings once, before any bot is created."""
    if config.proxy:
        apihelper.proxy = {"https": config.proxy}
        logging.info("Proxy set: %s", mask_proxy(config.proxy))
    urls = None
    if config.tele_base_urls:
        urls = BaseUrlPool(config.tele_base_urls)
        apihelper.API_URL = urls.template()
        logging.info("Telegram base URLs (%i): %s", len(urls),
                     ", ".join(urls.host(i) for i in range(len(urls))))
    apihelper.CONNECT_TIMEOUT = config.tele_connect_timeout
    apihelper.READ_TIMEOUT = config.tele_read_timeout
    logging.info("Telegram timeouts: connect %ss, read %ss, send attempts %i, "
                 "senders %i",
                 config.tele_connect_timeout, config.tele_read_timeout,
                 config.tele_send_attempts, SEND_WORKERS)
    return urls


def _reset_session():
    """Drop telebot's keep-alive pool so a retry opens a fresh connection."""
    try:
        apihelper._get_req_session(reset=True)
    except Exception as e:  # private telebot API, never break a send because of it
        logging.debug("Cannot reset Telegram HTTP session: %s", e)


def _remove_files(paths):
    for path in paths or ():
        try:
            Path(path).unlink(missing_ok=True)
        except OSError as e:
            logging.warning("Cannot remove %s: %s", path, e)


@dataclass
class _Job:
    kind: str
    token: str
    chat_id: object
    label: str
    call: object
    paths: tuple
    max_age: float
    created: float = field(default_factory=time.time)
    done: threading.Event = field(default_factory=threading.Event)


class Notifier:
    def __init__(self, config, url_pool=None):
        self._config = config
        self._urls = url_pool
        self._bots = {}
        self._registry_lock = threading.Lock()
        self._queue = queue.Queue(maxsize=SEND_QUEUE_SIZE)
        self._workers = []
        for index in range(SEND_WORKERS):
            thread = threading.Thread(target=self._send_loop,
                                      name="tg-send-%i" % index, daemon=True)
            thread.start()
            self._workers.append(thread)

    def _bot(self, token):
        with self._registry_lock:
            bot = self._bots.get(token)
            if bot is None:
                bot = telebot.TeleBot(token)
                self._bots[token] = bot
            return bot

    @staticmethod
    def _chat_action(bot, chat_id):
        try:
            bot.send_chat_action(chat_id, "upload_video",
                                 timeout=TELEGRAM_CHAT_ACTION_TIMEOUT_SECS)
        except Exception as e:
            logging.warning("Cannot send chat action to %s: %s: %s",
                            chat_id, type(e).__name__, e)

    # -- queue ------------------------------------------------------------

    def _enqueue(self, kind, token, chat_id, paths, label, call, max_age):
        """Queue a send; returns the job, or None when the queue is full.

        The caller owns the files only until this returns; a full queue removes
        them, a queued job removes them after the upload (or when it is dropped
        as stale).
        """
        job = _Job(kind, token, chat_id, label, call, tuple(paths), max_age)
        try:
            self._queue.put_nowait(job)
        except queue.Full:
            logging.error("Telegram queue is full (%i jobs); dropping %s (%s)",
                          SEND_QUEUE_SIZE, kind, label)
            _remove_files(paths)
            return None
        depth = self._queue.qsize()
        if depth > QUEUE_WARN_DEPTH:
            logging.warning("Telegram queue depth %i; uploads are backing up", depth)
        return job

    def _send_loop(self):
        while True:
            job = self._queue.get()
            try:
                if job is None:
                    return
                waited = time.time() - job.created
                if waited > job.max_age:
                    logging.warning("Telegram %s (%s) waited %.0fs; dropping a "
                                    "stale message", job.kind, job.label, waited)
                    continue
                if waited > 5:
                    logging.info("Telegram %s (%s): %.0fs in the send queue",
                                 job.kind, job.label, waited)
                self._upload(job)
            except Exception:
                logging.exception("Unexpected error in the Telegram sender")
            finally:
                if job is not None:
                    _remove_files(job.paths)
                    job.done.set()
                self._queue.task_done()

    def close(self, timeout=20):
        """Ask the senders to stop; queued but unsent messages are logged."""
        pending = self._queue.qsize()
        if pending:
            logging.warning("Telegram: %i message(s) still queued at shutdown",
                            pending)
        for _ in self._workers:
            try:
                self._queue.put_nowait(None)
            except queue.Full:
                break
        deadline = time.monotonic() + timeout
        for thread in self._workers:
            thread.join(max(0.0, deadline - time.monotonic()))

    # -- upload -----------------------------------------------------------

    def _upload(self, job):
        """Run `job.call(bot)` with bounded retries."""
        bot = self._bot(job.token)
        max_attempts = self._config.tele_send_attempts
        backoff = self._config.tele_retry_backoff_secs

        for attempt in range(1, max_attempts + 1):
            started = time.monotonic()
            retry_after = None
            endpoint = self._urls.begin() if self._urls else None
            if attempt == 1:
                self._chat_action(bot, job.chat_id)
            try:
                job.call(bot)
                if endpoint is not None:
                    self._urls.confirm(endpoint)
                logging.info("Telegram %s sent (%s, attempt %i/%i, %.1fs%s)",
                             job.kind, job.label, attempt, max_attempts,
                             time.monotonic() - started, self._endpoint(endpoint))
                return True

            except ApiTelegramException as e:
                elapsed = time.monotonic() - started
                error_code = e.error_code or 0
                description = (e.result_json or {}).get("description", e)
                if error_code == 429:
                    parameters = (e.result_json or {}).get("parameters") or {}
                    try:
                        retry_after = float(parameters.get("retry_after", backoff))
                    except (TypeError, ValueError):
                        retry_after = backoff
                    retry_after = min(retry_after, TELEGRAM_RETRY_AFTER_CAP_SECS)
                    logging.warning("Telegram rate limit (%s, attempt %i/%i, "
                                    "%.1fs), retry_after %ss", job.label, attempt,
                                    max_attempts, elapsed, retry_after)
                elif error_code >= 500:
                    logging.warning("Telegram transient error %i (%s, attempt %i/%i, "
                                    "%.1fs): %s", error_code, job.label, attempt,
                                    max_attempts, elapsed, description)
                else:
                    logging.error("Telegram rejected %s (%s, HTTP %i, attempt %i/%i, "
                                  "%.1fs): %s; not retrying", job.kind, job.label,
                                  error_code, attempt, max_attempts, elapsed,
                                  description)
                    return False

            except requests.exceptions.RequestException as e:
                logging.warning("Telegram %s attempt %i/%i (%s) failed after %.1fs: "
                                "%s: %s", job.kind, attempt, max_attempts, job.label,
                                time.monotonic() - started, type(e).__name__, e)

            except ApiException as e:
                status = getattr(e.result, "status_code", None)
                if status is None or status == 200 or status >= 500:
                    logging.warning("Telegram %s attempt %i/%i (%s) failed after %.1fs: "
                                    "%s (HTTP %s, transient)", job.kind, attempt,
                                    max_attempts, job.label,
                                    time.monotonic() - started, type(e).__name__, status)
                else:
                    logging.error("Telegram API error (%s, HTTP %s, attempt %i/%i, "
                                  "%.1fs): %s; not retrying", job.label, status, attempt,
                                  max_attempts, time.monotonic() - started, e)
                    return False

            except Exception:
                logging.exception("Unexpected error sending %s (%s, attempt %i/%i); "
                                  "not retrying", job.kind, job.label, attempt,
                                  max_attempts)
                return False

            if attempt >= max_attempts:
                break
            delay = retry_after if retry_after is not None else backoff * attempt
            if retry_after is None:
                _reset_session()
            if endpoint is not None:
                self._urls.rotate(endpoint)
                logging.warning("Telegram endpoint switched to %s",
                                self._urls.host())
            logging.info("Retrying Telegram %s (%s) in %.1fs (attempt %i/%i)",
                         job.kind, job.label, delay, attempt + 1, max_attempts)
            time.sleep(delay)

        logging.error("Giving up sending %s (%s) after %i attempts",
                      job.kind, job.label, max_attempts)
        return False

    def _endpoint(self, index):
        if self._urls is None or index is None:
            return ""
        return ", via %s" % self._urls.host(index)

    # -- public API -------------------------------------------------------

    @staticmethod
    def _photo_call(chat_id, path, caption):
        def call(bot):
            with open(path, "rb") as photo:
                bot.send_photo(chat_id, photo, caption=caption,
                               disable_notification=True)
        return call

    @staticmethod
    def _album_call(chat_id, chunk, caption):
        def call(bot):
            opened = [open(path, "rb") for path in chunk]
            try:
                media = [
                    InputMediaPhoto(fh, caption=caption if i == 0 else None)
                    for i, fh in enumerate(opened)
                ]
                bot.send_media_group(chat_id, media, disable_notification=True)
            finally:
                for fh in opened:
                    fh.close()
        return call

    def send_photo(self, token, chat_id, path, caption, label):
        path = str(path)
        job = self._enqueue("photo", token, chat_id, [path], label,
                            self._photo_call(chat_id, path, caption),
                            PHOTO_MAX_QUEUE_AGE_SECS)
        return job is not None

    def send_photos(self, token, chat_id, paths, caption, label, wait=False):
        """Send one photo, or several as a single album (media group).

        Telegram allows 2..10 items per group, so longer bursts are chunked.
        The caption goes on the first photo of the first group. With ``wait``
        the call returns only after the uploads finished, which is how the
        album is kept ahead of the clip.
        """
        paths = [str(p) for p in paths]
        if not paths:
            return True
        jobs = []
        if len(paths) == 1:
            jobs.append(self._enqueue("photo", token, chat_id, [paths[0]], label,
                                      self._photo_call(chat_id, paths[0], caption),
                                      PHOTO_MAX_QUEUE_AGE_SECS))
        else:
            for start in range(0, len(paths), ALBUM_MAX_ITEMS):
                chunk = paths[start:start + ALBUM_MAX_ITEMS]
                if len(chunk) == 1:
                    jobs.append(self._enqueue(
                        "photo", token, chat_id, chunk, label,
                        self._photo_call(chat_id, chunk[0], None),
                        PHOTO_MAX_QUEUE_AGE_SECS))
                    continue
                group_caption = caption if start == 0 else None
                jobs.append(self._enqueue(
                    "album", token, chat_id, chunk, label,
                    self._album_call(chat_id, chunk, group_caption),
                    PHOTO_MAX_QUEUE_AGE_SECS))
        if wait:
            for job in jobs:
                if job is not None:
                    job.done.wait(PHOTO_MAX_QUEUE_AGE_SECS + 60)
        return all(job is not None for job in jobs)

    def send_animation(self, token, chat_id, path, caption, label):
        path = str(path)

        def call(bot):
            with open(path, "rb") as anim:
                bot.send_animation(chat_id, anim, caption=caption,
                                   disable_notification=True)

        job = self._enqueue("animation", token, chat_id, [path], label, call,
                            VIDEO_MAX_QUEUE_AGE_SECS)
        return job is not None

    def send_video(self, token, chat_id, path, caption, label):
        path = str(path)

        def call(bot):
            with open(path, "rb") as video:
                bot.send_video(chat_id, video, caption=caption,
                               disable_notification=True)

        job = self._enqueue("video", token, chat_id, [path], label, call,
                            VIDEO_MAX_QUEUE_AGE_SECS)
        return job is not None