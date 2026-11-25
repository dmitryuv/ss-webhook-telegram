"""Configuration loading and validation (TOML).

The app has no per-camera settings: cameras and the target chat are carried by
the webhook body. The config holds Synology/Telegram credentials, the webhook
listener, named chat groups and the global defaults.

Every key listed under "Tunables" (see config-sample.toml) can be overridden
per Action Rule through the webhook body's ``config`` object, using the *same*
key name.
"""
from __future__ import annotations

import logging
import tomllib
from dataclasses import dataclass
from pathlib import Path
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from .media import DEFAULT_VIDEO_FPS, FPS_PASSTHROUGH


class ConfigError(Exception):
    """Raised when the config file is missing, malformed or inconsistent."""


TOP_KEYS = {
    "data_folder", "ffmpeg_working_folder", "timezone", "log_level",
    "processing_threads", "ffmpeg_hwaccel",
    "synology", "webhook", "telegram", "defaults", "chats",
}
# Table [defaults]: merged into every event and overridable via the payload
# "config" object using the same key names.
DEFAULTS_KEYS = {
    "episode_gap_secs", "cooldown_secs", "max_photos_per_episode",
    "burst_photos", "burst_interval_secs", "burst_skip_first_secs",
    "subsequent_photos", "photo_max_side", "photo_quality",
    "video_skip_first_n_secs", "video_max_length_secs", "video_max_height",
    "video_fps", "video_format", "video_preset", "use_snapshot_api",
    "recording_stream",
}
SYNOLOGY_KEYS = {"base_api_url", "user", "password", "verify_ssl",
                 "timeout_connect", "timeout_read"}
WEBHOOK_KEYS = {"listen", "path", "auth"}
AUTH_KEYS = {"user", "password"}
TELEGRAM_KEYS = {"base_url", "proxy", "connect_timeout", "read_timeout",
                 "send_attempts", "retry_backoff_secs", "default_chat"}
CHAT_KEYS = {"name", "tele_bot_token", "tele_chat_id"}

FORMATS = {"mp4", "gif"}
LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR"}
HWACCEL_MODES = {"auto", "vaapi", "none"}


@dataclass(frozen=True)
class Chat:
    name: str
    tele_bot_token: str
    tele_chat_id: int


@dataclass(frozen=True)
class WebhookAuth:
    user: str
    password: str


@dataclass(frozen=True)
class Config:
    # paths / general
    data_folder: str
    ffmpeg_working_folder: str
    timezone: str
    log_level: str
    processing_threads: int
    ffmpeg_hwaccel: str
    # synology
    synology_base_api_url: str
    synology_user: str
    synology_password: str
    syno_verify_ssl: bool
    syno_timeout_connect: float
    syno_timeout_read: float
    # webhook
    webhook_listen: str
    webhook_path: str
    webhook_auth: WebhookAuth | None
    # telegram
    tele_base_urls: tuple[str, ...]
    proxy: str | None
    tele_connect_timeout: float
    tele_read_timeout: float
    tele_send_attempts: int
    tele_retry_backoff_secs: float
    default_chat: str
    chats: dict[str, Chat]
    # tunables (overridable via the payload "config" object)
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

    def chat(self, name):
        """Chat group by name, falling back to default_chat, or None."""
        if name:
            chat = self.chats.get(name)
            if chat is None:
                logging.warning("unknown chat group %r, using default %r",
                                name, self.default_chat)
            else:
                return chat
        return self.chats.get(self.default_chat)


def _number(raw, key, default, *, minimum=None, positive=False):
    if raw is None:
        raw = default
    if isinstance(raw, bool) or not isinstance(raw, (int, float, str)):
        raise ConfigError("%s: expected a number, got %r" % (key, raw))
    try:
        value = float(raw)
    except (TypeError, ValueError):
        raise ConfigError("%s: expected a number, got %r" % (key, raw))
    if positive and value <= 0:
        raise ConfigError("%s: must be greater than 0, got %r" % (key, raw))
    if minimum is not None and value < minimum:
        raise ConfigError("%s: must be >= %s, got %r" % (key, minimum, raw))
    return value


def _integer(raw, key, default, *, minimum=None, positive=False):
    value = _number(raw, key, default, minimum=minimum, positive=positive)
    if value != int(value):
        raise ConfigError("%s: expected a whole number, got %r" % (key, raw))
    return int(value)


def _boolean(raw, key, default):
    if raw is None:
        return default
    if isinstance(raw, bool):
        return raw
    if isinstance(raw, (int, float)):
        return bool(raw)
    if isinstance(raw, str):
        if raw.lower() in {"true", "yes", "1"}:
            return True
        if raw.lower() in {"false", "no", "0"}:
            return False
    raise ConfigError("%s: expected a boolean, got %r" % (key, raw))


def _required_string(raw, key):
    if not isinstance(raw, str) or not raw.strip():
        raise ConfigError("%s: required non-empty string" % key)
    return raw.strip()


def _chat_id(raw, key):
    value = _integer(raw, key, None)
    if value == 0:
        raise ConfigError("%s: must not be 0" % key)
    return value


def _table(raw, key):
    if not isinstance(raw, dict):
        raise ConfigError("%s: expected a table" % key)
    return raw


def _warn_unknown(raw, allowed, where):
    unknown = set(raw) - allowed
    if unknown:
        logging.warning("%s: ignoring unknown keys: %s", where,
                        ", ".join(sorted(unknown)))


def _choice(raw, key, default, allowed):
    value = str(raw if raw is not None else default).strip().lower().replace(" ", "")
    if value not in allowed:
        raise ConfigError("%s: expected one of %s, got %r"
                          % (key, ", ".join(sorted(allowed)), raw))
    return value


def _video_fps(raw, key, default=DEFAULT_VIDEO_FPS):
    """A positive frame rate, or "pass" to keep the recording's timestamps."""
    if isinstance(raw, str) and raw.strip().lower() == FPS_PASSTHROUGH:
        return FPS_PASSTHROUGH
    return _integer(raw, key, default, positive=True)


def _snapshot_mode(raw, key, default="auto"):
    """use_snapshot_api: auto (try, fall back), true, or false."""
    if raw is None:
        return default
    if isinstance(raw, bool):
        return "true" if raw else "false"
    text = str(raw).strip().lower()
    if text in {"auto", "true", "false"}:
        return text
    if text in {"1", "yes", "on"}:
        return "true"
    if text in {"0", "no", "off"}:
        return "false"
    raise ConfigError("%s: expected auto, true or false, got %r" % (key, raw))


def _webhook(raw):
    raw = _table(raw or {}, "webhook")
    _warn_unknown(raw, WEBHOOK_KEYS, "webhook")
    listen = _required_string(raw.get("listen", "0.0.0.0:8099"), "webhook.listen")
    path = _required_string(raw.get("path", "/webhook"), "webhook.path")
    if not path.startswith("/"):
        path = "/" + path
    auth = None
    if raw.get("auth") is not None:
        auth_raw = _table(raw["auth"], "webhook.auth")
        _warn_unknown(auth_raw, AUTH_KEYS, "webhook.auth")
        auth = WebhookAuth(
            user=_required_string(auth_raw.get("user"), "webhook.auth.user"),
            password=_required_string(auth_raw.get("password"), "webhook.auth.password"),
        )
    return listen, path, auth


def _chats(raw):
    if not isinstance(raw, list) or not raw:
        raise ConfigError("chats: required non-empty array of chat groups ([[chats]])")
    chats = {}
    for i, item in enumerate(raw):
        where = "chats[%i]" % i
        item = _table(item, where)
        _warn_unknown(item, CHAT_KEYS, where)
        name = _required_string(item.get("name"), where + ".name")
        if name in chats:
            raise ConfigError("%s: duplicate chat name %r" % (where, name))
        chats[name] = Chat(
            name=name,
            tele_bot_token=_required_string(item.get("tele_bot_token"),
                                            where + ".tele_bot_token"),
            tele_chat_id=_chat_id(item.get("tele_chat_id"), where + ".tele_chat_id"),
        )
    return chats


def load(path) -> Config:
    """Read, parse and validate the TOML config file. Raises ConfigError."""
    path = Path(path)
    try:
        raw = tomllib.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        raise ConfigError("config file not found: %s" % path)
    except tomllib.TOMLDecodeError as e:
        raise ConfigError("config file is not valid TOML: %s (%s)" % (path, e))
    if not isinstance(raw, dict):
        raise ConfigError("config file must contain a TOML table")

    _warn_unknown(raw, TOP_KEYS, "config")

    tz = str(raw.get("timezone", "Europe/Moscow")).strip()
    try:
        ZoneInfo(tz)
    except (ZoneInfoNotFoundError, ValueError):
        raise ConfigError("timezone: unknown zone %r" % tz)

    log_level = str(raw.get("log_level", "INFO")).strip().upper()
    if log_level not in LOG_LEVELS:
        raise ConfigError("log_level: expected one of %s, got %r"
                          % (", ".join(sorted(LOG_LEVELS)), raw.get("log_level")))

    synology = _table(raw.get("synology"), "synology")
    _warn_unknown(synology, SYNOLOGY_KEYS, "synology")

    defaults = _table(raw.get("defaults") or {}, "defaults")
    _warn_unknown(defaults, DEFAULTS_KEYS, "defaults")

    telegram = _table(raw.get("telegram") or {}, "telegram")
    _warn_unknown(telegram, TELEGRAM_KEYS, "telegram")

    attempts = _integer(telegram.get("send_attempts"), "telegram.send_attempts",
                        3, positive=True)
    if attempts > 3:
        logging.warning("telegram.send_attempts %i is above the hard limit, using 3", attempts)
        attempts = 3

    tele_base_urls = ()
    raw_base_url = telegram.get("base_url")
    if raw_base_url:
        text = _required_string(raw_base_url, "telegram.base_url")
        urls = []
        for part in text.split(","):
            url = part.strip().rstrip("/")
            if not url:
                continue
            if not url.startswith(("http://", "https://")):
                raise ConfigError("telegram.base_url: %r is not an http(s) URL"
                                  % url)
            urls.append(url)
        if not urls:
            raise ConfigError("telegram.base_url: no URLs found")
        tele_base_urls = tuple(urls)

    listen, wh_path, auth = _webhook(raw.get("webhook"))
    chats = _chats(raw.get("chats"))

    default_chat = str(telegram.get("default_chat") or "default").strip()
    if default_chat not in chats:
        raise ConfigError("telegram.default_chat: %r is not among chats %s"
                          % (default_chat, ", ".join(sorted(chats))))

    burst_interval = _number(defaults.get("burst_interval_secs"),
                             "defaults.burst_interval_secs", 1.5, positive=True)

    video_format = _choice(defaults.get("video_format"), "defaults.video_format",
                           "mp4", FORMATS)
    video_fps = _video_fps(defaults.get("video_fps"), "defaults.video_fps")
    if video_format == "gif" and video_fps == FPS_PASSTHROUGH:
        raise ConfigError("defaults.video_fps: 'pass' keeps the recording's own "
                          "timestamps and cannot be used with video_format = "
                          "'gif'; set a frame rate instead")

    return Config(
        data_folder=str(raw.get("data_folder") or "/data"),
        ffmpeg_working_folder=str(raw.get("ffmpeg_working_folder") or "/clips"),
        timezone=tz,
        log_level=log_level,
        processing_threads=_integer(raw.get("processing_threads"),
                                    "processing_threads", 1, positive=True),
        ffmpeg_hwaccel=_choice(raw.get("ffmpeg_hwaccel"), "ffmpeg_hwaccel",
                               "auto", HWACCEL_MODES),
        synology_base_api_url=_required_string(
            synology.get("base_api_url"), "synology.base_api_url").rstrip("/"),
        synology_user=_required_string(synology.get("user"), "synology.user"),
        synology_password=_required_string(synology.get("password"), "synology.password"),
        syno_verify_ssl=_boolean(synology.get("verify_ssl"), "synology.verify_ssl", False),
        syno_timeout_connect=_number(synology.get("timeout_connect"),
                                     "synology.timeout_connect", 10, positive=True),
        syno_timeout_read=_number(synology.get("timeout_read"),
                                  "synology.timeout_read", 60, positive=True),
        webhook_listen=listen,
        webhook_path=wh_path,
        webhook_auth=auth,
        tele_base_urls=tele_base_urls,
        proxy=telegram.get("proxy") or None,
        tele_connect_timeout=_number(telegram.get("connect_timeout"),
                                     "telegram.connect_timeout", 20, positive=True),
        tele_read_timeout=_number(telegram.get("read_timeout"),
                                  "telegram.read_timeout", 60, positive=True),
        tele_send_attempts=attempts,
        tele_retry_backoff_secs=_number(telegram.get("retry_backoff_secs"),
                                        "telegram.retry_backoff_secs", 5, positive=True),
        default_chat=default_chat,
        chats=chats,
        episode_gap_secs=_number(defaults.get("episode_gap_secs"), "defaults.episode_gap_secs",
                                 60, positive=True),
        cooldown_secs=_number(defaults.get("cooldown_secs"), "defaults.cooldown_secs", 30, minimum=0),
        max_photos_per_episode=_integer(defaults.get("max_photos_per_episode"),
                                        "defaults.max_photos_per_episode", 0, minimum=0),
        burst_photos=_integer(defaults.get("burst_photos"), "defaults.burst_photos", 3, minimum=0),
        burst_interval_secs=burst_interval,
        burst_skip_first_secs=_number(defaults.get("burst_skip_first_secs"),
                                      "defaults.burst_skip_first_secs", 2.0, minimum=0),
        subsequent_photos=_integer(defaults.get("subsequent_photos"),
                                   "defaults.subsequent_photos", 1, minimum=0),
        photo_max_side=_integer(defaults.get("photo_max_side"), "defaults.photo_max_side",
                                1280, positive=True),
        photo_quality=_integer(defaults.get("photo_quality"), "defaults.photo_quality", 2, minimum=2),
        video_skip_first_n_secs=_number(defaults.get("video_skip_first_n_secs"),
                                        "defaults.video_skip_first_n_secs", 0, minimum=0),
        video_max_length_secs=_number(defaults.get("video_max_length_secs"),
                                      "defaults.video_max_length_secs", 8, positive=True),
        video_max_height=_integer(defaults.get("video_max_height"), "defaults.video_max_height",
                                  480, positive=True),
        video_fps=video_fps,
        video_format=video_format,
        video_preset=str(defaults.get("video_preset") or "veryfast"),
        use_snapshot_api=_snapshot_mode(defaults.get("use_snapshot_api"),
                                        "defaults.use_snapshot_api", "auto"),
        recording_stream=_choice(defaults.get("recording_stream"),
                                 "defaults.recording_stream", "auto",
                                 {"auto", "main", "dual"}),
    )
