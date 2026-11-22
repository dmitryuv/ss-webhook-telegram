"""SQLite state: webhook dedup keys and per-camera episode bookkeeping.

Two tables:

* dedup     - one row per seen (camera, event-second). Survives restarts and
              lets repeated deliveries of the same event be ignored;
* episodes  - the current episode of a camera: when it started and how many
              photos were sent (for the cooldown and the per-episode cap);
* camera_gop - the measured key-frame interval of a camera, used to size the
              follow-up window (learned from the burst fragment).

WAL + a lock keep it safe for the HTTP thread and the per-camera workers.
"""
from __future__ import annotations

import datetime
import sqlite3
import threading
from dataclasses import dataclass
from pathlib import Path

SCHEMA = """
CREATE TABLE IF NOT EXISTS dedup (
    key          TEXT PRIMARY KEY,
    processed_at timestamp NOT NULL
);
CREATE TABLE IF NOT EXISTS episodes (
    camera       TEXT PRIMARY KEY,
    episode_start real NOT NULL,
    last_event   real NOT NULL,
    photos_sent  integer NOT NULL DEFAULT 0,
    last_photo   real NOT NULL DEFAULT 0,
    updated_at   timestamp NOT NULL
);
CREATE TABLE IF NOT EXISTS camera_gop (
    camera     TEXT PRIMARY KEY,
    gop        REAL NOT NULL,
    updated_at timestamp NOT NULL
);
"""


@dataclass
class Episode:
    camera: str
    episode_start: float
    last_event: float
    photos_sent: int
    last_photo: float

    @classmethod
    def from_row(cls, row):
        return cls(
            camera=row["camera"],
            episode_start=float(row["episode_start"]),
            last_event=float(row["last_event"]),
            photos_sent=int(row["photos_sent"]),
            last_photo=float(row["last_photo"]),
        )


def _now():
    """Timestamp as ISO text.

    SQLite's default datetime adapter is deprecated since Python 3.12, so the
    value is formatted here. `isoformat(sep=" ")` yields exactly the
    "YYYY-MM-DD HH:MM:SS.ffffff" string the old adapter wrote, so the
    comparison in prune_dedup() still works on rows written before the change.
    """
    return datetime.datetime.now().isoformat(sep=" ")


class EventStore:
    def __init__(self, data_folder):
        Path(data_folder).mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(
            str(Path(data_folder) / "events.db"),
            check_same_thread=False,
        )
        self._conn.row_factory = sqlite3.Row
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA busy_timeout=5000")
            self._conn.executescript(SCHEMA)
            # Older databases carried the destination chat and the send-mode
            # bookkeeping; neither is used any more, so drop the columns. A
            # fresh database does not have them and the statements fail quietly.
            for column in ("chat", "video_sent", "clip_json"):
                try:
                    self._conn.execute("ALTER TABLE episodes DROP COLUMN " + column)
                except sqlite3.OperationalError:
                    pass
            self._conn.commit()

    # -- dedup ------------------------------------------------------------

    def mark_seen(self, key):
        """Claim a dedup key atomically.

        Returns True for the first caller and False for every repeat, even
        when several camera threads process the same event at once.
        """
        with self._lock:
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO dedup(key, processed_at) VALUES(?,?)",
                (key, _now()))
            self._conn.commit()
            return cur.rowcount > 0

    def prune_dedup(self, keep_secs):
        """Drop dedup keys older than keep_secs (housekeeping)."""
        cutoff = (datetime.datetime.now()
                  - datetime.timedelta(seconds=keep_secs)).isoformat(sep=" ")
        with self._lock:
            cur = self._conn.execute("DELETE FROM dedup WHERE processed_at < ?",
                                     (cutoff,))
            self._conn.commit()
            return cur.rowcount

    # -- episodes ---------------------------------------------------------

    def get_episode(self, camera):
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM episodes WHERE camera=?", (camera,)).fetchone()
        return Episode.from_row(row) if row else None

    def save_episode(self, episode):
        with self._lock:
            self._conn.execute(
                """INSERT INTO episodes(camera, episode_start, last_event,
                       photos_sent, last_photo, updated_at)
                   VALUES(?,?,?,?,?,?)
                   ON CONFLICT(camera) DO UPDATE SET
                       episode_start=excluded.episode_start,
                       last_event=excluded.last_event,
                       photos_sent=excluded.photos_sent,
                       last_photo=excluded.last_photo,
                       updated_at=excluded.updated_at""",
                (episode.camera, episode.episode_start, episode.last_event,
                 episode.photos_sent, episode.last_photo,
                 _now()))
            self._conn.commit()

    # -- learned per-camera key-frame interval ----------------------------

    def gop(self, camera, default):
        """Measured key-frame interval for a camera, or `default` if unknown."""
        with self._lock:
            row = self._conn.execute(
                "SELECT gop FROM camera_gop WHERE camera=?", (camera,)).fetchone()
        return float(row["gop"]) if row else default

    def set_gop(self, camera, gop):
        with self._lock:
            self._conn.execute(
                """INSERT INTO camera_gop(camera, gop, updated_at) VALUES(?,?,?)
                   ON CONFLICT(camera) DO UPDATE SET
                       gop=excluded.gop, updated_at=excluded.updated_at""",
                (camera, gop, _now()))
            self._conn.commit()

    def close(self):
        with self._lock:
            self._conn.close()
