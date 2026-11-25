"""Thin client for the Synology Surveillance Station Web API.

Reused from the polling service (login / Camera.List / Recording.List /
Recording.Download with CMS redirect) and extended for the webhook flow:

* find_record_at(...)    - the recording that covers a webhook event time;
* find_record_after(...) - the next recording that starts after it. Together
  they let the caller stitch two chunks of a continuous recording: Surveillance
  Station cuts every continuous run into `recordTime` chunks (1800 s measured),
  and Recording.Download never crosses that seam.

Notes verified against a live SS 9.x host:

* events on a recording server (dsId > 0) must be downloaded through
  SYNO.SurveillanceStation.CMS Redirect; a direct Download returns error 117
  even with mountId set (mountId is always 0 for camera events there);
* Recording.List v5 takes dualRecModeList=0, which hides the chunks of a
  continuous dual recording (dual_rec_mode=1); Events with that flag are also
  skipped defensively in code;
* offset/limit pagination works and data.total gives the full count;
* login accepts POST, so the password never has to sit in a URL.
"""
from __future__ import annotations

import json
import logging
import threading
from dataclasses import dataclass
import requests
import urllib3

AUTH_ERROR_CODES = {105, 106, 107, 119}
EVENT_PAGE_SIZE = 100
DOWNLOAD_CHUNK_SIZE = 1 << 16
DOWNLOAD_ERROR_MAX = 64 * 1024
SNAPSHOT_PATH = "/webapi/SurveillanceStation/ThirdParty/SnapShot/Take/v1"
# Recording.List takes dualRecModeList: 0 is the main/event list, 1 the
# low-profile (dual) chunks of a continuous recording.
SOURCE_MAIN = 0
SOURCE_DUAL = 1

ERROR_MESSAGES = {
    105: "no permission or session expired",
    106: "session invalid",
    107: "session expired",
    119: "session expired",
    117: "recording belongs to another recording server",
    400: "invalid parameter",
    401: "camera disabled",
    402: "camera disabled",
    403: "no permission for this action",
    407: "ip temporarily blocked by DSM",
}


class SynologyError(Exception):
    """Base error for Synology calls."""


class SynologyTransientError(SynologyError):
    """Network, 5xx or unparsable response: the same call may succeed later."""


class SynologyApiError(SynologyError):
    """The API answered success=false, or an HTTP 4xx."""

    def __init__(self, code, context=""):
        self.code = code
        message = ERROR_MESSAGES.get(code, "error code %s" % code)
        if context:
            message = "%s (%s)" % (message, context)
        super().__init__(message)


@dataclass(frozen=True)
class Event:
    """One recording as returned by Recording.List."""

    id: int
    camera_id: int
    ds_id: int
    start_time: float
    stop_time: float
    recording: bool
    dual_rec_mode: int
    width: int = 0
    height: int = 0
    size_bytes: int = 0
    complete: bool = True
    codec: str = ""

    @property
    def duration(self):
        return self.stop_time - self.start_time

    def is_complete(self, min_duration):
        """True when the recording is finished, or already long enough to cut."""
        return not self.recording or self.duration >= min_duration

    def covers(self, timestamp):
        return self.start_time <= timestamp <= self.stop_time


def _parse_event(raw):
    try:
        # `is_complete` is absent on older builds; there a record that is not
        # flagged `recording` is final. Defaulting to True keeps the chunk
        # planner from treating every old record as still being written.
        complete = raw.get("is_complete")
        return Event(
            id=int(raw["id"]),
            camera_id=int(raw["cameraId"]),
            ds_id=int(raw.get("dsId") or 0),
            start_time=float(raw.get("startTime") or 0),
            stop_time=float(raw.get("stopTime") or 0),
            recording=bool(raw.get("recording")),
            dual_rec_mode=int(raw.get("dual_rec_mode") or 0),
            width=int(raw.get("imgWidth") or 0),
            height=int(raw.get("imgHeight") or 0),
            size_bytes=int(raw.get("event_size_bytes") or 0),
            complete=True if complete is None else bool(complete),
            codec=str(raw.get("videoCodec") or ""),
        )
    except (KeyError, TypeError, ValueError):
        logging.warning("Skipping malformed event record: %r", raw)
        return None


def _json_error_code(body):
    try:
        data = json.loads(body.decode("utf-8", "replace"))
    except (ValueError, AttributeError):
        return None
    if isinstance(data, dict):
        return _error_code(data)
    return None


def _error_code(data):
    """Error code of an API reply, tolerating a missing/mistyped error object."""
    error = data.get("error")
    if isinstance(error, dict):
        return error.get("code")
    return None


class SurveillanceClient:
    """One logged-in session, shared by all threads.

    requests.Session is not safe for concurrent use, so each thread gets its
    own; the sid and the re-login are shared under a lock.
    """

    def __init__(self, base_url, user, password, *, connect_timeout=10,
                 read_timeout=60, verify=True):
        base_url = base_url.rstrip("/")
        self._base_url = base_url
        self._auth_url = base_url + "/webapi/auth.cgi"
        self._entry_url = base_url + "/webapi/entry.cgi"
        self._snapshot_url = base_url + SNAPSHOT_PATH
        self._user = user
        self._password = password
        self._timeout = (connect_timeout, read_timeout)
        self._verify = verify
        self._lock = threading.RLock()
        self._local = threading.local()
        self._sid = None

    def _session(self):
        session = getattr(self._local, "session", None)
        if session is None:
            session = requests.Session()
            self._local.session = session
        return session

    def _ensure_sid(self):
        with self._lock:
            if self._sid is None:
                self.login()
            return self._sid

    def _invalidate_sid(self):
        with self._lock:
            self._sid = None

    def login(self):
        """Authenticate and store the sid. Re-login is safe to call again."""
        with self._lock:
            params = {
                "api": "SYNO.API.Auth",
                "method": "login",
                "version": 6,
                "session": "SurveillanceStation",
                "format": "cookie",
                "account": self._user,
                "passwd": self._password,
            }
            try:
                response = self._session().post(self._auth_url, data=params,
                                                timeout=self._timeout,
                                                verify=self._verify)
            except requests.RequestException as e:
                raise SynologyTransientError("login: %s: %s" % (type(e).__name__, e)) from e
            if response.status_code >= 400:
                raise SynologyApiError(response.status_code, "login HTTP")
            try:
                data = response.json()
            except ValueError:
                raise SynologyTransientError("login returned a non-JSON response")
            if not data.get("success"):
                raise SynologyApiError(_error_code(data), "login")
            self._sid = data["data"]["sid"]
            logging.info("Synology login OK")

    def logout(self):
        with self._lock:
            if self._sid is None:
                return
            try:
                self._session().get(self._auth_url, params={
                    "api": "SYNO.API.Auth", "method": "logout", "version": 6,
                    "session": "SurveillanceStation", "_sid": self._sid,
                }, timeout=self._timeout, verify=self._verify)
            except requests.RequestException as e:
                logging.debug("Logout failed: %s", e)
            self._sid = None

    def _call(self, params):
        """GET entry.cgi and return the parsed JSON on success.

        A stale session (105/106/107/119) triggers exactly one re-login and one
        retry; other API errors are raised to the caller.
        """
        for attempt in (1, 2):
            query = dict(params)
            query["_sid"] = self._ensure_sid()
            try:
                response = self._session().get(self._entry_url, params=query,
                                               timeout=self._timeout,
                                               verify=self._verify)
            except requests.RequestException as e:
                raise SynologyTransientError("%s: %s" % (type(e).__name__, e)) from e
            if response.status_code >= 500:
                raise SynologyTransientError("HTTP %i from Surveillance Station"
                                             % response.status_code)
            if response.status_code >= 400:
                raise SynologyApiError(response.status_code, "HTTP error")
            try:
                data = response.json()
            except ValueError:
                raise SynologyTransientError("Surveillance Station returned non-JSON")
            if data.get("success"):
                return data
            code = _error_code(data)
            if code in AUTH_ERROR_CODES and attempt == 1:
                logging.info("Synology session expired (code %s), logging in again", code)
                self._invalidate_sid()
                continue
            raise SynologyApiError(code)
        raise SynologyTransientError("request failed after re-login")

    @staticmethod
    def _payload(data):
        payload = data.get("data")
        if not isinstance(payload, dict):
            raise SynologyTransientError("response without a data object")
        return payload

    def cameras(self):
        data = self._call({
            "api": "SYNO.SurveillanceStation.Camera",
            "method": "List",
            "version": 1,
        })
        return self._payload(data).get("cameras") or []

    def _list_page(self, camera_id, src_type, src_id, limit, offset,
                   source=SOURCE_MAIN):
        data = self._call({
            "api": "SYNO.SurveillanceStation.Recording",
            "method": "List",
            "version": 5,
            "locked": 0,
            "dualRecModeList": source,
            "limit": limit,
            "offset": offset,
            "cameraIds": camera_id,
            "evtSrcType": src_type,
            "evtSrcId": src_id,
        })
        payload = self._payload(data)
        return payload.get("events") or [], int(payload.get("total") or 0)

    def list_recordings(self, camera_id, src_type, src_id, *, source=SOURCE_MAIN,
                        limit=EVENT_PAGE_SIZE, offset=0, memo=None):
        """One page of recordings, newest first.

        `memo` (a dict owned by one planning pass) deduplicates the calls:
        find_record_at() and find_record_after() read the same first page, and
        a caller that retries cannot see a stale index because the memo never
        outlives the pass it was created for.
        """
        key = (source, offset, limit)
        if memo is not None and key in memo:
            return memo[key]
        raw, total = self._list_page(camera_id, src_type, src_id, limit, offset,
                                     source)
        events = [e for e in (_parse_event(r) for r in raw)
                  if e is not None and e.camera_id == camera_id]
        result = (events, total)
        if memo is not None:
            memo[key] = result
        return result

    def find_record_at(self, camera_id, src_type, src_id, timestamp,
                       *, source=SOURCE_MAIN, memo=None, max_pages=5):
        """Newest recording that covers `timestamp`, or None.

        Recording.List returns newest first, so the first recording that
        contains the timestamp is the right one; scanning stops once records
        are entirely before the timestamp.

        The index lags the writer: a live recording's `stop_time` can still be
        behind an event that is already on disk (Recording.Download serves the
        material as it is written). A live recording that starts at or before
        the timestamp therefore counts as covering it, even when `covers()`
        says otherwise; the caller verifies the downloaded fragment and retries
        if the material is not there yet.
        """
        offset = 0
        for _ in range(max_pages):
            page, total = self.list_recordings(camera_id, src_type, src_id,
                                               source=source, offset=offset,
                                               memo=memo)
            if not page:
                return None
            for event in page:
                if event.covers(timestamp):
                    return event
                if event.recording and event.start_time <= timestamp:
                    return event
                if event.stop_time < timestamp:
                    # Newest first: everything after this is even older.
                    return None
            if offset + EVENT_PAGE_SIZE >= total:
                return None
            offset += EVENT_PAGE_SIZE
        return None

    def find_record_after(self, camera_id, src_type, src_id, timestamp,
                          *, source=SOURCE_MAIN, memo=None, max_pages=5):
        """Earliest recording that starts after `timestamp`, or None.

        Used to tell a seam (the next chunk starts exactly where the previous
        one stopped) from a real gap in the recording: Download never crosses
        a chunk boundary, so the planner needs the next chunk to stitch.
        """
        candidate = None
        offset = 0
        for _ in range(max_pages):
            page, total = self.list_recordings(camera_id, src_type, src_id,
                                               source=source, offset=offset,
                                               memo=memo)
            if not page:
                return candidate
            for event in page:
                if event.start_time >= timestamp:
                    # Newest first: the last one still after the timestamp is
                    # the closest one to it. A chunk that starts exactly at the
                    # timestamp is the next chunk of a seam, not the current one
                    # (find_record_at already returned the chunk covering it).
                    candidate = event
                    continue
                return candidate
            if offset + EVENT_PAGE_SIZE >= total:
                return candidate
            offset += EVENT_PAGE_SIZE
        return candidate

    def find_recording(self, camera_id, src_type, src_id, timestamp, **kwargs):
        """Deprecated alias: use find_record_at()."""
        return self.find_record_at(camera_id, src_type, src_id, timestamp,
                                   **kwargs)

    def latest_event(self, camera_id, src_type, src_id):
        """Newest event of a camera (kept for diagnostics/CLI)."""
        page, _ = self._list_page(camera_id, src_type, src_id, 1, 0)
        for raw in page:
            event = _parse_event(raw)
            if event is not None and event.camera_id == camera_id:
                return event
        return None

    def events(self, camera_id, src_type, src_id, since_id):
        """All events newer than since_id, oldest first (kept for diagnostics)."""
        collected = []
        offset = 0
        while True:
            page, total = self._list_page(camera_id, src_type, src_id,
                                          EVENT_PAGE_SIZE, offset)
            reached_checkpoint = False
            for raw in page:
                event = _parse_event(raw)
                if event is None or event.camera_id != camera_id:
                    continue
                if event.id <= since_id:
                    reached_checkpoint = True
                    break
                collected.append(event)
            if (reached_checkpoint or len(page) < EVENT_PAGE_SIZE
                    or offset + EVENT_PAGE_SIZE >= total):
                break
            offset += EVENT_PAGE_SIZE
        collected.sort(key=lambda event: event.id)
        return collected

    def _download_params(self, event, offset_time_ms, play_time_ms, sid):
        if event.ds_id > 0:
            web_api = json.dumps({
                "api": "SYNO.SurveillanceStation.Recording",
                "version": 6,
                "method": "Download",
                "id": event.id,
                "offsetTimeMs": offset_time_ms,
                "playTimeMs": play_time_ms,
            }, separators=(",", ":"))
            return {
                "api": "SYNO.SurveillanceStation.CMS",
                "method": "Redirect",
                "version": 1,
                "dsId": event.ds_id,
                "webAPI": web_api,
                "isDownloadFile": "true",
                "_sid": sid,
            }
        return {
            "api": "SYNO.SurveillanceStation.Recording",
            "method": "Download",
            "version": 6,
            "id": event.id,
            "offsetTimeMs": offset_time_ms,
            "playTimeMs": play_time_ms,
            "_sid": sid,
        }

    def download(self, event, offset_time_ms, play_time_ms, out_path):
        """Stream a partial recording into out_path.

        The API answers with JSON (not video) when something is wrong; that is
        detected from the Content-Type and the first byte and raised, so an
        error body never ends up in the .mp4 file.
        """
        for attempt in (1, 2):
            params = self._download_params(event, offset_time_ms, play_time_ms,
                                           self._ensure_sid())
            try:
                response = self._session().get(self._entry_url, params=params,
                                               stream=True, timeout=self._timeout,
                                               verify=self._verify)
            except requests.RequestException as e:
                raise SynologyTransientError("download event %i: %s: %s"
                                             % (event.id, type(e).__name__, e)) from e
            try:
                if response.status_code >= 500:
                    raise SynologyTransientError("download event %i: HTTP %i"
                                                 % (event.id, response.status_code))
                if response.status_code >= 400:
                    raise SynologyApiError(response.status_code, "download event %i" % event.id)

                content_type = (response.headers.get("Content-Type") or "").lower()
                # Read the raw stream, not Response.iter_content(): that one
                # decodes into an internal buffer and drops the tail of a body
                # that ends early, which is exactly the case handled below.
                stream = response.raw
                declared = response.headers.get("Content-Length")
                try:
                    first = stream.read(DOWNLOAD_CHUNK_SIZE, decode_content=False)
                except urllib3.exceptions.HTTPError as e:
                    raise SynologyTransientError(
                        "download event %i: %s: %s"
                        % (event.id, type(e).__name__, e)) from e

                if "json" in content_type or first.lstrip()[:1] == b"{":
                    body = first
                    while len(body) <= DOWNLOAD_ERROR_MAX:
                        try:
                            chunk = stream.read(DOWNLOAD_CHUNK_SIZE,
                                                decode_content=False)
                        except urllib3.exceptions.HTTPError:
                            break
                        if not chunk:
                            break
                        body += chunk
                    code = _json_error_code(body)
                    if code in AUTH_ERROR_CODES and attempt == 1:
                        logging.info("Synology session expired during download (code %s)", code)
                        self._invalidate_sid()
                        continue
                    if code is None:
                        raise SynologyTransientError(
                            "download event %i: unparsable error body %r"
                            % (event.id, body[:200]))
                    raise SynologyApiError(code, "download event %i" % event.id)

                written = 0
                with open(out_path, "wb") as out:
                    if first:
                        out.write(first)
                        written += len(first)
                    while True:
                        try:
                            chunk = stream.read(DOWNLOAD_CHUNK_SIZE,
                                                decode_content=False)
                        except urllib3.exceptions.HTTPError as e:
                            # Surveillance Station can declare a body a few bytes
                            # longer than it sends (measured: exactly 100 bytes,
                            # reproducibly, also on a closed file). What arrived
                            # is a valid fragment; the caller checks its
                            # duration, so keep it instead of losing the event.
                            if written == 0:
                                raise SynologyTransientError(
                                    "download event %i: %s: %s"
                                    % (event.id, type(e).__name__, e)) from e
                            logging.debug(
                                "download event %i: short body %i/%s bytes (%s); "
                                "keeping the fragment", event.id, written,
                                declared or "?", type(e).__name__)
                            break
                        if not chunk:
                            break
                        out.write(chunk)
                        written += len(chunk)
                if written == 0:
                    raise SynologyTransientError("download event %i: empty body" % event.id)
                if declared and declared.isdigit() and int(declared) != written:
                    logging.debug("download event %i: %i of %s bytes", event.id,
                                  written, declared)
                logging.info("Downloaded event %i: %i bytes", event.id, written)
                return written
            finally:
                response.close()
        raise SynologyTransientError("download event %i failed after re-login" % event.id)

    # -- live snapshot ----------------------------------------------------

    def take_snapshot(self, cam_id, at=None):
        """JPEG snapshot (profileType=0, download=true, save=false).

        Without ``at`` this is the live frame. With ``at`` (epoch seconds) SS
        returns the frame of the *recording* at that second instead - the
        recorded snapshot stream, not the latest key frame - which is what a
        follow-up photo needs. ``at`` is sent as epoch seconds: this API build
        answers an ISO 8601 string with error 400. A time still in the future,
        or one not covered by a recording, is also error 400.

        The live frame works while Surveillance Station processes the camera
        codec server-side; after DSM 7.2.2 it fails for H.265 cameras, so the
        caller may fall back to recording frames.
        """
        url = self._snapshot_url
        params = {"camId": cam_id, "profileType": 0, "download": "true",
                  "save": "false"}
        if at is not None:
            params["time"] = int(at)
        for attempt in (1, 2):
            query = dict(params)
            query["_sid"] = self._ensure_sid()
            try:
                response = self._session().get(url, params=query,
                                               timeout=self._timeout,
                                               verify=self._verify)
            except requests.RequestException as e:
                raise SynologyTransientError("snapshot cam %s: %s: %s"
                                             % (cam_id, type(e).__name__, e)) from e
            if response.status_code >= 500:
                raise SynologyTransientError("snapshot cam %s: HTTP %i"
                                             % (cam_id, response.status_code))
            if response.status_code >= 400:
                raise SynologyApiError(response.status_code, "snapshot cam %s" % cam_id)
            content = response.content
            content_type = (response.headers.get("Content-Type") or "").lower()
            if content.lstrip()[:1] == b"{" or "json" in content_type:
                code = _json_error_code(content)
                if code in AUTH_ERROR_CODES and attempt == 1:
                    logging.info("Synology session expired during snapshot (code %s)", code)
                    self._invalidate_sid()
                    continue
                if code is not None:
                    raise SynologyApiError(code, "snapshot cam %s" % cam_id)
                raise SynologyTransientError("snapshot cam %s: JSON body %r"
                                             % (cam_id, content[:200]))
            if not content:
                raise SynologyTransientError("snapshot cam %s: empty body" % cam_id)
            return content
        raise SynologyTransientError("snapshot cam %s failed after re-login" % cam_id)
