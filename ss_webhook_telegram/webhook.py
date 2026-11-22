"""HTTP receiver for Surveillance Station Action Rule -> Webhook.

Accepts a POST on a configured path (Basic auth optional), parses the JSON body
and hands the event to the caller's callback. GET answers 200 as a no-op,
because SS "AUTO" tries GET first.
"""
from __future__ import annotations

import hmac
import json
import logging
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

MAX_BODY_BYTES = 64 * 1024

# Well-known keys, plus tolerant aliases for the camera name. Any other key in
# the body is ignored.
KEY_ALIASES = {
    "camera": "camera",
    "device": "camera",
    "device_name": "camera",
    "name": "camera",
    "time": "time",
    "event_time": "time",
    "chat": "chat",
    "config": "config",
}


def _clean(value):
    """Drop a value that is still an unsubstituted %TOKEN% or empty."""
    if value is None:
        return None
    if not isinstance(value, str):
        return value
    value = value.strip()
    if not value or (value.startswith("%") and value.endswith("%")):
        return None
    return value


def _normalize(raw):
    payload = {}
    for key, value in raw.items():
        target = KEY_ALIASES.get(str(key).strip().lower())
        if target is None:
            logging.debug("webhook: ignoring unknown key %r", key)
            continue
        if target == "config":
            payload["config"] = value if isinstance(value, dict) else None
        else:
            payload[target] = _clean(value)
    return payload


def parse_payload(body):
    """Parse a JSON webhook body into a normalized dict, or raise ValueError."""
    text = body.decode("utf-8", "replace").strip()
    if not text:
        raise ValueError("empty body")
    if text[:1] not in "{[":
        raise ValueError("body is not JSON: %r" % text[:200])

    try:
        raw = json.loads(text)
    except json.JSONDecodeError as e:
        raise ValueError("invalid JSON: %s" % e)
    if not isinstance(raw, dict):
        raise ValueError("JSON body is not an object")
    return _normalize(raw)


def _authorized(config, header):
    auth = config.webhook_auth
    if auth is None:
        return True
    if not header or not header.startswith("Basic "):
        return False
    import base64
    try:
        decoded = base64.b64decode(header[6:]).decode("utf-8")
    except (ValueError, UnicodeDecodeError):
        return False
    user, _, password = decoded.partition(":")
    return (hmac.compare_digest(user, auth.user)
            and hmac.compare_digest(password, auth.password))


def _make_handler(config, on_payload):
    class Handler(BaseHTTPRequestHandler):
        server_version = "ss_webhook_telegram"
        protocol_version = "HTTP/1.1"

        def _reply(self, code, text="ok"):
            body = text.encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _path_ok(self):
            path = self.path.split("?", 1)[0]
            return path == config.webhook_path

        def do_GET(self):  # SS "AUTO" probes with GET first
            if not self._path_ok():
                self._reply(404, "not found")
                return
            self._reply(200, "ok")

        def do_POST(self):
            if not self._path_ok():
                self._reply(404, "not found")
                return
            if not _authorized(config, self.headers.get("Authorization")):
                logging.warning("webhook: rejected request from %s (bad auth)",
                                self.client_address[0])
                self._reply(401, "unauthorized")
                return
            try:
                length = int(self.headers.get("Content-Length") or 0)
            except ValueError:
                self._reply(400, "bad length")
                return
            if length <= 0 or length > MAX_BODY_BYTES:
                self._reply(400, "bad length")
                return
            body = self.rfile.read(length)
            try:
                payload = parse_payload(body)
            except ValueError as e:
                logging.warning("webhook: unparsable body from %s: %s",
                                self.client_address[0], e)
                self._reply(400, "bad payload")
                return
            if not payload.get("camera"):
                logging.warning("webhook: payload without a camera name: %r", payload)
                self._reply(400, "no camera")
                return
            payload["_received_at"] = time.time()
            on_payload(payload)
            self._reply(200, "ok")

        def log_message(self, fmt, *args):
            logging.debug("webhook %s - %s", self.address_string(), fmt % args)

    return Handler


def serve(config, on_payload):
    """Build (but do not start) the HTTP server for config.webhook_listen."""
    host, _, port = config.webhook_listen.rpartition(":")
    if not host:
        host = "0.0.0.0"
    try:
        port = int(port)
    except ValueError:
        raise ValueError("webhook.listen must be host:port, got %r"
                         % config.webhook_listen)
    httpd = ThreadingHTTPServer((host, port), _make_handler(config, on_payload))
    httpd.daemon_threads = True
    return httpd
