# ss-webhook-telegram

Turn Synology Surveillance Station motion events into **Telegram photo bursts and
one short video clip per episode**, using the station's built-in *Action Rule ->
Webhook* action.

It is event-driven: Surveillance Station pushes a webhook, the service decides
what to send and when. There is no polling loop and no email relay in the
middle.

```
SS Action Rule (one per camera)
      | POST JSON  http://<host>:8099/webhook      (Basic auth optional)
      v
webhook.py   - HTTP receiver, payload parser, auth
      | per-camera queue (one thread per camera: a burst of one camera never delays another)
      v
episodes + dedup (SQLite)
      | one recording window per event
      v
Recording.List -> Recording.Download -> processing pool: one ffmpeg pass
      |                                   (clip + photo frames)    (N threads)
      +-------------> send queue (3 threads) <---------+
                            |
                            v
                       Telegram (album, then video)
```

## Why it exists

Typical home setups do one of two things: they let Surveillance Station send an
email with a snapshot and run an `SMTP -> Telegram` bridge, or they run a
poller that repeatedly asks the recording API for new events. The first gives
you one low-quality picture per mail, the second wastes API calls and is always
late.

This service takes the third route: Surveillance Station calls *you* the moment
something happens, and the service is free to build a proper episode out of it
- a burst of photos cut from the recording, sparse follow-up photos, and a
single trimmed clip, delivered as ordinary Telegram messages.

## Highlights

- **Push, not poll.** The only trigger is the Action Rule webhook. No timers,
  no event backlog to catch up with.
- **No cameras in the config.** The rule sends `%DEVICE_NAME%`; the service
  resolves it to a camera id (and its recording server) through `Camera.List`.
- **Episodes.** Events of one camera with pauses shorter than
  `episode_gap_secs` belong to one episode: a burst on the first event, sparse
  follow-ups with a cooldown, and exactly one video clip.
- **Photos are frames of the recording.** One download and one ffmpeg pass
  produce both the clip and the photo frames, so a burst costs no extra decode.
  Nothing depends on Surveillance Station snapshots, so the service keeps
  working for H.265 as well, where the station drops server-side processing
  from DSM 7.2.2 on.
- **Hardware decoding, picked automatically.** The image ships both Intel VAAPI
  drivers, `iHD` and `i965`, and probes them in that order at startup: modern
  kernels get `iHD`, the older Synology kernels where `iHD` cannot initialise
  get `i965`, and a host without a usable GPU quietly decodes in software. No
  configuration, no extra flags - the startup log just says which one is in
  use. It is codec-agnostic, H.264 and H.265 alike; on a real 4 MP H.265
  recording that is about half the time and a third of the CPU of software
  decode. (The clip itself is still encoded in software, which is negligible at
  these sizes.)
- **Album delivery.** A burst arrives as a single Telegram media group instead
  of N separate notifications, sent before the clip.
- **A clip that really starts at the event.** The recording API returns video
  from the previous key frame, not from the requested second. The service asks
  for the fragment starting at the event, measures how far back the key frame
  actually was from the fragment duration, and cuts with an exact `ffmpeg -ss`,
  so the clip begins at `event time + video_skip_first_n_secs` - also when a new
  episode starts inside a recording that has been open for minutes.
- **No reliance on the recording index.** `Recording.List` lags several seconds
  behind the writer; readiness is decided by wall clock plus a length check of
  the downloaded fragment, so the clip is fetched as early as it exists.
- **Isolated cameras.** One thread and one bounded queue per camera for the
  download and the sends; the CPU-bound ffmpeg passes run on a small shared
  processing pool (`processing_threads`), and a Telegram send queue absorbs
  slow uploads. A slow camera, a slow encode or a stuck upload never stalls the
  rest.
- **Several Telegram endpoints.** `base_url` accepts a comma-separated list;
  a failed send rotates to the next endpoint, and the last one that worked
  stays active until it fails.
- **Per-rule tuning.** Every tunable can be overridden from the Action Rule
  body, using the same key names as the config file.
- **Named destinations.** The rule body picks a chat group by name; unknown
  names fall back to `default_chat`.

## Requirements

- Synology Surveillance Station 9.x (tested against 9.3) with the Web API
  reachable over HTTPS, and an account allowed to list cameras and list/download
  recordings. Recording must be enabled for the cameras you want photos from.
- Python 3.11+ and `ffmpeg`/`ffprobe` if you run it outside Docker. The
  provided image already contains both.
- Optional: an Intel iGPU exposed as `/dev/dri` for hardware video decoding.
  The image carries both Intel VAAPI drivers (`iHD` and `i965`) and picks the
  one the host supports; without a usable GPU it decodes in software.
- A Telegram bot and the chat id(s) to deliver to.

## Quick start

1. Copy `config-sample.toml` to `config.toml` and fill in the Synology
   credentials, the Telegram bot token(s) and the chat id(s). See
   [Configuration](#configuration) for every key.
2. Run it:

   ```bash
   docker build -t ss-webhook-telegram .
   docker run -d --name ss-webhook-telegram --restart unless-stopped \
     -p 8099:8099 \
     --device /dev/dri \
     -v "$PWD/config.toml:/config/config.toml:ro" \
     -v ss-webhook-data:/data \
     -v ss-webhook-clips:/clips \
     ss-webhook-telegram
   ```

   `--device /dev/dri` enables hardware video decoding on an Intel iGPU
   (VAAPI). The image ships both VAAPI drivers and picks the one that works, so
   no extra flags are needed. The device is optional: without it (or when the
   GPU is busy) the service decodes in software and says so in the startup log.

   Or with Compose:

   ```yaml
   services:
     ss-webhook-telegram:
       build: .
       restart: unless-stopped
       ports:
         - "8099:8099"
       devices:
         - /dev/dri:/dev/dri
       volumes:
         - ./config.toml:/config/config.toml:ro
         - ./data:/data
         - ./clips:/clips
   ```

3. Check the startup log. It prints the parsed settings, so a typo or a stale
   config is visible immediately:

   ```
   Config: timezone=GMT burst=3@1.5s skip=2.0s subsequent=1 cooldown=30s gap=60s
   Photos: max_side=1280px quality=2 snapshot_api=auto (burst frames come from the recording)
   Video: start=+0.0s length=8.0s max_height=480p fps=15 format=mp4
   FFmpeg: decode=vaapi (iHD, /dev/dri/renderD128), encode=libx264
   Concurrency: camera queues, 1 processing thread(s), 3 telegram senders
   Webhook: listen=0.0.0.0:8099 path=/webhook auth=off
   Chats: default (default=default)
   ```

   The `FFmpeg:` line shows the chosen decode path: `vaapi` with the driver
   that initialised (`iHD` or `i965`) and the device, or `software` when no
   driver works.

4. Create one Action Rule per camera (see
   [Surveillance Station setup](#surveillance-station-action-rule-setup)) and
   use *Test Send* / trigger real motion.

Running without Docker:

```bash
pip install -r requirements.txt
python3 -m ss_webhook_telegram config.toml
```

For a Synology NAS you usually want an amd64 image: `./build.sh` builds
`linux/amd64`, tags it with the version from `ss_webhook_telegram/__init__.py`
plus `:latest`, and saves a `ss_webhook_telegram.tar` for manual upload to the
NAS registry.

## How it works

1. **Receive.** The HTTP server (`webhook.py`) accepts a `POST` on
   `webhook.path`, optionally checks Basic auth, parses the body and puts the
   event on a queue. `GET` answers `200` as a no-op because Surveillance Station
   probes with `GET` first.
2. **Resolve.** `%DEVICE_NAME%` is looked up in a cached `Camera.List` (name
   trimmed and NFC-normalized, case-insensitive fallback). The camera's
   recording server (`ownerDsId`) is what decides whether the recording has to
   be downloaded through a CMS redirect.
3. **Deduplicate.** The key `"<camera>|<int(event time)>"` is claimed atomically
   in SQLite, so a repeated delivery of the same event is ignored - even if two
   threads see it at once.
4. **Episode.** The service keeps the current episode per camera. A pause longer
   than `episode_gap_secs` starts a new one.
5. **Recording window.** The first event of an episode downloads one recording
   window starting at the event; a single ffmpeg pass on the processing pool
   turns it into the photo burst and the clip. Later events send at most
   `subsequent_photos` key frames from a tiny window, never faster than
   `cooldown_secs`.
6. **Send.** The burst goes as one album, then the clip follows immediately,
   both through a small send queue with retries, so a slow or stuck upload does
   not block the rest of the pipeline.

## Event timing and the Action Rule delay

`%EVENT_TIME%` in the rule body is the moment Surveillance Station stamped the
motion event, not the moment the webhook arrives. Two things on the station's
side separate the two:

- **Events come on a fixed per-camera cadence.** While motion continues, the
  camera emits an event on its own detection interval. `%EVENT_TIME%` sits on
  that grid, so it is *not* the exact instant motion began.
- **The rule occasionally fires late.** Most firings land in the same second as
  the event: the webhook is immediate, because the rule itself is what sends it,
  so our receipt time equals the rule's execution time. Now and then the rule
  engine skips a slot and fires ~10-15s after the previous firing, releasing the
  events it held back in one batch. Those are the deliveries logged with
  `SS delivery` of 5-10s.

The service never assumes the webhook is on time:

- readiness is decided by wall clock (`event + window + 1s`, where `window` is
  the longer of the clip and the burst), not by the recording index, so a late
  event does not push the material back further;
- `episode_gap_secs`, `cooldown_secs` and the dedup key all work on
  `%EVENT_TIME%`, so a late delivery still joins the right episode;
- the log splits the lateness into `SS delivery` (station side) and
  `our queue` (our own per-camera queue), so the two are never confused.

To see the station's own view, export **Action Rule History** (SS -> Log) and
compare it with the service log: the rule's execution time matches our `handled`
time, while `%EVENT_TIME%` can be several seconds earlier.

## Photo pipeline

- **Source is the recording.** The burst frames are cut from the same fragment
  as the clip, in the same ffmpeg pass, so a burst adds no extra decode. That is
  also what keeps the burst working for H.265, where Surveillance Station stops
  server-side processing from DSM 7.2.2 on.
- **Skip the trigger.** `burst_skip_first_secs` delays the first frame: the rule
  often fires before the subject really moves.
- **Exact moments.** Frames are taken at
  `event + burst_skip_first_secs + i * burst_interval_secs`: real moments of the
  recording, not key frames. If the burst window reaches past the clip, the
  download simply covers the longer of the two.
- **Follow-ups are key frames.** A later event of an episode has no clip to
  share a decode with, so it downloads a tiny window and decodes key frames
  only (`-skip_frame nokey`, a few times cheaper). The window is
  `(subsequent_photos - 1) * GOP + 1s`; the interval itself is measured in the
  background on the last burst (the largest gap between key frames) and
  remembered per camera in `camera_gop`, with a 4s fallback until the first
  episode. The measurement runs in parallel with the clip pass and the sends,
  so it never delays the event.
- **Or the live Snapshot API.** `use_snapshot_api` picks the follow-up source:
  `auto` tries the live snapshot (a cheap JPEG) and falls back to recording key
  frames if it fails, remembering the failure per camera in memory (a restart
  re-evaluates, e.g. after a codec change); `true` uses snapshots only and logs
  an error instead of falling back; `false` uses recording frames only. The
  burst always comes from the recording (it shares the clip's decode).
- **Size.** Photos are scaled to fit `photo_max_side` on the long side
  (default 1280, the same box Telegram recompresses to) and encoded with
  `-q:v photo_quality`. Note that for `-q:v` **lower is better** (2 = best,
  31 = worst).
- **Delivery.** `burst_photos` > 1 is sent as a media group (chunks of 10).

## Video pipeline

- **One clip per episode**, produced together with the burst from a single
  download and a single decode.
- **Exact cut.** `Recording.Download` snaps the requested offset back to the
  previous key frame, so asking for `event + skip` returns video from up to a
  key-frame interval earlier. The service therefore asks for the fragment
  starting at the event (`[event, event + window]`), measures the fragment
  duration to learn how far before the requested offset the key frame began
  (`snap`), and cuts with `ffmpeg -ss` at exactly
  `event + video_skip_first_n_secs`. Nothing before the event is requested: the
  crop only ever needs material at or after it, so the download is as small as
  the burst and the clip allow. This holds for a long-running recording too,
  i.e. when a new episode starts inside the post-record file of the previous
  one.
- **Readiness.** The recording index (`stop_time` in `Recording.List`) lags the
  writer by several seconds, but `Recording.Download` serves the material as it
  is written. So the first attempt waits for `event + window + 1s` (wall clock),
  and after downloading the service measures the fragment and retries after
  exactly the missing time if it is short. A missing recording retries after 1s.
- **Format.** `mp4` (H.264, `video_preset`, `video_fps`,
  `video_max_height`) or `gif`.
- **No cover frame.** Short videos autoplay in Telegram, so no cover frame is
  attached.
- **Logs.** Every clip logs `lead Xs, snap Ys, fragment As, N frame(s)`,
  which makes both the timing of the cut and the download overhead easy to
  audit.

## Delivery

There is a single delivery mode: every episode is one album of photos followed
by one clip. Both are cut from the same recording fragment.

## Reliability and concurrency

- **Per-camera isolation.** Events are fanned out to a bounded queue and a
  dedicated thread per camera, so events of one camera keep their order while
  different cameras run in parallel.
- **Processing pool.** Decoding the recording and encoding the clip are CPU
  bound, so the ffmpeg passes run on a shared pool of `processing_threads`
  (1 by default). The download and the sends stay in the per-camera thread, so
  cameras overlap their I/O while the CPU work is serialized. A full pool
  blocks the camera, which is the intended backpressure.
- **GPU decode when available.** With `/dev/dri` passed through, the recording
  is decoded on the Intel iGPU (VAAPI). The image ships both drivers and probes
  them in order, so `iHD` is used on modern kernels and `i965` on the older
  Synology kernels where `iHD` cannot initialise; a host without a usable GPU
  falls back to software. On a real 4 MP H.265 recording that is roughly half
  the time and a third of the CPU of software decode, which also removes most
  of the pool serialization; on a small SD stream the gain is naturally
  smaller, since the encode and the scaling dominate there. `ffmpeg_hwaccel`
  controls this; a failed hardware pass is retried in software automatically.
  With HW decoding available it’s safe to increase `processing_threads` value.
- **Telegram send queue.** Uploads go through a bounded queue served by three
  threads with retries (transport errors, 5xx, and `429` with `retry_after`).
  A stuck upload occupies one sender instead of blocking every camera. Queued
  jobs older than 120s (photos) or 600s (video) are dropped as stale.
- **Multiple endpoints.** With several `base_url` values a retry rotates to the
  next one; the endpoint that last succeeded stays active until a failure.
- **Restart safety.** Stale working files older than an hour are removed on
  start; an interrupted episode is simply re-triggered by the next event.
- **Backpressure.** Camera queues, the processing queue and the send queue are
  bounded; if a camera falls far behind, the newest events are dropped with a
  warning, and events older than 60s do not produce photos (the clip is still
  produced). While a camera's episode burst is being produced, its further
  events are dropped at dispatch (they would land inside the cooldown anyway)
  instead of waiting in the camera queue to be skipped.

## Configuration

The format is **TOML**. Start from `config-sample.toml`.

> **TOML gotcha:** the top-level scalars (`data_folder`,
> `ffmpeg_working_folder`, `timezone`, `log_level`) must stay **above** the
> first `[table]` header, otherwise TOML attaches them to that table.

### Top level

| Key | Default | Meaning |
|---|---|---|
| `data_folder` | `/data` | Folder for the SQLite state (`events.db`); must be writable |
| `ffmpeg_working_folder` | `/clips` | Scratch folder for downloads, clips and photos; must be writable |
| `timezone` | `GMT` | Zone used to parse `%EVENT_TIME%` (it carries no zone) and to print logs |
| `log_level` | `INFO` | `DEBUG`, `INFO`, `WARNING` or `ERROR` |
| `processing_threads` | `1` | Concurrent ffmpeg passes (video decode is CPU bound unless the GPU does it) |
| `ffmpeg_hwaccel` | `auto` | Video decode: `auto` (hardware when `/dev/dri` works, otherwise software) \| `vaapi` (require hardware) \| `none` (software only). The driver (`iHD`/`i965`) is picked automatically |

### `[synology]`

| Key | Default | Meaning |
|---|---|---|
| `base_api_url` | required | Surveillance Station base URL, e.g. `https://192.168.0.3:5001` |
| `user` | required | Account used for the Web API |
| `password` | required | Its password |
| `verify_ssl` | `false` | NAS certificates are usually self-signed |
| `timeout_connect` | `10` | Connect timeout, seconds |
| `timeout_read` | `60` | Read timeout, seconds |

### `[webhook]`

| Key | Default | Meaning |
|---|---|---|
| `listen` | `0.0.0.0:8099` | Address the HTTP receiver binds to |
| `path` | `/webhook` | URL path the Action Rule posts to |
| `auth.user`, `auth.password` | - | Optional Basic auth (`[webhook.auth]` table); when present, the rule must send the same credentials |

### `[telegram]`

| Key | Default | Meaning |
|---|---|---|
| `base_url` | Telegram | One base URL, or several comma-separated (retries rotate through them) |
| `proxy` | - | Optional proxy for Telegram, e.g. `socks5h://user:pass@host:port` |
| `connect_timeout` | `20` | Connect timeout, seconds |
| `read_timeout` | `60` | Read (upload) timeout, seconds |
| `send_attempts` | `3` | Attempts per message (hard maximum 3) |
| `retry_backoff_secs` | `5` | Base backoff between attempts (multiplied by the attempt number) |
| `default_chat` | `default` | Chat group used when the body has no/unknown `chat`; must be one of `[[chats]]` |

### `[[chats]]`

One entry per destination. The Action Rule body picks one with `"chat": "<name>"`.

| Key | Meaning |
|---|---|
| `name` | Group name referenced by the rule body |
| `tele_bot_token` | Bot token to send with |
| `tele_chat_id` | Target chat id (group ids are negative) |

### `[defaults]` - tunables

Every key below can be overridden per Action Rule through the webhook body's
`config` object, using the **same key name**. Defaults are shown for reference.

| Key | Default | Meaning |
|---|---|---|
| `episode_gap_secs` | `60` | Pause that closes an episode and opens a new one |
| `cooldown_secs` | `30` | Minimum pause between follow-up photos in an episode, counted from the last send |
| `max_photos_per_episode` | `0` | Cap on follow-up photos per episode (`0` = no cap); the initial burst always goes |
| `burst_photos` | `3` | Frames on the first event of an episode |
| `burst_skip_first_secs` | `2.0` | Delay before the first frame (the rule often fires before the subject moves) |
| `burst_interval_secs` | `1.5` | Pause between the burst frames |
| `subsequent_photos` | `1` | Key frames on each follow-up event (after the cooldown) |
| `photo_max_side` | `1280` | Long-side limit in pixels (matches Telegram's photo cap) |
| `photo_quality` | `2` | JPEG `-q:v`, 2..31 - **lower is better** |
| `use_snapshot_api` | `auto` | Follow-up source: `auto` (snapshot, fall back to frames) \| `true` (snapshot only) \| `false` (frames only) |
| `video_skip_first_n_secs` | `0` | Clip start, seconds after the event |
| `video_max_length_secs` | `8` | Clip length |
| `video_max_height` | `480` | Output height (rounded to an even number) |
| `video_fps` | `15` | Output frame rate |
| `video_format` | `mp4` | `mp4` \| `gif` |
| `video_preset` | `veryfast` | x264 preset |

The download covers `max(video_skip_first_n_secs + video_max_length_secs,
burst_skip_first_secs + (burst_photos - 1) * burst_interval_secs)`, so a long
`video_skip_first_n_secs` also delays the photos.

### Per-rule overrides (payload `config`)

The Action Rule body may carry a `config` object whose keys are exactly the
`[defaults]` keys above, plus `enabled` to turn a rule off:

```json
{
  "config": {
    "burst_photos": 2,
    "cooldown_secs": 15,
    "enabled": true
  }
}
```

Invalid or unknown values are ignored with a warning in the log; for
`video_format` the global default is used.

## Webhook payload contract

The body is JSON. Only `camera` is required.

```json
{
  "camera": "%DEVICE_NAME%",
  "time":   "%EVENT_TIME%",
  "chat":   "test",
  "config": { "cooldown_secs": 30 }
}
```

- `camera` - device name; resolved through `Camera.List`.
- `time` - event time. Parsed in `timezone`; several formats are accepted
  (`YYYY-MM-DDTHH:MM:SS`, `YYYY-MM-DD HH:MM:SS`, `YYYY/MM/DD HH:MM:SS`, or an
  epoch). If it cannot be parsed, the receive time is used with a warning.
- `chat` - optional; defaults to `default_chat`.
- `config` - optional per-rule overrides.

Key aliases are accepted (`device`, `device_name`, `name` for the camera;
`event_time` for the time). Any other key is ignored. Values that are still an
unsubstituted `%TOKEN%`, or empty, become `null`.

## Surveillance Station Action Rule setup

Create one rule per camera (or reuse a template).

1. **Action: Webhook.**
   - Method `POST`, Content-Type `application/json`.
   - URL `http://<host-running-the-service>:8099/webhook`.
   - Username/Password only if `[webhook.auth]` is configured.
   - Body: the JSON template above. Request the ingredients *Device name* and
     *Event time*.
2. **Event.** For direct cameras use *Camera -> Motion Detection*; for
   CMS/DVA-hosted cameras pick whatever analytics the camera exposes.
3. **Action type: `Uninterruptible`.** The setting asks whether the action may
   be interrupted by another action. *Interruptible* actions can be cut short
   by the next event; *Uninterruptible* ones run to completion, which is what
   you want for a webhook call.
4. **Times = 1.** The webhook action can repeat: *"Execute outgoing webhook
   every {interval}, for a total of {times} times"*. Surveillance Station
   renders the body once and repeats the **same** call, so extra times only
   produce identical events (with the same timestamp). Let the service build
   the burst instead.
5. **Event Interval = 0.** *"Minimum time between events (seconds)"* is a
   debounce on the station side. Keep it at minimum and let the service handle
   throttling, cooldowns and episodes.
6. **Test Send** is useful but sends placeholder values (for example
   `Camera01`), so an unknown-camera warning during a test is normal. Verify
   with real motion.

Notes:

- Photos come from the recording, so the rule does not have to pass any image
  URL, and there is no extra DNS lookup for one.
- Surveillance Station executes the action at its own moment, which can be a
  few seconds after `%EVENT_TIME%` (the start of the event). The log splits the
  delay into *SS delivery* and *our queue*, for example:
  `event 21:11:51 handled 4.7s late (SS delivery 0.6s, our queue 4.1s)`.

## Diagnostics

```bash
python3 -m ss_webhook_telegram config.toml --list-cameras   # print Camera.List and exit
python3 -m ss_webhook_telegram config.toml --dry-run        # do everything except send and persist
python3 -m ss_webhook_telegram config.toml --log-level DEBUG
```

What to look for in the log:

- `New episode: <camera> (N photo(s), clip=mp4)` - an episode opened.
- `Episode <camera>: +N photo(s) for a follow-up event` - a later event in the
  same episode.
- `Clip <camera> @...: lead Xs, snap Ys, fragment As, N frame(s)` -
  download and cut timing.
- `Telegram endpoint switched to <host>` - endpoint rotation kicked in.
- `unknown chat group 'x', using default 'y'` - the rule body and the config
  disagree on the group name.

State lives in `data_folder/events.db` (SQLite, WAL): `dedup` (seen events),
`episodes` (current episode per camera) and `camera_gop` (learned key-frame
interval per camera). Dedup keys older than `episode_gap_secs` are pruned at
startup and then hourly, so the table does not grow forever.

## Troubleshooting

- **Nothing arrives.** Check that the rule fires (Action Rule history), that the
  service is reachable on the configured port, and that `webhook.path` matches
  the rule URL. A `GET` to the path should answer `200`.
- **Photos are fewer than `burst_photos`.** Normal on a static scene: identical
  frames are dropped on purpose.
- **Photos look soft.** Raise `photo_quality` *down* towards 2 (lower is
  better) and/or `photo_max_side`. Above ~1280 on the long side Telegram
  recompresses anyway.
- **The clip arrives a few seconds after it should.** It is the recording
  index lag on the station side; the service already fetches the material as
  soon as it exists.
- **The clip starts a little late.** Reduce `video_skip_first_n_secs`.
- **`unknown chat group`.** Either rename the group in `[[chats]]` or fix
  `"chat"` in the rule body.

## Project layout

| File | Responsibility |
|---|---|
| `ss_webhook_telegram/webhook.py` | HTTP receiver, Basic auth, payload parsing |
| `ss_webhook_telegram/worker.py` | episodes, per-camera queues, recording window, processing pool |
| `ss_webhook_telegram/synology.py` | API client: login, `Camera.List`, `Recording.List/Download` |
| `ss_webhook_telegram/media.py` | ffmpeg: one pass for mp4/gif + photo frames, duration probe |
| `ss_webhook_telegram/telegram.py` | send queue, endpoint rotation, retries |
| `ss_webhook_telegram/store.py` | SQLite: dedup, episodes, learned key-frame interval |
| `ss_webhook_telegram/config.py` | TOML loading and validation |
| `ss_webhook_telegram/__main__.py` | CLI and lifecycle |
| `config-sample.toml` | Documented configuration template |
| `Dockerfile`, `build.sh` | Container image and an amd64 build helper |

## Security notes

- `config.toml` holds secrets. It is gitignored; keep it that way and mount it
  read-only.
- The webhook receiver is unauthenticated unless `[webhook.auth]` is set.
  Prefer a private network or a reverse proxy with TLS in front of it.
