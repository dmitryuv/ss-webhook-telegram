# The iHD VAAPI driver is packaged for Debian, but the i965 driver - the one
# that works on Synology kernels older than 4.x, where iHD cannot initialise -
# was dropped from the archive long ago. Build it from the upstream release
# tarball in a throwaway stage so no toolchain lands in the shipped image.
FROM python:3.13-slim AS vaapi-i965

ARG I965_VERSION=2.4.1
ARG I965_SHA1=c998e06f13238c6419bb38ede9a13e453312737b

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential pkg-config libva-dev libdrm-dev ca-certificates curl \
 && rm -rf /var/lib/apt/lists/*
RUN curl -fsSL -o /tmp/i965.tar.bz2 \
      "https://github.com/intel/intel-vaapi-driver/releases/download/${I965_VERSION}/intel-vaapi-driver-${I965_VERSION}.tar.bz2" \
 && echo "${I965_SHA1}  /tmp/i965.tar.bz2" | sha1sum -c - \
 && mkdir -p /src && tar -xjf /tmp/i965.tar.bz2 -C /src --strip-components=1 \
 && cd /src \
 && ./configure --prefix=/usr --disable-x11 --disable-wayland --disable-tests \
 && make -j"$(nproc)" \
 && make install DESTDIR=/out


FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LANG=C.UTF-8

# Both VAAPI drivers are shipped; libva picks the one that initialises (iHD on
# modern kernels, i965 on old Synology ones). ffmpeg ships libva/libvpl but not
# the drivers, so without them `-hwaccel vaapi` fails on /dev/dri. Pass the
# device through (--device /dev/dri) to decode HEVC on the GPU; otherwise the
# service falls back to software decoding.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg intel-media-va-driver \
 && rm -rf /var/lib/apt/lists/*
COPY --from=vaapi-i965 /out/usr/lib/x86_64-linux-gnu/dri/i965_drv_video.so \
     /usr/lib/x86_64-linux-gnu/dri/i965_drv_video.so

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
 && rm /tmp/requirements.txt

COPY ss_webhook_telegram /ss_webhook_telegram

# Relative data_folder/ffmpeg_working_folder from the config ("/data", "/clips")
# resolve inside the mounted volumes from here.
WORKDIR /

VOLUME ["/config", "/clips", "/data"]

ENTRYPOINT ["python3", "-m", "ss_webhook_telegram"]
CMD ["/config/config.toml"]
