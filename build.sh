#!/usr/bin/env bash
# Local build for the NAS (amd64) as a docker-save tarball, for manual upload.
#
#   ./build.sh            -> tag from __version__
#   ./build.sh 0.1.1      -> explicit tag
set -euo pipefail

cd "$(dirname "$0")"

IMAGE="ss_webhook_telegram"
TAG="${1:-$(sed -n 's/^__version__ = "\(.*\)"/\1/p' ss_webhook_telegram/__init__.py)}"

if [ -z "$TAG" ]; then
  echo "cannot determine the version tag from ss_webhook_telegram/__init__.py" >&2
  exit 1
fi

echo "building $IMAGE:$TAG"
docker buildx build --platform linux/amd64 -t "$IMAGE:$TAG" -t "$IMAGE:latest" .

echo "saving to $IMAGE.tar"
docker save "$IMAGE:$TAG" -o "$IMAGE.tar"

echo "done: $IMAGE:$TAG and $IMAGE.tar"
