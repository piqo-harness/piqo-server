#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TARGET=aarch64-apple-darwin
MIN_MACOS=26.0
VERSION=$(cargo pkgid -p piqo-server | sed 's/.*#//')
OUTPUT_DIR=${1:-"$ROOT_DIR/dist"}
STAGING_DIR=$(mktemp -d)
trap 'rm -rf "$STAGING_DIR"' EXIT

export MACOSX_DEPLOYMENT_TARGET="$MIN_MACOS"
cargo build --locked --release -p piqo-server --target "$TARGET"

BINARY="$ROOT_DIR/target/$TARGET/release/piqo-server"
if [[ "$(lipo -archs "$BINARY")" != "arm64" ]]; then
    echo "built binary is not a thin arm64 Mach-O: $BINARY" >&2
    exit 1
fi

BUILT_MIN_MACOS=$(otool -l "$BINARY" \
    | awk '/LC_BUILD_VERSION/{found=1; next} found && /minos/{print $2; exit}')
if [[ "$BUILT_MIN_MACOS" != "$MIN_MACOS" ]]; then
    echo "built binary targets macOS $BUILT_MIN_MACOS instead of $MIN_MACOS" >&2
    exit 1
fi

PACKAGE_NAME="piqo-server-v${VERSION}-macos-arm64"
PACKAGE_DIR="$STAGING_DIR/$PACKAGE_NAME"
mkdir -p "$PACKAGE_DIR"
cp "$BINARY" "$PACKAGE_DIR/piqo-server"
strip -x "$PACKAGE_DIR/piqo-server"
chmod 755 "$PACKAGE_DIR/piqo-server"
cp "$ROOT_DIR/LICENSE" "$PACKAGE_DIR/LICENSE"
printf '%s\n' '{"server_version":"'"$VERSION"'","api_version":"v1","protocol_version":1,"target":"aarch64-apple-darwin","minimum_macos":"'"$MIN_MACOS"'"}' > "$PACKAGE_DIR/manifest.json"

if [[ "$(lipo -archs "$PACKAGE_DIR/piqo-server")" != "arm64" ]]; then
    echo "packaged binary is not a thin arm64 Mach-O" >&2
    exit 1
fi

SIGNING_INFO=$(codesign -dvv "$PACKAGE_DIR/piqo-server" 2>&1 || true)
if grep -q '^Authority=' <<< "$SIGNING_INFO"; then
    echo "packaged binary must not carry a distribution identity" >&2
    exit 1
fi

mkdir -p "$OUTPUT_DIR"
ARCHIVE="$OUTPUT_DIR/$PACKAGE_NAME.tar.gz"
tar -C "$STAGING_DIR" -czf "$ARCHIVE" "$PACKAGE_NAME"
printf '%s\n' "$ARCHIVE"
