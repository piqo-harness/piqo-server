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
cargo build --release -p piqo-server --target "$TARGET"

BINARY="$ROOT_DIR/target/$TARGET/release/piqo-server"
if ! file "$BINARY" | grep -q 'arm64'; then
    echo "built binary is not an arm64 Mach-O: $BINARY" >&2
    exit 1
fi

PACKAGE_NAME="piqo-server-v${VERSION}-macos-arm64"
PACKAGE_DIR="$STAGING_DIR/$PACKAGE_NAME"
mkdir -p "$PACKAGE_DIR"
cp "$BINARY" "$PACKAGE_DIR/piqo-server"
strip -x "$PACKAGE_DIR/piqo-server"
cp "$ROOT_DIR/LICENSE" "$PACKAGE_DIR/LICENSE"
printf '%s\n' '{"server_version":"'"$VERSION"'","api_version":"v1","protocol_version":1,"target":"aarch64-apple-darwin","minimum_macos":"'"$MIN_MACOS"'"}' > "$PACKAGE_DIR/manifest.json"

mkdir -p "$OUTPUT_DIR"
ARCHIVE="$OUTPUT_DIR/$PACKAGE_NAME.tar.gz"
tar -C "$STAGING_DIR" -czf "$ARCHIVE" "$PACKAGE_NAME"
(cd "$OUTPUT_DIR" && shasum -a 256 "$(basename "$ARCHIVE")" > "$(basename "$ARCHIVE").sha256")
printf '%s\n' "$ARCHIVE"
