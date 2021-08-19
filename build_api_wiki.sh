#!/bin/bash

set -e

BASE_DIR=$(pwd)
SCHEMA_WORKDIR="$BASE_DIR/cruise-control/src/yaml"
WIKI_TARGET="$BASE_DIR/target/api_wiki"

rm -rf "$WIKI_TARGET"
mkdir -p "$WIKI_TARGET"

cd "$SCHEMA_WORKDIR"; npx redoc-cli bundle base.yaml
mv -v "$SCHEMA_WORKDIR"/redoc-static.html "$WIKI_TARGET"
