#!/bin/bash

# Source folder with generated RDFs
SOURCE_DIR="./versions"

# Local target directory (used as Docker volume)
TARGET_DIR="./aopwikirdf"

# Create target dir if it doesn't exist
mkdir -p "$TARGET_DIR"

echo "[→] Copying .ttl files into local folder: $TARGET_DIR"

# Copy all relevant TTL files from versions/ into a flat structure in aopwikirdf/
find "$SOURCE_DIR" -type f \( \
    -name "AOPWikiRDF-*.ttl" -o \
    -name "AOPWikiRDF-Genes-*.ttl" -o \
    -name "AOPWikiRDF-Void-*.ttl" \
\) | while read -r file; do
    echo "[↓] Copying: $file"
    cp "$file" "$TARGET_DIR/"
done

echo "[✓] All .ttl files copied to: $TARGET_DIR"
