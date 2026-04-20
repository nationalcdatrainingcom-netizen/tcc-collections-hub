#!/usr/bin/env bash
# Render build script — installs both Node and Python dependencies.
# Render's default Node service only runs `npm install`; we also need pdfplumber
# (via requirements.txt) so parse_cdc.py can run at request time.
set -euo pipefail

echo "=== Installing Node dependencies ==="
npm install

echo "=== Installing Python dependencies ==="
# Render provides python3 + pip in its Node runtime images; --user keeps pdfplumber
# accessible without needing sudo or touching system site-packages.
python3 -m pip install --user --upgrade pip
python3 -m pip install --user -r requirements.txt

echo "=== Build complete ==="
