#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${ROOT_DIR}/.venv-linux"

if ! command -v apt-get >/dev/null 2>&1; then
  echo "This setup script currently supports Debian/Ubuntu via apt-get."
  echo "Install these packages manually on your distro: poppler-utils tesseract-ocr tesseract-ocr-rus ocrmypdf python3-venv python3-pip"
  exit 1
fi

sudo apt-get update
sudo apt-get install -y \
  python3 \
  python3-venv \
  python3-pip \
  poppler-utils \
  tesseract-ocr \
  tesseract-ocr-rus \
  ocrmypdf

python3 -m venv "${VENV_DIR}"
source "${VENV_DIR}/bin/activate"
python -m pip install --upgrade pip
python -m pip install -r "${ROOT_DIR}/requirements-order-service.txt"

echo
echo "Linux environment is ready."
echo "Virtualenv: ${VENV_DIR}"
echo "Check OCR stack:"
echo "  ${VENV_DIR}/bin/python ${ROOT_DIR}/scripts/document_text_extractor.py --show-tools --pretty"
