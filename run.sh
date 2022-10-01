#!/bin/bash
set -e

source venv/bin/activate
python -m crypto_screener
libreoffice data/CryptoScreenerWithValues.csv