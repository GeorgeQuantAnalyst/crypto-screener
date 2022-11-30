#!/bin/bash
set -e

python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt

LOG_FOLDER="$HOME/App/log"
APP_LOG_FOLDER="$HOME/App/log/crypto-screener"
DATABASE_FOLDER="$HOME/App/database"

mkdir -p ${LOG_FOLDER}
mkdir -p ${APP_LOG_FOLDER}
mkdir -p ${DATABASE_FOLDER}