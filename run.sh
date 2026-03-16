#!/bin/bash
set -e  # exit immediately if any command fails

cd "$(dirname "$0")" # make sure working directory is correct
git pull
docker compose run --rm --build pipeline >> logs/daily.log 2>&1