#!/usr/bin/env bash
set -euo pipefail

docker pull quantconnect/lean:latest
docker image inspect quantconnect/lean:latest --format 'LEAN image ready: {{.Id}}'

