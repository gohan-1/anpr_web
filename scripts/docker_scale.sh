#!/usr/bin/env bash
# Set how many bot containers ONE stage has, without touching the others:
#
#   scripts/docker_scale.sh ocr 3        (make docker-scale STAGE=ocr N=3)
#
# WHY a script: `docker compose up --scale bot-ocr=3` alone puts every OTHER bot service back
# to its default of 1 container. This reads how many each stage runs now and keeps those.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")/.."
STAGES=(reader preprocess ocr llm trip)

stage=${1:-}; n=${2:-}
if [[ ! " ${STAGES[*]} " =~ " ${stage} " ]] || ! [[ "$n" =~ ^[0-9]+$ ]]; then
  echo "Usage: make docker-scale STAGE=<reader|preprocess|ocr|llm|trip> N=<number of containers>"
  echo "  e.g. make docker-scale STAGE=ocr N=3"
  exit 2
fi

args=()
for s in "${STAGES[@]}"; do
  if [ "$s" = "$stage" ]; then
    count=$n
  else
    count=$(docker compose ps -q "bot-$s" | wc -l)      # running containers now
  fi
  args+=(--scale "bot-$s=$count")
done

echo "bot-$stage -> $n container(s); other stages keep their current count."
docker compose up -d --no-recreate "${args[@]}"
echo
docker compose ps --format '{{.Service}}' | grep '^bot-' | sort | uniq -c | awk '{printf "  %-16s %s container(s)\n", $2, $1}'
