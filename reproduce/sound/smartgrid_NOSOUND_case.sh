#!/usr/bin/env bash

function usage() {
    echo "Usage: $0 #reps #duration_min"
    exit 1
}

[[ -z $1 ]] && usage
[[ -z $2 ]] && usage
#[[ -z $3 ]] && usage

REPS="$1"
DURATION="$2"
#KAFKA_HOST="$3"
DATE_CODE=$(date +%j_%H%M)
COMMIT_CODE=$(git rev-parse --short HEAD)
EXPERIMENT_FOLDER="${DATE_CODE}_${COMMIT_CODE}_$(hostname)"

echo "Starting experiment. Results will be stored in data/output/$EXPERIMENT_FOLDER"
sleep 2

./scripts/run.py ./experiments/checkresults/SmartGridAnomalyCheckResultNOSOUNDCase.yaml -d "$DURATION" -r "$REPS" -c "$DATE_CODE"



cmd="./reproduce/plot_checkresult.py --path \"data/output/$EXPERIMENT_FOLDER\" --plot comparison_nosound"

echo "To re-run only the plot, execute: $cmd"
bash -c "$cmd"
