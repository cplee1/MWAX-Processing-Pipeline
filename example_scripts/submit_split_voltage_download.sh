#!/bin/bash -l

OBSID=$1
OFFSET_INIT=$2
DUR_INIT=$3
NUM_JOBS=$4

if [[ $# -ne 4 ]]; then
    echo "Error: Invalid number of arguments given."
    exit 1
fi

DUR_PER_JOB_FLOAT=$(echo "$DUR_INIT / $NUM_JOBS" | bc -l)
if [[ $(echo "$DUR_PER_JOB_FLOAT % 1 == 0" | bc ) != 1 ]]; then
    echo "Error: ${DUR_INIT} cannot be cleanly divided by ${NUM_JOBS}."
    exit 1
fi

echo "module load singularity"
module load singularity

DUR_PER_JOB=$(( DUR_INIT / NUM_JOBS ))
for ((i = 0; i < NUM_JOBS; i++)); do
    OFFSET=$(( OFFSET_INIT + DUR_PER_JOB*i ))
    echo "giant-squid submit-volt -n -v -d scratch -o $OFFSET -d $DUR_PER_JOB $OBSID"
    singularity exec docker://mwatelescope/giant-squid:latest /opt/cargo/bin/giant-squid \
        submit-volt -v -d scratch -o $OFFSET -u $DUR_PER_JOB $OBSID
done
