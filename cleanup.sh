#!/bin/bash

MACHINES=("santa-fe" "ferrari" "porsche" "eldora" "tokyo" "montgomery" "cooper" "sunlight" "vail" "telluride")

for machine in "${MACHINES[@]}"; do
    echo "Cleaning up $machine"
    ssh $machine 'pkill java >/dev/null 2>&1'
done