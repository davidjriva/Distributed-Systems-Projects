#!/bin/bash

# A bash array is separated by spaces
# For the list of machines, visit https://cs.colostate.edu/~info/machines
# Or `cat ~info/machines` (there are also cuda_machines, if needed)
MACHINES=("albany" "annapolis" "atlanta" "augusta" "baton-rouge" "bismarck" "boise" "boston" "carson-city" "charleston" "jackson")
REGIONS=("US" "CA" "GB" "JP" "RU" "BR" "DE" "FR" "IN" "KR" "MX")

COMMAND="echo Hello World"

# Loop through the machines and their corresponding regions
for i in "${!MACHINES[@]}"; do
    machine="${MACHINES[i]}"
    region="${REGIONS[i]}"
    
    # The -t is how it prints in the terminal being run from, instead of just on the remote machine
    # 2>/dev/null moves error output, so you don't see "Connection closed"
    # Note the quotes around the command, so that it is treated as one item and not expanded before being sent
    ssh -t "$machine" "python3 /s/bach/l/under/driva/csx55/Term-Project/distributed_random_forest_model.py $region" 2>/dev/null &
done


# All the ssh commands are run as background jobs using `&`
# This will wait for all the children processes to finish before exiting the script
wait
