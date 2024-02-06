#!/bin/bash
SESSION="cs555-hw1"

if tmux has-session -t $SESSION; then
    echo "Cleaning an existing session."

    tmux kill-session -t $SESSION
    sleep 1
    ./cleanup.sh
    sleep 1
fi

LOCAL_DIR="hw1/build/classes/java/main"
REMOTE_DIR="csx55/$LOCAL_DIR"

REGISTRY_COMMAND="csx55.overlay.node.Registry 40015"
MESSAGING_NODE_COMMAND="csx55.overlay.node.MessagingNode jackson 40015"

STARTUP_REGISTRY="java $REGISTRY_COMMAND"
STARTUP_MESSAGING_NODE="java $MESSAGING_NODE_COMMAND"

MACHINES=("santa-fe" "ferrari" "porsche" "eldora" "tokyo" "montgomery")

tmux new-session -d -s $SESSION
tmux send-keys -t $SESSION:0 "cd $LOCAL_DIR && $STARTUP_REGISTRY" C-m

for i in {1..6}; do
    tmux split-window -h -t $SESSION:0
    tmux select-layout tiled

    tmux send-keys -t $SESSION:0.$i "ssh ${MACHINES[$i - 1]} 'cd $REMOTE_DIR && printf \"\033c\" && $STARTUP_MESSAGING_NODE'" C-m
done

tmux select-pane -t $SESSION:0.0
tmux attach-session -t $SESSION