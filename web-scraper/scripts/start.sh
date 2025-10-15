#!/usr/bin/env bash
# start.sh — starts one or more uv-based services (controller, worker, etc.)

set -euo pipefail

# Usage info
usage() {
  echo "Usage: $0 {controller|worker|all}"
  echo
  echo "Examples:"
  echo "  $0 controller   # start only the FastAPI controller"
  echo "  $0 worker       # start only the worker process"
  echo "  $0 all          # start both controller and worker concurrently"
  exit 1
}

# Start the controller service
start_controller() {
  echo "Starting controller..."
  uv run fastapi dev src/controller/main.py
}

# Start the worker service
start_worker() {
  echo "Starting worker..."
  uv run -m src.worker.main
}

# Check the argument
if [[ $# -lt 1 ]]; then
  usage
fi

case "$1" in
  controller)
    start_controller
    ;;
  worker)
    start_worker
    ;;
  all)
    # Start both concurrently and wait for them
    start_controller &
    controller_pid=$!
    start_worker &
    worker_pid=$!

    echo "Controller PID: $controller_pid"
    echo "Worker PID: $worker_pid"
    echo "Press Ctrl+C to stop both services."

    # Trap Ctrl+C to kill both cleanly
    trap "echo 'Stopping services...'; kill $controller_pid $worker_pid 2>/dev/null; exit 0" SIGINT SIGTERM

    # Wait for both processes
    wait $controller_pid $worker_pid
    ;;
  *)
    usage
    ;;
esac
