#!/usr/bin/env bash
# Entry point for the Spark 4.2 + Iceberg 1.12 container.
#
#   start.sh jupyter        JupyterLab on :8888  (default)
#   start.sh pyspark        interactive PySpark shell
#   start.sh spark-sql      interactive Spark SQL shell
#   start.sh <cmd> [args]   run <cmd> as the unprivileged `spark` user
#
# The container starts as root so that it can fix up the ownership of the
# bind-mounted notebook directory before dropping privileges with gosu.
set -euo pipefail

NOTEBOOKS_DIR="${NOTEBOOKS_DIR:-/home/spark/notebooks}"
EVENTS_DIR="${EVENTS_DIR:-/home/spark/spark-events}"
LOCAL_DIR="${SPARK_LOCAL_DIR:-/home/spark/spark-local}"

mkdir -p "$NOTEBOOKS_DIR" "$EVENTS_DIR" "$LOCAL_DIR"

# The notebook directory is bind mounted from the host, whose uid does not match
# the `spark` uid in the image, so make it group/other writable.
chmod -R a+rwX "$NOTEBOOKS_DIR" 2>/dev/null || true
chown -R spark:spark "$EVENTS_DIR" "$LOCAL_DIR" 2>/dev/null || true

if [ "${1:-jupyter}" = "jupyter" ]; then
  shift || true
  exec gosu spark jupyter lab \
    --ip=0.0.0.0 \
    --port="${JUPYTER_PORT:-8888}" \
    --no-browser \
    --notebook-dir="$NOTEBOOKS_DIR" \
    --ServerApp.token='' \
    --ServerApp.password='' \
    --ServerApp.disable_check_xsrf=True \
    --ServerApp.allow_origin='*' \
    "$@"
fi

exec gosu spark "$@"
