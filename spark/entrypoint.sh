#!/bin/bash

# STREAMING JOB
# 1. Start the streaming ingest job (spark_streaming.py) in the background
echo "Starting Spark streaming ingest job (spark_streaming.py) in the background..."
/opt/spark/bin/spark-submit --master local[*] /app/spark_streaming.py &

# 2. Save the Process ID (PID) of the streaming job
STREAMING_JOB_PID=$!
echo "Streaming job started with PID: $STREAMING_JOB_PID"

# Give it a moment to start up before the batch job runs
echo "Waiting 15 seconds for streaming job to initialize..."
sleep 15

# BATCH JOB LOOP
# 3. Start the batch aggregation job (spark_batch.py) in a loop
echo "Starting Spark batch aggregation loop (spark_batch.py)..."
while true; do

  # 4. Check if the streaming job (by its PID) is still running
  if ! kill -0 $STREAMING_JOB_PID > /dev/null 2>&1; then
    echo "--- ERROR: Streaming job (PID $STREAMING_JOB_PID) has died. ---"
    echo "--- Exiting container to force a full restart. ---"
    # Exit with a non-zero status code
    exit 1
  fi

  echo "Streaming job (PID $STREAMING_JOB_PID) is still running. Starting batch job..."

  # 5. Run the batch job in the foreground
  /opt/spark/bin/spark-submit --master local[*] /app/spark_batch.py

  # Check exit code of batch job.
  if [ $? -ne 0 ]; then
    echo "Spark batch job failed. Retrying in 10 seconds..."
  else
    echo "Batch job finished successfully. Sleeping for 10 seconds..."
  fi

  # 6. Wait before the next loop
  sleep 10
done