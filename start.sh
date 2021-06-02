# start the scheduler
# open a new terminal or else run webserver with ``-D`` option to run it as a daemon

#!/bin/bash

# Start airflow
./venv/bin/airflow scheduler --daemon
./venv/bin/airflow webserver --daemon -p 3000

# Wait till airflow web-server is ready
echo "Waiting for Airflow web server..."
while true; do
  _RUNNING=$(ps aux | grep airflow-webserver | grep ready | wc -l)
  if [ $_RUNNING -eq 0 ]; then
    sleep 1
  else
    echo "Airflow web server is ready"
    break;
  fi
done
