# start the web server, default port is 8080
./venv/bin/airflow webserver --port 8080

# start the scheduler
# open a new terminal or else run webserver with ``-D`` option to run it as a daemon
./venv/bin/airflow scheduler
