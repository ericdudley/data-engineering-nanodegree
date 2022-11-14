start:
	source ./venv/bin/activate && export NO_PROXY=* && airflow standalone

start-scheduler:
	source ./venv/bin/activate && export NO_PROXY=* && airflow scheduler