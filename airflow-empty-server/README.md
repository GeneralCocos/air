# Airflow empty server

This repository provides a stripped-down variant of the original Airflow stack
with no sample DAGs or data. It is intended as a clean starting point where you
can immediately spin up Airflow using the most recent stable container images
and then add your own pipelines.

## Requirements
- [Python](https://www.python.org/downloads/) 3.8+
- [Docker](https://docs.docker.com/engine/install/) and [Docker Compose](https://docs.docker.com/compose/install/)
- Install the local helper tools:
  ```shell
  pip install -r requirements.txt
  ```

## Project layout
```
├── dags/            # empty placeholder – add your DAGs here
├── db/              # postgres volume (created on first run)
├── docker/
│   ├── Dockerfile   # extends apache/airflow with optional Python deps
│   └── requirements.txt
├── docker-compose/
│   └── airflow.yml  # compose definition for metadata DB and Airflow services
├── logs/            # scheduler/webserver logs
├── scripts/         # host scripts mounted into the containers
└── tasks/           # invoke tasks for convenience wrappers
```

The `dags`, `db`, `logs`, and `scripts` directories only contain `.gitkeep`
placeholders so that they exist in a fresh clone.

## Configuration
Airflow specific configuration is stored in
[`docker-compose/airflow/airflow.env`](docker-compose/airflow/airflow.env). By
default examples are disabled and the `LocalExecutor` is used.

The first user is created through
[`scripts/entrypoint.sh`](scripts/entrypoint.sh).
Update the `_AIRFLOW_WWW_USER_*` environment variables in the compose file to
change the credentials.

### Using newer image tags
The compose file builds a local image named `my-custom-airflow` that extends the
official `apache/airflow` image. The Airflow version can be changed either by
exporting an `AIRFLOW_VERSION` environment variable before running the helper
commands or by editing the default value inside the compose file and Dockerfile.
All services default to Airflow **2.9.1**, while PostgreSQL uses the official
**16** series image.

## Running the stack
From the repository root run:

```shell
invoke compose.up_airflow --build
```

This command will build the custom image (so that you can add Python
dependencies later) and start PostgreSQL, the scheduler, the webserver, and the
initialization job. Once the containers are healthy, visit
`http://localhost:8080` and log in using the username and password configured in
the compose file.

## Stopping and cleaning up
- Stop containers without removing them:
  ```shell
  invoke compose.stop
  ```
- Stop and remove containers:
  ```shell
  invoke compose.down
  ```
- Stop, remove containers, and prune volumes:
  ```shell
  invoke compose.down --volumes
  ```

## Customizing the Airflow image
Add any extra Python dependencies to `docker/requirements.txt` and rebuild using
`invoke compose.up_airflow --build`. The packages will be installed on top of the
selected `apache/airflow` base image.
