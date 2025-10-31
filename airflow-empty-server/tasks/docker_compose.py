from invoke import Collection, task

from .utils import docker_compose_command


@task
def up_airflow(ctx, version=2, build=False, extra_services=None):
    """Start the core Airflow stack."""
    services = [
        "airflow-metadata",
        "airflow-scheduler",
        "airflow-webserver",
        "airflow-init",
    ]
    if extra_services is not None:
        services.extend(extra_services)

    compose_args = ["up", "-d"]
    if build:
        compose_args.append("--build")
    compose_args.extend(services)

    docker_compose_command(ctx, " ".join(compose_args), version=version)


@task
def down(ctx, volumes=False, version=2):
    """Stop and remove containers."""
    compose_args = ["down"]
    if volumes:
        compose_args.append("--volumes")
    docker_compose_command(ctx, " ".join(compose_args), version=version)


@task
def stop(ctx, version=2):
    """Stop containers without removing them."""
    docker_compose_command(ctx, "stop", version=version)


@task
def command(ctx, cmd, version=2):
    """Run an arbitrary docker compose command."""
    docker_compose_command(ctx, cmd, version=version)


docker_compose_collection = Collection()
docker_compose_collection.add_task(up_airflow, name="up_airflow")
docker_compose_collection.add_task(down, name="down")
docker_compose_collection.add_task(stop, name="stop")
docker_compose_collection.add_task(command, name="cmd")
