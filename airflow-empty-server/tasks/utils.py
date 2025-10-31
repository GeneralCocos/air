from pathlib import Path


def get_project_dir(ctx) -> Path:
    return Path(ctx.run("git rev-parse --show-toplevel").stdout.strip()) / "airflow-empty-server"


def docker_compose_command(ctx, command, version=2):
    project_dir = get_project_dir(ctx)
    compose_folder = project_dir / "docker-compose"
    compose_files = ["airflow"]
    compose_cmd = "docker compose" if version == 2 else "docker-compose"
    compose_args = " ".join(
        f"-f {compose_folder.joinpath(name)}.yml" for name in compose_files
    )
    full_cmd = f"{compose_cmd} {compose_args} {command}"
    print(full_cmd)
    ctx.run(full_cmd)
