"""Airflow DAG to ingest Code::Stats user metrics into PostgreSQL."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import Json

API_BASE_URL = "https://codestats.net/api/users"
DEFAULT_CODESTATS_USERNAME = "antti"  # Replace with the desired Code::Stats user name
POSTGRES_CONN_ID = "postgres_default"
TARGET_TABLE = "codestats_user_snapshots"


def _build_headers() -> Dict[str, str]:
    """Return headers for Code::Stats API requests."""
    return {
        "Accept": "application/json",
        "User-Agent": "airflow-codestats-ingest/1.0",
    }


def _resolve_username() -> str:
    """Return the configured Code::Stats username.

    The value is read from the Airflow Variable ``codestats_username`` if set.
    Otherwise, the ``DEFAULT_CODESTATS_USERNAME`` constant is used.
    """

    return Variable.get(
        "codestats_username",
        default_var=DEFAULT_CODESTATS_USERNAME,
    )


@task
def fetch_user_snapshot(username: str) -> Dict[str, Any]:
    """Fetch the latest statistics for ``username`` from the Code::Stats API."""
    response = requests.get(
        f"{API_BASE_URL}/{username}",
        headers=_build_headers(),
        timeout=30,
    )
    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        if response.status_code == 404:
            raise AirflowFailException(
                "Code::Stats user '%s' was not found. Configure the Airflow Variable "
                "'codestats_username' with a valid username." % username
            ) from exc
        raise
    return response.json()


@task
def load_snapshot_into_postgres(username: str, snapshot: Dict[str, Any]) -> None:
    """Store the retrieved snapshot in PostgreSQL as a JSON document."""
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    hook.run(
        f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            id BIGSERIAL PRIMARY KEY,
            username TEXT NOT NULL,
            payload JSONB NOT NULL,
            fetched_at TIMESTAMPTZ NOT NULL
        )
        """
    )

    hook.insert_rows(
        table=TARGET_TABLE,
        rows=[(username, Json(snapshot), datetime.now(timezone.utc))],
        target_fields=["username", "payload", "fetched_at"],
        replace=False,
    )


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
}

with DAG(
    dag_id="codestats_to_postgres",
    description="Load Code::Stats API snapshots into PostgreSQL",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["codestats", "postgres"],
) as dag:
    codestats_username = _resolve_username()

    snapshot = fetch_user_snapshot(codestats_username)
    load_snapshot_into_postgres(codestats_username, snapshot)
