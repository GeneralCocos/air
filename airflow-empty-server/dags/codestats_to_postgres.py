"""Airflow DAG to ingest Code::Stats user metrics into PostgreSQL."""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import Json

API_BASE_URL = "https://codestats.net/api/users"
CODESTATS_USERNAME = "codeemo"  # Replace with the desired Code::Stats user name
POSTGRES_CONN_ID = "postgres_default"
TARGET_TABLE = "codestats_user_snapshots"


def _build_headers() -> Dict[str, str]:
    """Return headers for Code::Stats API requests."""
    return {
        "Accept": "application/json",
        "User-Agent": "airflow-codestats-ingest/1.0",
    }


@task
def fetch_user_snapshot(username: str) -> Dict[str, Any]:
    """Fetch the latest statistics for ``username`` from the Code::Stats API."""
    response = requests.get(
        f"{API_BASE_URL}/{username}",
        headers=_build_headers(),
        timeout=30,
    )
    response.raise_for_status()
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
    snapshot = fetch_user_snapshot(CODESTATS_USERNAME)
    load_snapshot_into_postgres(CODESTATS_USERNAME, snapshot)
