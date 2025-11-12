"""Airflow DAG to ingest Code::Stats user metrics into PostgreSQL."""
from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict

import requests
from airflow import DAG
from airflow.decorators import task
from airflow.exceptions import AirflowFailException, AirflowNotFoundException
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import Json

API_BASE_URL = "https://codestats.net/api/users"
DEFAULT_CODESTATS_USERNAME = "antti"
# Default User-Agent that follows https://codestats.net/api-docs guidance by including
# contact information. Users can override this via the ``codestats_user_agent``
# Airflow Variable if needed.
DEFAULT_USER_AGENT = "airflow-codestats-ingest/1.0 (+https://example.com/contact)"
POSTGRES_CONN_ID = "postgres_default"
POSTGRES_CONN_ID_VARIABLE = "codestats_postgres_conn_id"
TARGET_TABLE = "codestats_user_snapshots"


def _build_headers() -> Dict[str, str]:
    """Return headers for Code::Stats API requests."""

    user_agent = Variable.get("codestats_user_agent", default_var=DEFAULT_USER_AGENT)
    return {
        # The public API requires callers to explicitly request JSON responses.
        "Accept": "application/json",
        # The documentation asks clients to supply a descriptive User-Agent with
        # contact details so the maintainers can get in touch if necessary.
        "User-Agent": user_agent,
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


def _resolve_postgres_conn_id() -> str:
    """Return the Postgres connection id to use for persistence."""

    return Variable.get(
        POSTGRES_CONN_ID_VARIABLE,
        default_var=POSTGRES_CONN_ID,
    )


def _get_user_snapshot(username: str) -> requests.Response:
    """Perform the Code::Stats API request for ``username``."""

    return requests.get(
        f"{API_BASE_URL}/{username}",
        headers=_build_headers(),
        timeout=30,
    )


@task
def fetch_user_snapshot(username: str) -> Dict[str, Any]:
    """Fetch the latest statistics for ``username`` with a fallback to the default user."""

    configured_username = username
    response = _get_user_snapshot(configured_username)

    if response.status_code == 404 and configured_username != DEFAULT_CODESTATS_USERNAME:
        logging.warning(
            "Code::Stats user '%s' was not found. Falling back to default user '%s'.",
            configured_username,
            DEFAULT_CODESTATS_USERNAME,
        )
        response = _get_user_snapshot(DEFAULT_CODESTATS_USERNAME)
        if response.status_code == 404:
            raise AirflowFailException(
                "Configured Code::Stats user '%s' was not found, and the default user '%s' "
                "is also unavailable. Please configure a valid username in the Airflow "
                "Variable 'codestats_username'."
                % (configured_username, DEFAULT_CODESTATS_USERNAME)
            )
        username = DEFAULT_CODESTATS_USERNAME
    else:
        username = configured_username

    try:
        response.raise_for_status()
    except requests.HTTPError as exc:
        if response.status_code == 404:
            raise AirflowFailException(
                "Code::Stats user '%s' was not found. Configure the Airflow Variable "
                "'codestats_username' with a valid username." % username
            ) from exc
        raise

    return {"username": username, "payload": response.json()}


@task
def load_snapshot_into_postgres(record: Dict[str, Any]) -> None:
    """Store the retrieved snapshot in PostgreSQL as a JSON document."""

    username = record["username"]
    snapshot = record["payload"]
    postgres_conn_id = _resolve_postgres_conn_id()

    try:
        hook = PostgresHook(postgres_conn_id=postgres_conn_id)
    except AirflowNotFoundException as exc:
        raise AirflowFailException(
            "The configured Postgres connection '%s' is not defined. "
            "Create the Airflow connection or override the connection id via the "
            "'codestats_postgres_conn_id' Variable."
            % postgres_conn_id
        ) from exc

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
    load_snapshot_into_postgres(snapshot)
