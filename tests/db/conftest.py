"""Ephemeral PostgreSQL for the `db` tests. Skips cleanly when initdb/pg_ctl are not on PATH.

Unix sockets are disabled (pytest's tmp paths exceed the socket path limit on macOS); the
cluster listens on 127.0.0.1 on a free port and is destroyed at session end."""

from __future__ import annotations

import shutil
import socket
import subprocess
import time

import pytest

pytestmark = pytest.mark.db


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture(scope="session")
def pg_dsn(tmp_path_factory):
    initdb, pg_ctl = shutil.which("initdb"), shutil.which("pg_ctl")
    if not initdb or not pg_ctl:
        pytest.skip("PostgreSQL binaries (initdb/pg_ctl) not on PATH; db tests skipped")
    base = tmp_path_factory.mktemp("pg")
    data, logf = base / "data", base / "server.log"
    subprocess.run([initdb, "-D", str(data), "-U", "postgres", "-A", "trust", "--no-sync", "-E", "UTF8"],
                   check=True, capture_output=True)
    port = _free_port()
    opts = f"-p {port} -h 127.0.0.1 -c unix_socket_directories='' -c fsync=off -c synchronous_commit=off -F"
    subprocess.run([pg_ctl, "-D", str(data), "-o", opts, "-l", str(logf), "-W", "start"], check=True, capture_output=True)
    dsn = f"postgresql://postgres@127.0.0.1:{port}/postgres"
    import psycopg
    deadline = time.time() + 30
    while True:
        try:
            with psycopg.connect(dsn, connect_timeout=2):
                break
        except psycopg.OperationalError:
            if time.time() > deadline:
                subprocess.run([pg_ctl, "-D", str(data), "-m", "immediate", "stop"], capture_output=True)
                raise RuntimeError(f"postgres did not come up; log:\n{logf.read_text() if logf.exists() else ''}")
            time.sleep(0.1)
    try:
        yield dsn
    finally:
        subprocess.run([pg_ctl, "-D", str(data), "-m", "immediate", "stop"], capture_output=True)


@pytest.fixture(scope="session")
def pg_store(pg_dsn):
    from geoecon_pipeline_mcp.pg import PostgresStore

    store = PostgresStore.connect(pg_dsn)
    yield store
    store.close()


@pytest.fixture
def repo(pg_store):
    return pg_store.repo
