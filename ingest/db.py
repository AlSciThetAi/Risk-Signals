# ingest/db.py
"""
Database connection helper.

This file creates a reusable SQLAlchemy Engine that all ingest scripts can use
to talk to Postgres (running in Docker).

Key idea:
- Read connection info from environment variables in your .env file
- Build a Postgres connection URL
- Return a SQLAlchemy engine (connection factory)
"""

import os
from sqlalchemy import create_engine
from dotenv import load_dotenv


def get_engine():
    """
    Create and return a SQLAlchemy Engine.

    Why an Engine?
    - It's the standard SQLAlchemy object for making DB connections.
    - You can reuse it across scripts and queries.

    Where do the credentials come from?
    - load_dotenv() reads the .env file in your current working directory (repo root)
    - Then we read variables like POSTGRES_DB, POSTGRES_USER, etc.
    """

    # Load variables from .env into the process environment (os.environ)
    load_dotenv()

    # Read database settings from environment variables.
    # The second argument is a default value if the env var is missing.
    db = os.getenv("POSTGRES_DB", "risk_signals")
    user = os.getenv("POSTGRES_USER", "risk")
    pw = os.getenv("POSTGRES_PASSWORD", "risk")

    # On your machine, "localhost" works because Docker is publishing the port to your host.
    # If you later run Python *inside* a container, host would be the service name (e.g. "postgres").
    host = os.getenv("POSTGRES_HOST", "localhost")

    # This is the HOST port from your .env (e.g. 5432 or 5433), not the container's internal 5432.
    port = os.getenv("POSTGRES_PORT", "5432")

    # Build the SQLAlchemy database URL
    # Format: postgresql+psycopg2://username:password@host:port/database
    url = f"postgresql+psycopg2://{user}:{pw}@{host}:{port}/{db}"

    # Create the engine. pool_pre_ping=True helps avoid stale connections.
    return create_engine(url, pool_pre_ping=True)