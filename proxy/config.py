"""Proxy settings, loaded from environment variables (and a .env file if present)."""
from __future__ import annotations

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass(frozen=True)
class Settings:
    listen_host: str = "127.0.0.1"
    listen_port: int = 6432
    client_password: str = "postgres"   # password clients must give the proxy
    primary_host: str = "127.0.0.1"
    primary_port: int = 5433
    backend_user: str = "postgres"      # service credentials toward Postgres
    backend_password: str = "postgres"
    pool_max_size: int = 20
    pool_acquire_timeout: float = 5.0
    max_client_connections: int = 200

    @classmethod
    def from_env(cls) -> "Settings":
        load_dotenv()
        env = os.getenv
        pg_password = env("PG_PASSWORD", "postgres")
        return cls(
            listen_host=env("PROXY_HOST", "127.0.0.1"),
            listen_port=int(env("PROXY_PORT", "6432")),
            client_password=env("PROXY_PASSWORD", pg_password),
            primary_host=env("PRIMARY_HOST", "127.0.0.1"),
            primary_port=int(env("PRIMARY_PORT", "5433")),
            backend_user=env("BACKEND_USER", "postgres"),
            backend_password=env("BACKEND_PASSWORD", pg_password),
            pool_max_size=int(env("POOL_MAX_SIZE", "20")),
            pool_acquire_timeout=float(env("POOL_ACQUIRE_TIMEOUT", "5")),
            max_client_connections=int(env("MAX_CLIENT_CONNECTIONS", "200")),
        )