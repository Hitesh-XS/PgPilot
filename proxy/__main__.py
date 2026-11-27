"""Run with:  python -m proxy"""
import asyncio
import logging
import os

from proxy.config import Settings
from proxy.server import ProxyServer


async def _run(settings: Settings) -> None:
    server = ProxyServer(settings)
    await server.start()
    try:
        await server.serve_forever()
    finally:
        await server.stop()


def main() -> None:
    logging.basicConfig(
        level=os.getenv("PROXY_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    try:
        asyncio.run(_run(Settings.from_env()))
    except KeyboardInterrupt:
        print("\nPgPilot stopped.")


if __name__ == "__main__":
    main()
