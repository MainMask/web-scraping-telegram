"""Load Telegram API credentials from the environment / a .env file."""

import os
from dataclasses import dataclass

from dotenv import load_dotenv


@dataclass
class Credentials:
    api_id: int
    api_hash: str
    phone: str | None = None
    password: str | None = None


def load_credentials() -> Credentials:
    """Read TG_* variables. Real environment variables win over the .env file."""
    load_dotenv()

    api_id = os.getenv("TG_API_ID")
    api_hash = os.getenv("TG_API_HASH")

    missing = [name for name, value in (("TG_API_ID", api_id), ("TG_API_HASH", api_hash)) if not value]
    if missing:
        raise SystemExit(
            f"Missing credentials: {', '.join(missing)}. "
            "Copy .env.example to .env and fill it in (values from https://my.telegram.org/apps)."
        )

    try:
        api_id_int = int(api_id)
    except ValueError:
        raise SystemExit(f"TG_API_ID must be an integer, got {api_id!r}.")

    return Credentials(
        api_id=api_id_int,
        api_hash=api_hash,
        phone=os.getenv("TG_PHONE") or None,
        password=os.getenv("TG_PASSWORD") or None,
    )
