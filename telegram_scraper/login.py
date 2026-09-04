"""One-time interactive authorisation: save a Telethon session."""

import asyncio

from telethon import TelegramClient

from telegram_scraper.config import Credentials


async def _login(creds: Credentials, session: str) -> None:
    client = TelegramClient(session, creds.api_id, creds.api_hash)
    await client.start(phone=creds.phone, password=creds.password)
    try:
        me = await client.get_me()
        print(f"Logged in as {me.first_name} (@{me.username}), id {me.id}")
        print(f"Session saved: {session}.session")
    finally:
        await client.disconnect()


def login(creds: Credentials, session: str) -> None:
    asyncio.run(_login(creds, session))
