
import os
from dotenv import load_dotenv
from loguru import logger
from typing import List, Optional
from plexapi.server import PlexServer
from plexapi.myplex import MyPlexAccount

from src.functions import str_to_bool, CONFIG_DIR
from src.plex import Plex

load_dotenv(os.path.join(CONFIG_DIR, ".env"), override=True)


def generate_server_connections() -> list[Plex]:
    servers: list[Plex] = []

    plex_baseurl_str: str | None = os.getenv("PLEX_BASEURL", None)
    plex_token_str: str | None = os.getenv("PLEX_TOKEN", None)
    plex_username_str: str | None = os.getenv("PLEX_USERNAME", None)
    plex_password_str: str | None = os.getenv("PLEX_PASSWORD", None)
    plex_servername_str: str | None = os.getenv("PLEX_SERVERNAME", None)
    ssl_bypass = str_to_bool(os.getenv("SSL_BYPASS", "False"))

    if plex_baseurl_str and plex_token_str:
        plex_baseurl = plex_baseurl_str.split(",")
        plex_token = plex_token_str.split(",")

        if len(plex_baseurl) != len(plex_token):
            raise Exception(
                "PLEX_BASEURL and PLEX_TOKEN must have the same number of entries"
            )

        for i, url in enumerate(plex_baseurl):
            server = Plex(
                base_url=url.strip(),
                token=plex_token[i].strip(),
                user_name=None,
                password=None,
                server_name=None,
                ssl_bypass=ssl_bypass,
            )

            logger.debug(f"[System] Connected to: {server.info()}")

            servers.append(server)

    if plex_username_str and plex_password_str and plex_servername_str and not servers:
        plex_username = plex_username_str.split(",")
        plex_password = plex_password_str.split(",")
        plex_servername = plex_servername_str.split(",")

        if len(plex_username) != len(plex_password) or len(plex_username) != len(
            plex_servername
        ):
            raise Exception(
                "PLEX_USERNAME, PLEX_PASSWORD and PLEX_SERVERNAME must have the same number of entries"
            )

        for i, username in enumerate(plex_username):
            server = Plex(
                base_url=None,
                token=None,
                user_name=username.strip(),
                password=plex_password[i].strip(),
                server_name=plex_servername[i].strip(),
                ssl_bypass=ssl_bypass,
            )

            logger.debug(f"[System] Connected to: {server.info()}")
            servers.append(server)

    return servers
