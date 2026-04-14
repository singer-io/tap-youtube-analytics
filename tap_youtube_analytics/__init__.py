import sys
import json
import singer
from tap_youtube_analytics.client import Client
from tap_youtube_analytics.discover import discover
from tap_youtube_analytics.sync import sync

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["client_id", "client_secret", "channel_ids", "start_date", "user_agent", "refresh_token"]


def ensure_refresh_token(config):
    """Populate config['refresh_token'] from supported OAuth payloads."""
    if config.get("refresh_token"):
        return

    for candidate in (
        config.get("oauth_credentials"),
        config.get("oauth"),
    ):
        if isinstance(candidate, dict) and candidate.get("refresh_token"):
            config["refresh_token"] = candidate["refresh_token"]
            return

    raise ValueError(
        "Missing refresh_token. Provide it in the config or ensure the OAuth "
        "handshake supplies one via 'oauth_credentials'."
    )

def do_discover():
    """Discover and emit the catalog to stdout"""
    LOGGER.info("Starting discover")
    catalog = discover()
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info("Finished discover")


@singer.utils.handle_top_exception(LOGGER)
def main():
    """Run the tap"""
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    ensure_refresh_token(parsed_args.config)
    state = {}
    if parsed_args.state:
        state = parsed_args.state

    with Client(parsed_args.config) as client:
        if parsed_args.discover:
            do_discover()
        elif parsed_args.catalog:
            sync(
                 client=client,
                 config=parsed_args.config,
                 catalog=parsed_args.catalog,
                 state=state)


if __name__ == "__main__":
    main()
