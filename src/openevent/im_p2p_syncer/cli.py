from __future__ import annotations

import argparse
import logging
import signal

from .config import load_config
from .syncer import P2PSyncer


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="im-p2p-syncer")
    parser.add_argument("--config", required=True)
    args = parser.parse_args(argv)

    config = load_config(args.config)
    logging.basicConfig(level=getattr(logging, str(config.logging.get("level", "INFO")).upper()))

    openevent_client = _create_openevent_client(config.openevent.target)
    syncer = P2PSyncer(config, openevent_client)

    def handle_signal(signum, frame):
        syncer.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    try:
        syncer.start()
    finally:
        syncer.stop()
        channel = getattr(openevent_client, "channel", None)
        close = getattr(channel, "close", None)
        if close is not None:
            close()
    return 0


def _create_openevent_client(target: str):
    from openevent.sdk import OpenEventClient

    return OpenEventClient(target)


if __name__ == "__main__":
    raise SystemExit(main())
