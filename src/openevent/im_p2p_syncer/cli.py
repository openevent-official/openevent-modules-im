from __future__ import annotations

import argparse
import logging
import signal
import threading

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
    shutdown_requested = threading.Event()
    worker_error: list[BaseException] = []

    def handle_signal(signum, frame):
        shutdown_requested.set()

    def run_syncer() -> None:
        try:
            syncer.start()
        except BaseException as exc:
            worker_error.append(exc)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    worker = threading.Thread(target=run_syncer, name="im-p2p-syncer", daemon=True)
    worker.start()
    shutdown_timed_out = False
    try:
        while worker.is_alive() and not shutdown_requested.wait(0.1):
            pass
    finally:
        syncer.stop()
        worker.join(config.worker.shutdown_timeout_ms / 1000)
        shutdown_timed_out = worker.is_alive()
        channel = getattr(openevent_client, "channel", None)
        close = getattr(channel, "close", None)
        if close is not None:
            close()
    if shutdown_timed_out:
        logging.getLogger(__name__).error(
            "graceful shutdown exceeded %sms",
            config.worker.shutdown_timeout_ms,
        )
        return 1
    if worker_error:
        raise worker_error[0]
    return 0


def _create_openevent_client(target: str):
    from openevent.sdk import OpenEventClient

    return OpenEventClient(target)


if __name__ == "__main__":
    raise SystemExit(main())
