"""
receive_messages.py
-------------------
Integration script — listens for cloud-to-device (C2D) messages from Azure
IoT Hub and prints them to stdout.

Run from the project root:
    uv --directory raspberry run python scripts/receive_messages.py

Press Ctrl+C to stop.
"""

import json
import logging
import signal
import sys
from typing import Any
from pathlib import Path
# Make sure 'src' is importable when the script is run directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


def on_message(device_id: str, payload: dict[str, Any] | str) -> None:
    """Called whenever a C2D message arrives for any device."""
    print(f"\n{'─' * 60}")
    print(f"  Device : {device_id}")
    if isinstance(payload, dict):
        print(f"  Payload: {json.dumps(payload, indent=4, ensure_ascii=False)}")
    else:
        print(f"  Payload: {payload}")
    print(f"{'─' * 60}\n")


def main() -> None:
    from src.receiver import AzureIoTReceiver, build_receivers

    receivers = build_receivers()

    if not receivers:
        print("ERROR: No devices found. Check DEVICE_IDS in .env", file=sys.stderr)
        sys.exit(1)

    print(f"Listening for C2D messages on {len(receivers)} device(s):")
    for rx in receivers:
        print(f"  [{rx.device_id}]")

    # Connect all receivers and register the handler
    for rx in receivers:
        rx.connect()
        rx.start(on_message)

    print("\nWaiting for messages… (Ctrl+C to stop)\n")

    # Graceful shutdown on SIGINT / SIGTERM
    def _shutdown(*_: object) -> None:
        print("\nShutting down…")
        for rx in receivers:
            rx.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # Block the main thread indefinitely
    receivers[0].wait()


if __name__ == "__main__":
    main()
