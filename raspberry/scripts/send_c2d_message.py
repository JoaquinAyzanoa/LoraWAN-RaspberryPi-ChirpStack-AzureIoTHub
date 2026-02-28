"""
send_c2d_message.py
-------------------
Send a cloud-to-device (C2D) message to a specific device via Azure IoT Hub.

A solution backend connects to IoT Hub and encodes a message with a destination
device. IoT Hub stores the message in its queue and delivers it to the target device.

The message payload encodes a "method" name so the target device can dispatch
to the right handler (run_hmi / stop_hmi / reset_hmi).

Requires IOTHUB_SERVICE_CONNECTION_STRING in .env — use iothubowner or a policy
that has "service" permission.

Run from project root:
    uv --directory raspberry run python scripts/send_c2d_message.py <device-id> <method> <user>

Examples:
    uv --directory raspberry run python scripts/send_c2d_message.py pulse-id-100 run_hmi admin
    uv --directory raspberry run python scripts/send_c2d_message.py pulse-id-101 stop_hmi operator '{"speed": 3}'
"""

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from azure.iot.hub import IoTHubRegistryManager  # type: ignore[import]
from src.settings import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

# Silence noisy uamqp internals — promote to WARNING
for _noisy in ("uamqp.connection", "uamqp.sender", "uamqp.c_uamqp", "uamqp.authentication.cbs_auth"):
    logging.getLogger(_noisy).setLevel(logging.WARNING)

VALID_METHODS = {"run_hmi", "stop_hmi", "reset_hmi"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Send a C2D message to a target device via Azure IoT Hub."
    )
    parser.add_argument("device_id", help="Target device ID (e.g. pulse-id-100)")
    parser.add_argument(
        "method",
        choices=sorted(VALID_METHODS),
        help="HMI method to invoke on the device",
    )
    parser.add_argument("user", help="User generating the HMI command")
    parser.add_argument(
        "payload",
        nargs="?",
        default="{}",
        help='Optional extra JSON merged into the message body (default: "{}")',
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    conn_str = settings.IOTHUB.SERVICE_CONNECTION_STRING
    if not conn_str:
        print(
            "ERROR: IOTHUB_SERVICE_CONNECTION_STRING is not set in .env\n"
            "       Use the iothubowner or service policy connection string from the portal.",
            file=sys.stderr,
        )
        sys.exit(1)

    try:
        extra = json.loads(args.payload)
    except json.JSONDecodeError as exc:
        print(f"ERROR: invalid JSON payload — {exc}", file=sys.stderr)
        sys.exit(1)

    body = json.dumps({"method": args.method, "user": args.user, **extra})

    logger.info("Connecting to IoT Hub (service)…")
    registry_manager = IoTHubRegistryManager.from_connection_string(conn_str)

    props = {
        "contentType": "application/json",
        "contentEncoding": "utf-8",
    }

    logger.info("→ [%s]  method='%s'  body=%s", args.device_id, args.method, body)
    registry_manager.send_c2d_message(args.device_id, body, properties=props)
    logger.info("Message queued in IoT Hub for [%s].", args.device_id)


if __name__ == "__main__":
    main()
