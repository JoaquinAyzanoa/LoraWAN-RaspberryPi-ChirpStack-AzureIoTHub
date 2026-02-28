"""
receive_direct_methods.py
-------------------------
Integration script — connects to Azure IoT Hub and handles direct method
invocations for all configured devices.

Run from the project root:
    uv --directory raspberry run python scripts/receive_direct_methods.py

Press Q + Enter (or Ctrl+C) to stop.
"""

import asyncio
import logging
from typing import Any
import sys
from pathlib import Path
# Make sure 'src' is importable when the script is run directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from azure.iot.device import MethodRequest  # type: ignore[import]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


async def main() -> None:
    from src.receiver.direct_method_receiver import build_method_receivers

    receivers = build_method_receivers()

    if not receivers:
        import sys
        print("ERROR: No devices found. Check DEVICE_IDS in .env", file=sys.stderr)
        sys.exit(1)

    print(f"Registering direct method handlers for {len(receivers)} device(s):")
    for rx in receivers:
        print(f"  [{rx.device_id}]")

    # ----------------------------------------------------------------
    # Register handlers on every receiver
    # ----------------------------------------------------------------
    for rx in receivers:

        @rx.register("ping")
        async def handle_ping(request: MethodRequest) -> tuple[int, dict[str, Any]]:
            print(f"[{rx.device_id}] ping received — payload: {request.payload}")
            return 200, {"result": True, "pong": True}

        @rx.register("reboot")
        async def handle_reboot(request: MethodRequest) -> tuple[int, dict[str, Any]]:
            print(f"[{rx.device_id}] reboot requested — payload: {request.payload}")
            # TODO: trigger actual device reboot here
            return 200, {"result": True, "message": "reboot scheduled"}

    print("\nWaiting for direct method calls… (Q + Enter to stop)\n")

    # Run all receivers concurrently
    await asyncio.gather(*(rx.run() for rx in receivers))


if __name__ == "__main__":
    asyncio.run(main())
