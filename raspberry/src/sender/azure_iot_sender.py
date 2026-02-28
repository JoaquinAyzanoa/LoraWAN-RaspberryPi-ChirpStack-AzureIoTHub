from __future__ import annotations

import logging
from typing import Any

from azure.iot.device.aio import IoTHubDeviceClient  # type: ignore[import]
from azure.iot.device import Message  # type: ignore[import]

from .payload_builder import build_payload
from ..settings import settings

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Single-device low-level sender (async transport)
# ---------------------------------------------------------------------------


class AzureIoTSender:
    """
    Async low-level IoT Hub transport for a single device.

    For most use-cases prefer :class:`~src.devices.runner.DeviceRunner`, which
    manages the full lifecycle (reconnect, send queue, C2D, direct methods).

    Parameters
    ----------
    connection_string:
        Full Azure IoT Hub connection string for this device.
    device_id:
        Human-readable identifier (used in log messages).
    n_valves:
        Number of valves — controls which valve keys are included in the payload.
    """

    def __init__(
        self,
        connection_string: str,
        device_id: str,
        n_valves: int,
    ) -> None:
        self.device_id = device_id
        self.n_valves = n_valves
        self._client = IoTHubDeviceClient.create_from_connection_string(
            connection_string
        )

    async def connect(self) -> None:
        """Open the async MQTT connection to IoT Hub."""
        await self._client.connect()
        logger.info("[%s] Connected to Azure IoT Hub.", self.device_id)

    async def shutdown(self) -> None:
        """Gracefully shut down the connection to IoT Hub."""
        await self._client.shutdown()
        logger.info("[%s] Disconnected from Azure IoT Hub.", self.device_id)

    async def send(self, raw_data: dict[str, Any]) -> None:
        """Build payload and send it. Call ``connect()`` first."""
        json_payload = build_payload(raw_data, self.n_valves)
        message = Message(json_payload, content_encoding="utf-8", content_type="application/json")
        await self._client.send_message(message)
        logger.info(
            "[%s] Telemetry sent (%d valves, %d bytes).",
            self.device_id,
            self.n_valves,
            len(json_payload),
        )

    async def __aenter__(self) -> "AzureIoTSender":
        await self.connect()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.shutdown()


# ---------------------------------------------------------------------------
# Multi-device helper — uses DeviceRunner.enqueue for queued delivery
# ---------------------------------------------------------------------------


async def send_all_devices(raw_data: dict[str, Any]) -> None:
    """
    Build the correct per-device payload and send it to every configured
    Azure IoT Hub device concurrently.

    Each device creates a temporary connection, sends the message, then
    shuts down.  For long-running use, prefer :class:`DeviceRunner`.
    """
    import asyncio
    from ..devices import build_devices

    async def _send_one(device: Any) -> None:
        async with AzureIoTSender(
            connection_string=device.connection_string,
            device_id=device.device_id,
            n_valves=device.n_valves,
        ) as sender:
            await sender.send(raw_data)

    await asyncio.gather(*(_send_one(d) for d in build_devices()))
