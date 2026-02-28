from __future__ import annotations

import logging
from typing import Any

from azure.iot.device.aio import IoTHubDeviceClient  # type: ignore[import]
from azure.iot.device import Message  # type: ignore[import]

from ..sender.payload_builder import build_payload as _build_payload
from ..settings import settings

logger = logging.getLogger(__name__)


class Device:
    """
    Represents a single LoRaWAN field device connected to Azure IoT Hub.

    Properties
    ----------
    device_id : str
        Unique device identifier (matches the Azure IoT Hub device ID).
    connection_string : str
        Full Azure IoT Hub connection string for this device.
    n_valves : int
        Number of valves on this device. Only Valvula_V1…Valvula_VN are
        included in the telemetry payload.
    """

    def __init__(
        self,
        device_id: str,
        connection_string: str,
        n_valves: int,
    ) -> None:
        self._device_id = device_id
        self._connection_string = connection_string
        self._n_valves = n_valves
        self._client = IoTHubDeviceClient.create_from_connection_string(
            connection_string
        )

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def device_id(self) -> str:
        return self._device_id

    @property
    def connection_string(self) -> str:
        return self._connection_string

    @property
    def n_valves(self) -> int:
        return self._n_valves

    # ------------------------------------------------------------------
    # Payload
    # ------------------------------------------------------------------

    def build_payload(self, raw_data: dict[str, Any]) -> str:
        """
        Build the JSON telemetry string for this device.

        Only the first ``n_valves`` valve entries (Valvula_V1…Valvula_VN)
        are included, so each device sends only its own valves.
        """
        return _build_payload(raw_data, self._n_valves)

    # ------------------------------------------------------------------
    # Async connection lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Open the async MQTT connection to Azure IoT Hub."""
        await self._client.connect()
        logger.info("[%s] Connected to Azure IoT Hub.", self._device_id)

    async def shutdown(self) -> None:
        """Gracefully shut down the connection to Azure IoT Hub."""
        await self._client.shutdown()
        logger.info("[%s] Disconnected from Azure IoT Hub.", self._device_id)

    async def __aenter__(self) -> Device:
        await self.connect()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.shutdown()

    # ------------------------------------------------------------------
    # Send
    # ------------------------------------------------------------------

    async def send(self, raw_data: dict[str, Any]) -> None:
        """
        Build a device-specific payload from *raw_data* and send it to
        Azure IoT Hub.  Call ``connect()`` first, or use as an async context manager.
        """
        json_payload = self.build_payload(raw_data)
        message = Message(
            json_payload,
            content_encoding="utf-8",
            content_type="application/json",
        )
        await self._client.send_message(message)
        logger.info(
            "[%s] Telemetry sent (%d valves, %d bytes).",
            self._device_id,
            self._n_valves,
            len(json_payload),
        )


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def build_devices() -> list[Device]:
    """
    Instantiate one :class:`Device` per entry in ``settings.DEVICE``.

    Raises
    ------
    ValueError
        If the three lists have different lengths.
    """
    conn_strings = settings.DEVICE.CONNECTION_STRINGS
    device_ids = settings.DEVICE.IDS
    n_valves_list = settings.DEVICE.N_VALVES

    if not (len(conn_strings) == len(device_ids) == len(n_valves_list)):
        raise ValueError(
            "DEVICE_CONNECTION_STRINGS, DEVICE_IDS, and DEVICE_N_VALVES "
            "must have the same number of entries. Got: "
            f"{len(conn_strings)}, {len(device_ids)}, {len(n_valves_list)}."
        )

    return [
        Device(
            device_id=did,
            connection_string=cs,
            n_valves=nv,
        )
        for did, cs, nv in zip(device_ids, conn_strings, n_valves_list)
    ]
