from __future__ import annotations

import logging
from typing import Any

from ..sender.payload_builder import build_payload as _build_payload
from ..settings import settings

logger = logging.getLogger(__name__)


class Device:
    """
    Pure data model for a single LoRaWAN field device connected to Azure IoT Hub.

    This class holds configuration only — it does **not** create or own an
    ``IoTHubDeviceClient``.  Connection lifecycle is managed by
    :class:`~src.devices.runner.DeviceRunner`.

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
