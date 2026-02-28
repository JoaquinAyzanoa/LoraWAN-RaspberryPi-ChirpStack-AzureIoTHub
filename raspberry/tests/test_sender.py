"""
Tests for src/sender — payload building and Azure IoT Hub dispatch.

All Azure IoT Hub network calls are mocked (AsyncMock for async methods)
so no real connection is needed.
Run from raspberry/ with: pytest tests/test_sender.py -v
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.sender.payload_builder import build_payload
from src.sender.azure_iot_sender import AzureIoTSender, send_all_devices
from src.devices.device import Device, build_devices


# ---------------------------------------------------------------------------
# Sample device data (4-valve device — mirrors the real payload structure)
# ---------------------------------------------------------------------------

SAMPLE_RAW_DATA = {
    "Alarma_Bajo_Nivel": {
        "Estado": True,
        "Grasa_Dispensada_Desde_Ultimo_Relleno": 5699.85,
    },
    "Bomba": {"Falla_Presion": True},
    "Estado_Equipo": True,
    "Valvula_V1": {
        "Estado": True,
        "Grasa_24h": 79.95,
        "Grasa_Dispensada_Desde_Ultimo_Relleno": 2202.85,
        "Grasa_Ultimo_Ciclo": 5.2,
        "Longitud_Pulsos_Ultimo_Ciclo": 8,
        "Pulsos_Ultimo_Ciclo": [947657322],
    },
    "Valvula_V2": {
        "Estado": True,
        "Grasa_24h": 20.15,
        "Grasa_Dispensada_Desde_Ultimo_Relleno": 583.05,
        "Grasa_Ultimo_Ciclo": 1.3,
        "Longitud_Pulsos_Ultimo_Ciclo": 2,
        "Pulsos_Ultimo_Ciclo": [947657322],
    },
    "Valvula_V3": {
        "Estado": True,
        "Grasa_24h": 82.55,
        "Grasa_Dispensada_Desde_Ultimo_Relleno": 2262.65,
        "Grasa_Ultimo_Ciclo": 5.2,
        "Longitud_Pulsos_Ultimo_Ciclo": 8,
        "Pulsos_Ultimo_Ciclo": [947657322],
    },
    "Valvula_V4": {
        "Estado": True,
        "Grasa_24h": 21.45,
        "Grasa_Dispensada_Desde_Ultimo_Relleno": 651.3,
        "Grasa_Ultimo_Ciclo": 1.3,
        "Longitud_Pulsos_Ultimo_Ciclo": 2,
        "Pulsos_Ultimo_Ciclo": [947657322],
    },
}


# ---------------------------------------------------------------------------
# build_payload — pure function, no mocks needed
# ---------------------------------------------------------------------------


class TestBuildPayload:
    def test_returns_valid_json(self):
        result = build_payload(SAMPLE_RAW_DATA, n_valves=4)
        parsed = json.loads(result)  # must not raise
        assert isinstance(parsed, dict)

    def test_top_level_keys_present(self):
        parsed = json.loads(build_payload(SAMPLE_RAW_DATA, n_valves=4))
        assert "Alarma_Bajo_Nivel" in parsed
        assert "Bomba" in parsed
        assert "Estado_Equipo" in parsed

    def test_correct_number_of_valves(self):
        for n in (1, 2, 3, 4):
            parsed = json.loads(build_payload(SAMPLE_RAW_DATA, n_valves=n))
            valve_keys = [k for k in parsed if k.startswith("Valvula_V")]
            assert len(valve_keys) == n, f"Expected {n} valves, got {len(valve_keys)}"

    def test_valve_fields_present(self):
        parsed = json.loads(build_payload(SAMPLE_RAW_DATA, n_valves=4))
        valve = parsed["Valvula_V1"]
        expected_fields = {
            "Estado", "Grasa_24h", "Grasa_Dispensada_Desde_Ultimo_Relleno",
            "Grasa_Ultimo_Ciclo", "Longitud_Pulsos_Ultimo_Ciclo",
            "Pulsos_Ultimo_Ciclo",
        }
        assert expected_fields == set(valve.keys())

    def test_valve_values_match_input(self):
        parsed = json.loads(build_payload(SAMPLE_RAW_DATA, n_valves=4))
        assert parsed["Valvula_V1"]["Grasa_24h"] == 79.95
        assert parsed["Valvula_V3"]["Pulsos_Ultimo_Ciclo"] == [947657322]
        assert parsed["Valvula_V4"]["Longitud_Pulsos_Ultimo_Ciclo"] == 2

    def test_alarma_bajo_nivel_values(self):
        parsed = json.loads(build_payload(SAMPLE_RAW_DATA, n_valves=4))
        assert parsed["Alarma_Bajo_Nivel"]["Estado"] is True
        assert parsed["Alarma_Bajo_Nivel"]["Grasa_Dispensada_Desde_Ultimo_Relleno"] == 5699.85

    def test_bomba_falla_presion(self):
        parsed = json.loads(build_payload(SAMPLE_RAW_DATA, n_valves=4))
        assert parsed["Bomba"]["Falla_Presion"] is True


class TestBuildPayloadValidation:
    """Tests for input validation in build_payload."""

    def test_missing_top_level_key_raises(self):
        bad_data = {k: v for k, v in SAMPLE_RAW_DATA.items() if k != "Bomba"}
        with pytest.raises(ValueError, match="Bomba"):
            build_payload(bad_data, n_valves=1)

    def test_missing_valve_key_raises(self):
        # Only provide V1, but request 2 valves
        data = {
            "Alarma_Bajo_Nivel": SAMPLE_RAW_DATA["Alarma_Bajo_Nivel"],
            "Bomba": SAMPLE_RAW_DATA["Bomba"],
            "Estado_Equipo": True,
            "Valvula_V1": SAMPLE_RAW_DATA["Valvula_V1"],
        }
        with pytest.raises(ValueError, match="Valvula_V2"):
            build_payload(data, n_valves=2)

    def test_missing_valve_field_raises(self):
        data = dict(SAMPLE_RAW_DATA)
        data["Valvula_V1"] = {"Estado": True}  # missing other fields
        with pytest.raises(ValueError, match="Grasa_24h"):
            build_payload(data, n_valves=1)


# ---------------------------------------------------------------------------
# AzureIoTSender — async, IoTHubDeviceClient is mocked with AsyncMock
# ---------------------------------------------------------------------------

_FAKE_CONN_STR = (
    "HostName=test-hub.azure-devices.net;"
    "DeviceId=test-device;"
    "SharedAccessKey=AAAA="
)


def _make_async_client():
    """Return a MagicMock with async SDK methods replaced by AsyncMock."""
    instance = MagicMock()
    instance.connect = AsyncMock()
    instance.shutdown = AsyncMock()
    instance.send_message = AsyncMock()
    return instance


@pytest.fixture()
def mock_client():
    """Patch IoTHubDeviceClient (aio) inside src.sender.azure_iot_sender."""
    with patch("src.sender.azure_iot_sender.IoTHubDeviceClient") as MockClass:
        instance = _make_async_client()
        MockClass.create_from_connection_string.return_value = instance
        yield instance


class TestAzureIoTSender:
    def test_send_calls_send_message(self, mock_client):
        async def _run():
            sender = AzureIoTSender(_FAKE_CONN_STR, device_id="test-device", n_valves=4)
            await sender.connect()
            await sender.send(SAMPLE_RAW_DATA)
        asyncio.run(_run())
        mock_client.send_message.assert_called_once()

    def test_send_message_contains_valid_json(self, mock_client):
        async def _run():
            sender = AzureIoTSender(_FAKE_CONN_STR, device_id="test-device", n_valves=4)
            await sender.connect()
            await sender.send(SAMPLE_RAW_DATA)
        asyncio.run(_run())
        sent_message = mock_client.send_message.call_args[0][0]
        payload = json.loads(sent_message.data)
        assert "Valvula_V1" in payload
        assert "Valvula_V4" in payload

    def test_send_message_content_type_json(self, mock_client):
        async def _run():
            sender = AzureIoTSender(_FAKE_CONN_STR, device_id="test-device", n_valves=4)
            await sender.connect()
            await sender.send(SAMPLE_RAW_DATA)
        asyncio.run(_run())
        sent_message = mock_client.send_message.call_args[0][0]
        assert sent_message.content_type == "application/json"

    def test_connect_and_shutdown_called(self, mock_client):
        async def _run():
            sender = AzureIoTSender(_FAKE_CONN_STR, device_id="test-device", n_valves=4)
            await sender.connect()
            await sender.shutdown()
        asyncio.run(_run())
        mock_client.connect.assert_called_once()
        mock_client.shutdown.assert_called_once()

    def test_async_context_manager(self, mock_client):
        async def _run():
            async with AzureIoTSender(_FAKE_CONN_STR, device_id="test-device", n_valves=4) as sender:
                await sender.send(SAMPLE_RAW_DATA)
        asyncio.run(_run())
        mock_client.connect.assert_called_once()
        mock_client.shutdown.assert_called_once()
        mock_client.send_message.assert_called_once()

    def test_n_valves_respected(self, mock_client):
        """A 2-valve sender must not include Valvula_V3 or V4 in the payload."""
        async def _run():
            async with AzureIoTSender(_FAKE_CONN_STR, device_id="test-device", n_valves=2) as sender:
                await sender.send(SAMPLE_RAW_DATA)
        asyncio.run(_run())
        sent_message = mock_client.send_message.call_args[0][0]
        payload = json.loads(sent_message.data)
        assert "Valvula_V1" in payload
        assert "Valvula_V2" in payload
        assert "Valvula_V3" not in payload
        assert "Valvula_V4" not in payload


# ---------------------------------------------------------------------------
# Device — pure data model (no client, no connect/shutdown/send)
# ---------------------------------------------------------------------------


class TestDevice:
    def test_properties(self):
        device = Device(
            device_id="pulse-id-100",
            connection_string=_FAKE_CONN_STR,
            n_valves=4,
        )
        assert device.device_id == "pulse-id-100"
        assert device.connection_string == _FAKE_CONN_STR
        assert device.n_valves == 4

    def test_build_payload_slices_correct_valves(self):
        """A 2-valve Device must only include V1 and V2 in its payload."""
        device = Device(device_id="d", connection_string=_FAKE_CONN_STR, n_valves=2)
        parsed = json.loads(device.build_payload(SAMPLE_RAW_DATA))
        assert "Valvula_V1" in parsed
        assert "Valvula_V2" in parsed
        assert "Valvula_V3" not in parsed
        assert "Valvula_V4" not in parsed

    def test_build_payload_4_valves(self):
        device = Device(device_id="d", connection_string=_FAKE_CONN_STR, n_valves=4)
        parsed = json.loads(device.build_payload(SAMPLE_RAW_DATA))
        assert len([k for k in parsed if k.startswith("Valvula_V")]) == 4

    def test_build_devices_returns_correct_count(self):
        with patch("src.devices.device.settings") as mock_settings:
            mock_settings.DEVICE.CONNECTION_STRINGS = [_FAKE_CONN_STR, _FAKE_CONN_STR]
            mock_settings.DEVICE.IDS = ["device-0", "device-1"]
            mock_settings.DEVICE.N_VALVES = [4, 2]
            devices = build_devices()
        assert len(devices) == 2
        assert devices[0].device_id == "device-0"
        assert devices[0].n_valves == 4
        assert devices[1].device_id == "device-1"
        assert devices[1].n_valves == 2

    def test_build_devices_raises_on_length_mismatch(self):
        with patch("src.devices.device.settings") as mock_settings:
            mock_settings.DEVICE.CONNECTION_STRINGS = [_FAKE_CONN_STR]
            mock_settings.DEVICE.IDS = ["device-0", "device-1"]  # mismatch
            mock_settings.DEVICE.N_VALVES = [4]
            with pytest.raises(ValueError, match="same number of entries"):
                build_devices()


# ---------------------------------------------------------------------------
# send_all_devices — verifies multi-device async dispatch
# ---------------------------------------------------------------------------


class TestSendAllDevices:
    def test_sends_to_every_configured_device(self):
        """send_all_devices must send one message per configured Device."""
        mock_instance = _make_async_client()

        with (
            patch("src.sender.azure_iot_sender.IoTHubDeviceClient") as MockClass,
            patch("src.devices.device.settings") as mock_settings,
        ):
            MockClass.create_from_connection_string.return_value = mock_instance
            mock_settings.DEVICE.CONNECTION_STRINGS = [_FAKE_CONN_STR, _FAKE_CONN_STR]
            mock_settings.DEVICE.IDS = ["device-0", "device-1"]
            mock_settings.DEVICE.N_VALVES = [4, 2]

            asyncio.run(send_all_devices(SAMPLE_RAW_DATA))

        # Two devices → two send_message calls
        assert mock_instance.send_message.call_count == 2

    def test_mismatched_lists_raise_value_error(self):
        with (
            patch("src.sender.azure_iot_sender.IoTHubDeviceClient"),
            patch("src.devices.device.settings") as mock_settings,
        ):
            mock_settings.DEVICE.CONNECTION_STRINGS = [_FAKE_CONN_STR]
            mock_settings.DEVICE.IDS = ["device-0", "device-1"]  # length mismatch
            mock_settings.DEVICE.N_VALVES = [4]

            with pytest.raises(ValueError, match="same number of entries"):
                asyncio.run(send_all_devices(SAMPLE_RAW_DATA))
