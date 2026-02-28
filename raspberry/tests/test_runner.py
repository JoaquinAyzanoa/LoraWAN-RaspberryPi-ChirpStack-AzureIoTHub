"""
Tests for src/devices/runner.py — DeviceRunner lifecycle.

All IoT Hub network calls are mocked with AsyncMock.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


_FAKE_CONN_STR = (
    "HostName=test-hub.azure-devices.net;"
    "DeviceId=test-device;"
    "SharedAccessKey=AAAA="
)

SAMPLE_RAW_DATA = {
    "Estado_Equipo": True,
    "Alarma_Bajo_Nivel": {"Estado": False, "Grasa_Dispensada_Desde_Ultimo_Relleno": 0.0},
    "Bomba": {"Falla_Presion": False},
    "Valvula_V1": {
        "Estado": True,
        "Grasa_24h": 10.0,
        "Grasa_Dispensada_Desde_Ultimo_Relleno": 100.0,
        "Grasa_Ultimo_Ciclo": 1.0,
        "Longitud_Pulsos_Ultimo_Ciclo": 1,
        "Pulsos_Ultimo_Ciclo": [123],
    },
    "Valvula_V2": {
        "Estado": True,
        "Grasa_24h": 5.0,
        "Grasa_Dispensada_Desde_Ultimo_Relleno": 50.0,
        "Grasa_Ultimo_Ciclo": 0.5,
        "Longitud_Pulsos_Ultimo_Ciclo": 1,
        "Pulsos_Ultimo_Ciclo": [456],
    },
}


def _make_device(device_id: str = "test-device", n_valves: int = 2) -> MagicMock:
    device = MagicMock()
    device.device_id = device_id
    device.connection_string = _FAKE_CONN_STR
    device.n_valves = n_valves
    device.build_payload.return_value = '{"Estado_Equipo": true}'
    return device


def _make_async_client() -> MagicMock:
    client = MagicMock()
    client.connected = False
    client.connect = AsyncMock()
    client.shutdown = AsyncMock()
    client.send_message = AsyncMock()
    return client


# ---------------------------------------------------------------------------
# DeviceRunner — basic properties and enqueue
# ---------------------------------------------------------------------------


class TestDeviceRunnerProperties:
    def test_device_property(self):
        with patch("src.devices.runner.IoTHubDeviceClient"):
            from src.devices.runner import DeviceRunner
            device = _make_device()
            runner = DeviceRunner(device)
            assert runner.device is device
            assert runner.device_id == "test-device"

    def test_initial_events(self):
        with patch("src.devices.runner.IoTHubDeviceClient"):
            from src.devices.runner import DeviceRunner
            runner = DeviceRunner(_make_device())
            assert runner.disconnected_event.is_set()
            assert not runner.connected_event.is_set()
            assert not runner.exit_event.is_set()

    def test_enqueue_nowait(self):
        with patch("src.devices.runner.IoTHubDeviceClient"):
            from src.devices.runner import DeviceRunner
            runner = DeviceRunner(_make_device())
            runner.enqueue_nowait(SAMPLE_RAW_DATA)
            assert runner._queue.qsize() == 1

    def test_enqueue_async(self):
        with patch("src.devices.runner.IoTHubDeviceClient"):
            from src.devices.runner import DeviceRunner
            runner = DeviceRunner(_make_device())
            asyncio.run(runner.enqueue(SAMPLE_RAW_DATA))
            assert runner._queue.qsize() == 1


# ---------------------------------------------------------------------------
# DeviceRunner — connection state handler
# ---------------------------------------------------------------------------


class TestConnectionStateHandler:
    def test_connected_sets_events(self):
        with patch("src.devices.runner.IoTHubDeviceClient") as MockClass:
            from src.devices.runner import DeviceRunner
            client = _make_async_client()
            client.connected = True
            MockClass.create_from_connection_string.return_value = client

            runner = DeviceRunner(_make_device())

            async def _run():
                await runner._on_connection_state_change()

            asyncio.run(_run())
            assert runner.connected_event.is_set()
            assert not runner.disconnected_event.is_set()
            assert runner._retry_factor == 1  # reset

    def test_disconnected_sets_events(self):
        with patch("src.devices.runner.IoTHubDeviceClient") as MockClass:
            from src.devices.runner import DeviceRunner
            client = _make_async_client()
            client.connected = False
            MockClass.create_from_connection_string.return_value = client

            runner = DeviceRunner(_make_device())
            runner.connected_event.set()
            runner.disconnected_event.clear()

            async def _run():
                await runner._on_connection_state_change()

            asyncio.run(_run())
            assert runner.disconnected_event.is_set()
            assert not runner.connected_event.is_set()


# ---------------------------------------------------------------------------
# DeviceRunner — send loop
# ---------------------------------------------------------------------------


class TestSendLoop:
    def test_message_sent_when_connected(self):
        with patch("src.devices.runner.IoTHubDeviceClient") as MockClass:
            from src.devices.runner import DeviceRunner
            client = _make_async_client()
            client.connected = True
            MockClass.create_from_connection_string.return_value = client

            device = _make_device()
            runner = DeviceRunner(device)
            runner.connected_event.set()
            runner.disconnected_event.clear()
            runner.enqueue_nowait(SAMPLE_RAW_DATA)

            async def _run():
                # Run the send loop briefly then stop
                runner.enqueue_nowait(SAMPLE_RAW_DATA)
                send_task = asyncio.create_task(runner._send_loop())
                await asyncio.sleep(0.05)
                runner.exit_event.set()
                await asyncio.gather(send_task, return_exceptions=True)

            asyncio.run(_run())
            assert client.send_message.call_count >= 1

    def test_message_reenqueued_on_send_failure(self):
        with patch("src.devices.runner.IoTHubDeviceClient") as MockClass:
            from src.devices.runner import DeviceRunner
            client = _make_async_client()
            client.connected = True
            client.send_message = AsyncMock(side_effect=Exception("send failed"))
            MockClass.create_from_connection_string.return_value = client

            device = _make_device()
            runner = DeviceRunner(device)
            runner.connected_event.set()
            runner.disconnected_event.clear()
            runner.enqueue_nowait(SAMPLE_RAW_DATA)

            async def _run():
                send_task = asyncio.create_task(runner._send_loop())
                await asyncio.sleep(0.05)
                runner.exit_event.set()
                await asyncio.gather(send_task, return_exceptions=True)

            asyncio.run(_run())
            # Message should be re-enqueued after failure
            assert runner._queue.qsize() >= 1


# ---------------------------------------------------------------------------
# DeviceRunner — reconnect loop
# ---------------------------------------------------------------------------


class TestReconnectLoop:
    def test_connect_called_on_startup(self):
        with patch("src.devices.runner.IoTHubDeviceClient") as MockClass:
            from src.devices.runner import DeviceRunner
            client = _make_async_client()

            async def fake_connect():
                client.connected = True

            client.connect = AsyncMock(side_effect=fake_connect)
            MockClass.create_from_connection_string.return_value = client

            runner = DeviceRunner(_make_device())

            async def _run():
                reconnect_task = asyncio.create_task(runner._reconnect_loop())
                await asyncio.sleep(0.1)
                runner.exit_event.set()
                await asyncio.gather(reconnect_task, return_exceptions=True)

            asyncio.run(_run())
            client.connect.assert_called()

    def test_exit_event_stops_reconnect_loop(self):
        with patch("src.devices.runner.IoTHubDeviceClient") as MockClass:
            from src.devices.runner import DeviceRunner
            client = _make_async_client()
            client.connect = AsyncMock(side_effect=Exception("no network"))
            MockClass.create_from_connection_string.return_value = client

            runner = DeviceRunner(_make_device())
            runner.exit_event.set()  # immediately exit

            async def _run():
                # Should return quickly without hanging
                await asyncio.wait_for(runner._reconnect_loop(), timeout=1.0)

            asyncio.run(_run())  # Must not hang


# ---------------------------------------------------------------------------
# build_runners — factory
# ---------------------------------------------------------------------------


class TestBuildRunners:
    def test_returns_one_runner_per_device(self):
        fake_devices = [_make_device("d0"), _make_device("d1")]
        with (
            patch("src.devices.runner.IoTHubDeviceClient"),
            patch("src.devices.device.build_devices", return_value=fake_devices),
        ):
            from src.devices.runner import build_runners
            runners = build_runners()

        assert len(runners) == 2
        assert runners[0].device_id == "d0"
        assert runners[1].device_id == "d1"
