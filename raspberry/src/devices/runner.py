from __future__ import annotations

import asyncio
import json
import logging
from typing import Any, Callable

from azure.iot.device.aio import IoTHubDeviceClient  # type: ignore[import]
from azure.iot.device import Message  # type: ignore[import]

from .device import Device
from ..receiver.azure_iot_receiver import DirectMethodRegistry

logger = logging.getLogger(__name__)

# Exponential back-off settings
_INITIAL_RETRY_INTERVAL: float = 2.0   # seconds
_MAX_RETRY_INTERVAL: float = 7200.0    # 2 hours

# Maximum pending messages before the queue applies back-pressure
_QUEUE_MAX_SIZE: int = 100


async def _wait_first(*events: asyncio.Event) -> None:
    """Block until the first of *events* is set, then cancel the rest."""
    tasks = [asyncio.create_task(e.wait()) for e in events]
    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for t in pending:
            t.cancel()
            await asyncio.gather(t, return_exceptions=True)
    except asyncio.CancelledError:
        for t in tasks:
            t.cancel()
        raise


class DeviceRunner:
    """
    Robust async lifecycle manager for a single :class:`Device`.

    Owns the **sole** ``IoTHubDeviceClient`` for this device and handles:

    - Outgoing telemetry via an internal ``asyncio.Queue``
    - Incoming C2D messages (generic cloud-to-device)
    - Incoming direct method invocations (via :class:`DirectMethodRegistry`)
    - Auto-reconnect with exponential back-off

    Parameters
    ----------
    device:
        Configuration source — ``device_id``, ``connection_string``, and
        ``n_valves`` are used; the device's own client is not touched.
    """

    def __init__(self, device: Device) -> None:
        self._device = device
        self._client = IoTHubDeviceClient.create_from_connection_string(
            device.connection_string
        )
        self.connected_event = asyncio.Event()
        self.disconnected_event = asyncio.Event()
        self.exit_event = asyncio.Event()
        self.disconnected_event.set()   # start in "disconnected" state

        self._queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(
            maxsize=_QUEUE_MAX_SIZE
        )
        self._retry_factor: int = 0
        self._retry_interval: float = _INITIAL_RETRY_INTERVAL
        self._try_number: int = 1

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def device(self) -> Device:
        return self._device

    @property
    def device_id(self) -> str:
        return self._device.device_id

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def enqueue_nowait(self, raw_data: dict[str, Any]) -> None:
        """Non-blocking: add *raw_data* to the outgoing queue."""
        self._queue.put_nowait(raw_data)

    async def enqueue(self, raw_data: dict[str, Any]) -> None:
        """Async: add *raw_data* to the outgoing queue."""
        await self._queue.put(raw_data)

    async def stop(self) -> None:
        """Signal the runner to shut down gracefully."""
        self.exit_event.set()

    # ------------------------------------------------------------------
    # Internal: connection-state handler
    # ------------------------------------------------------------------

    async def _on_connection_state_change(self) -> None:
        if self._client.connected:
            logger.info("[%s] Connected to Azure IoT Hub.", self.device_id)
            self.disconnected_event.clear()
            self.connected_event.set()
            # Reset back-off on success
            self._retry_factor = 0
            self._retry_interval = _INITIAL_RETRY_INTERVAL
            self._try_number = 1
        else:
            logger.info("[%s] Disconnected from Azure IoT Hub.", self.device_id)
            self.connected_event.clear()
            self.disconnected_event.set()

    # ------------------------------------------------------------------
    # Internal: reconnect loop
    # ------------------------------------------------------------------

    async def _reconnect_loop(self) -> None:
        while True:
            # Wait until disconnected or asked to exit
            await _wait_first(self.disconnected_event, self.exit_event)
            if self.exit_event.is_set():
                return

            while not self._client.connected:
                if self.exit_event.is_set():
                    return
                try:
                    logger.info(
                        "[%s] Connection attempt %d…",
                        self.device_id,
                        self._try_number,
                    )
                    await self._client.connect()
                    logger.info("[%s] Successfully connected.", self.device_id)
                except Exception as exc:  # noqa: BLE001
                    sleep = min(
                        _INITIAL_RETRY_INTERVAL * (2 ** self._retry_factor),
                        _MAX_RETRY_INTERVAL,
                    )
                    if sleep >= _MAX_RETRY_INTERVAL:
                        logger.error(
                            "[%s] Max retry interval (%.0fs) exceeded. Stopping.",
                            self.device_id,
                            _MAX_RETRY_INTERVAL,
                        )
                        self.exit_event.set()
                        raise RuntimeError(
                            f"[{self.device_id}] Could not reconnect within the retry limit."
                        ) from exc

                    logger.warning(
                        "[%s] Attempt %d failed (%s). Retrying in %.1fs.",
                        self.device_id,
                        self._try_number,
                        type(exc).__name__,
                        sleep,
                    )
                    self._retry_factor += 1
                    self._try_number += 1
                    await asyncio.sleep(sleep)

    # ------------------------------------------------------------------
    # Internal: send loop
    # ------------------------------------------------------------------

    async def _send_loop(self) -> None:
        while True:
            # If not connected, wait
            if not self._client.connected:
                logger.info("[%s] Waiting for connection before sending…", self.device_id)
                await _wait_first(self.connected_event, self.exit_event)
                if self.exit_event.is_set():
                    return

            # Wait for a queued message or exit signal
            get_task = asyncio.create_task(self._queue.get())
            exit_task = asyncio.create_task(self.exit_event.wait())
            done, pending = await asyncio.wait(
                [get_task, exit_task], return_when=asyncio.FIRST_COMPLETED
            )
            for t in pending:
                t.cancel()
                await asyncio.gather(t, return_exceptions=True)

            if self.exit_event.is_set():
                # If we already pulled a message, put it back
                if get_task in done and not get_task.cancelled():
                    try:
                        self._queue.put_nowait(get_task.result())
                    except Exception:  # noqa: BLE001
                        pass
                return

            raw_data: dict[str, Any] = get_task.result()

            try:
                payload = self._device.build_payload(raw_data)
                message = Message(
                    payload,
                    content_encoding="utf-8",
                    content_type="application/json",
                )
                await self._client.send_message(message)
                self._queue.task_done()
                logger.info(
                    "[%s] Telemetry sent (%d valves, %d bytes).",
                    self.device_id,
                    self._device.n_valves,
                    len(payload),
                )
            except Exception as exc:  # noqa: BLE001
                logger.warning(
                    "[%s] Send failed (%s) — re-enqueuing message.",
                    self.device_id,
                    type(exc).__name__,
                )
                self._queue.put_nowait(raw_data)

    # ------------------------------------------------------------------
    # run
    # ------------------------------------------------------------------

    async def run(
        self,
        on_c2d_message: Callable[[str, Any], None] | None = None,
        method_registry: DirectMethodRegistry | None = None,
    ) -> None:
        """
        Connect and run indefinitely.

        Starts the reconnect and send loops as concurrent tasks.
        Optionally handles C2D messages and direct method invocations.

        Parameters
        ----------
        on_c2d_message:
            Callback ``(device_id, payload)`` invoked for each generic
            cloud-to-device message.
        method_registry:
            A :class:`DirectMethodRegistry` whose handlers will be
            dispatched for incoming direct method invocations.
        """
        self._client.on_connection_state_change = self._on_connection_state_change

        # --- C2D message handler ---
        if on_c2d_message:
            def _c2d_handler(message: Any) -> None:
                raw = message.data
                body = raw.decode("utf-8") if isinstance(raw, (bytes, bytearray)) else raw
                try:
                    payload = json.loads(body)
                except (json.JSONDecodeError, TypeError):
                    payload = body
                logger.info("[%s] C2D message received.", self.device_id)
                on_c2d_message(self.device_id, payload)

            self._client.on_message_received = _c2d_handler

        # --- Direct method handler ---
        if method_registry:
            dispatcher = method_registry.create_dispatcher(
                self._client, self.device_id
            )
            self._client.on_method_request_received = dispatcher

        reconnect_task = asyncio.create_task(
            self._reconnect_loop(), name=f"{self.device_id}-reconnect"
        )
        send_task = asyncio.create_task(
            self._send_loop(), name=f"{self.device_id}-send"
        )
        tasks = {reconnect_task, send_task}

        try:
            done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
            for task in done:
                task.result()   # re-raise any exception
        finally:
            self.exit_event.set()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            self._client.on_message_received = None
            self._client.on_method_request_received = None
            try:
                await self._client.shutdown()
            except Exception:  # noqa: BLE001
                pass
            logger.info("[%s] DeviceRunner stopped.", self.device_id)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def build_runners() -> list[DeviceRunner]:
    """
    Instantiate one :class:`DeviceRunner` per configured device.

    Delegates to :func:`~src.devices.build_devices`.
    """
    from .device import build_devices  # local import avoids circular init
    return [DeviceRunner(device) for device in build_devices()]
