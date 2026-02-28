from __future__ import annotations

import asyncio
import logging
from typing import Any, Callable, Coroutine

from azure.iot.device.aio import IoTHubDeviceClient  # type: ignore[import]
from azure.iot.device import MethodRequest, MethodResponse  # type: ignore[import]

from ..settings import settings

logger = logging.getLogger(__name__)

# Handler type: receives a MethodRequest, returns (status_code, payload_dict)
MethodHandler = Callable[
    [MethodRequest],
    Coroutine[Any, Any, tuple[int, dict[str, Any]]],
]


class AzureIoTReceiver:
    """
    Async listener for Azure IoT Hub direct method invocations.

    Properties
    ----------
    device_id : str
        Unique device identifier.
    connection_string : str
        Full Azure IoT Hub connection string for this device.

    Unknown method names return an HTTP-like 400 response automatically.
    """

    def __init__(self, device_id: str, connection_string: str) -> None:
        self._device_id = device_id
        self._connection_string = connection_string
        self._client = IoTHubDeviceClient.create_from_connection_string(
            connection_string
        )
        self._handlers: dict[str, MethodHandler] = {}

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def device_id(self) -> str:
        return self._device_id

    @property
    def connection_string(self) -> str:
        return self._connection_string

    # ------------------------------------------------------------------
    # Handler registration
    # ------------------------------------------------------------------

    def register(self, method_name: str) -> Callable[[MethodHandler], MethodHandler]:
        """
        Decorator that registers an async handler for *method_name*.

        The decorated coroutine receives the raw :class:`MethodRequest` and
        must return a ``(status_code: int, payload: dict)`` tuple.

        Example::

            @rx.register("reboot")
            async def handle_reboot(request):
                return 200, {"result": True}
        """
        def decorator(fn: MethodHandler) -> MethodHandler:
            self._handlers[method_name] = fn
            logger.debug("[%s] Registered handler for method '%s'.", self._device_id, method_name)
            return fn
        return decorator

    # ------------------------------------------------------------------
    # Internal dispatcher
    # ------------------------------------------------------------------

    async def _dispatch(self, method_request: MethodRequest) -> None:
        name = method_request.name
        handler = self._handlers.get(name)

        if handler:
            logger.info("[%s] Invoking method '%s'.", self._device_id, name)
            try:
                status, payload = await handler(method_request)
            except Exception as exc:  # noqa: BLE001
                logger.exception("[%s] Handler for '%s' raised: %s", self._device_id, name, exc)
                status, payload = 500, {"result": False, "error": str(exc)}
        else:
            logger.warning("[%s] Unknown method '%s'.", self._device_id, name)
            status, payload = 400, {"result": False, "data": f"unknown method: {name}"}

        response = MethodResponse.create_from_method_request(method_request, status, payload)
        await self._client.send_method_response(response)
        logger.info("[%s] Responded to '%s' with status %d.", self._device_id, name, status)

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def run(self) -> None:
        """
        Connect to IoT Hub, start dispatching method requests, and block
        until :meth:`shutdown` is called or a ``KeyboardInterrupt`` is raised.
        """
        await self._client.connect()
        logger.info("[%s] Connected — waiting for direct method calls.", self._device_id)

        self._client.on_method_request_received = self._dispatch

        try:
            await asyncio.get_running_loop().run_in_executor(None, self._wait_for_quit)
        except asyncio.CancelledError:
            pass
        finally:
            await self.shutdown()

    async def shutdown(self) -> None:
        """Unregister the handler and gracefully disconnect."""
        self._client.on_method_request_received = None
        await self._client.shutdown()
        logger.info("[%s] AzureIoTReceiver shut down.", self._device_id)

    @staticmethod
    def _wait_for_quit() -> None:
        """Block the executor thread until the user presses Q."""
        while True:
            try:
                key = input("Press Q + Enter to quit\n")
                if key.strip().lower() == "q":
                    break
            except EOFError:
                break


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------


def build_receivers() -> list[AzureIoTReceiver]:
    """
    Instantiate one :class:`AzureIoTReceiver` per entry in
    ``settings.DEVICE``.

    Raises
    ------
    ValueError
        If DEVICE_CONNECTION_STRINGS and DEVICE_IDS have different lengths.
    """
    conn_strings = settings.DEVICE.CONNECTION_STRINGS
    device_ids = settings.DEVICE.IDS

    if len(conn_strings) != len(device_ids):
        raise ValueError(
            "DEVICE_CONNECTION_STRINGS and DEVICE_IDS must have the same number "
            f"of entries. Got: {len(conn_strings)}, {len(device_ids)}."
        )

    return [
        AzureIoTReceiver(device_id=did, connection_string=cs)
        for did, cs in zip(device_ids, conn_strings)
    ]
