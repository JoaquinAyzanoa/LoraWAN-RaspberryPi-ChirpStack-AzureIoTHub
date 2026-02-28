"""
Lightweight async direct-method handler registry.

Does **not** own an ``IoTHubDeviceClient`` — handlers are plugged into the
client managed by :class:`~src.devices.runner.DeviceRunner`.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Coroutine

from azure.iot.device import MethodRequest, MethodResponse  # type: ignore[import]
from azure.iot.device.aio import IoTHubDeviceClient  # type: ignore[import]

logger = logging.getLogger(__name__)

# Handler type: receives a MethodRequest, returns (status_code, payload_dict)
MethodHandler = Callable[
    [MethodRequest],
    Coroutine[Any, Any, tuple[int, dict[str, Any]]],
]


class DirectMethodRegistry:
    """
    Collects ``method_name → async handler`` mappings and creates a
    dispatcher callback that can be assigned to
    ``client.on_method_request_received``.

    Usage
    -----
    ::

        registry = DirectMethodRegistry()

        @registry.register("reboot")
        async def handle_reboot(request):
            return 200, {"result": True}

        # Later, inside DeviceRunner.run():
        client.on_method_request_received = registry.create_dispatcher(
            client, device_id
        )
    """

    def __init__(self) -> None:
        self._handlers: dict[str, MethodHandler] = {}

    # ------------------------------------------------------------------
    # Handler registration
    # ------------------------------------------------------------------

    def register(self, method_name: str) -> Callable[[MethodHandler], MethodHandler]:
        """
        Decorator that registers an async handler for *method_name*.

        The decorated coroutine receives the raw :class:`MethodRequest` and
        must return a ``(status_code: int, payload: dict)`` tuple.

        Example::

            @registry.register("reboot")
            async def handle_reboot(request):
                return 200, {"result": True}
        """
        def decorator(fn: MethodHandler) -> MethodHandler:
            self._handlers[method_name] = fn
            logger.debug("Registered handler for method '%s'.", method_name)
            return fn
        return decorator

    def add_handler(self, method_name: str, handler: MethodHandler) -> None:
        """Programmatically register a handler (non-decorator form)."""
        self._handlers[method_name] = handler
        logger.debug("Registered handler for method '%s'.", method_name)

    @property
    def handlers(self) -> dict[str, MethodHandler]:
        """Read-only view of registered handlers."""
        return dict(self._handlers)

    # ------------------------------------------------------------------
    # Dispatcher factory
    # ------------------------------------------------------------------

    def create_dispatcher(
        self,
        client: IoTHubDeviceClient,
        device_id: str,
    ) -> Callable[[MethodRequest], Any]:
        """
        Return an async callback suitable for
        ``client.on_method_request_received``.

        The callback dispatches each incoming :class:`MethodRequest` to
        the matching registered handler and sends back a
        :class:`MethodResponse`.
        """
        handlers = self._handlers

        async def _dispatch(method_request: MethodRequest) -> None:
            name = method_request.name
            handler = handlers.get(name)

            if handler:
                logger.info("[%s] Invoking direct method '%s'.", device_id, name)
                try:
                    status, payload = await handler(method_request)
                except Exception as exc:  # noqa: BLE001
                    logger.exception(
                        "[%s] Handler for '%s' raised: %s", device_id, name, exc
                    )
                    status, payload = 500, {"result": False, "error": str(exc)}
            else:
                logger.warning("[%s] Unknown direct method '%s'.", device_id, name)
                status, payload = 400, {
                    "result": False,
                    "data": f"unknown method: {name}",
                }

            response = MethodResponse.create_from_method_request(
                method_request, status, payload
            )
            await client.send_method_response(response)
            logger.info(
                "[%s] Responded to '%s' with status %d.", device_id, name, status
            )

        return _dispatch
