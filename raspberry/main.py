"""
Run from the project root:
    uv --directory raspberry run python main.py
"""

import asyncio
import logging
import signal

from src.devices import build_runners
from src.infra.database import close_db, get_db
from src.receiver import hmi_methods
from src.receiver.azure_iot_receiver import DirectMethodRegistry
from src.settings import settings

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)


def _build_method_registry() -> DirectMethodRegistry:
    """
    Build a :class:`DirectMethodRegistry` with one direct method per
    HMI handler (``run_hmi``, ``stop_hmi``, ``reset_hmi``, etc.).

    The backend can invoke any of them by name on a specific device.
    """
    registry = DirectMethodRegistry()

    for method_name, handler_fn in hmi_methods.HANDLERS.items():
        async def _handler(request, _fn=handler_fn):
            payload = request.payload or {}
            _fn(payload)
            return 200, {"result": True}

        registry.add_handler(method_name, _handler)

    return registry


async def main() -> None:
    runners = build_runners()

    # Eagerly open the database so data/database.db is created immediately.
    get_db()

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    # signal.signal works on both Windows and Linux
    def _shutdown(*_):
        stop_event.set()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # Prepare optional receive handlers
    on_c2d = None
    method_registry = None

    if settings.DEVICE.RECEIVE_DATA:
        logger.info("DEVICE_RECEIVE_DATA is enabled — registering C2D and direct method handlers.")

        def on_c2d_message(device_id: str, payload: dict) -> None:
            hmi_methods.dispatch(payload)

        on_c2d = on_c2d_message
        method_registry = _build_method_registry()
    else:
        logger.info("DEVICE_RECEIVE_DATA is disabled — running in send-only mode.")

    tasks = [
        asyncio.create_task(
            r.run(on_c2d_message=on_c2d, method_registry=method_registry),
            name=r.device_id,
        )
        for r in runners
    ]

    tasks.append(asyncio.create_task(stop_event.wait()))

    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            if not task.cancelled():
                exc = task.exception()
                if exc:
                    logging.error("Error in %s: %s", task.get_name(), exc)
    finally:
        for runner in runners:
            await runner.stop()
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
        close_db()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
