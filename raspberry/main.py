"""
Run from the project root:
    uv --directory raspberry run python main.py
"""

import asyncio
import logging
import signal

from src.devices import build_runners
from src.receiver import hmi_methods

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)


async def main() -> None:
    runners = build_runners()

    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    # signal.signal works on both Windows and Linux
    def _shutdown(*_):
        stop_event.set()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    # Start receivers to listen for C2D messages
    def on_c2d_message(device_id: str, payload: dict) -> None:
        hmi_methods.dispatch(payload)

    tasks = [asyncio.create_task(r.run(on_c2d_message), name=r.device_id) for r in runners]
    
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


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
