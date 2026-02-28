"""
run_devices.py
--------------
Robust multi-device runner.

Starts one DeviceRunner per configured device (from .env), all concurrently.
Each runner handles reconnections automatically with exponential back-off.

To send telemetry, call runner.enqueue_nowait(raw_data) from a producer task.

Run from the project root:
    uv --directory raspberry run python scripts/run_devices.py
"""

import asyncio
import logging
import signal
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

# ─── sample payload ────────────────────────────────────────────────────────────
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

TELEMETRY_INTERVAL = 10  # seconds between simulated readings


async def producer(runners, interval: float) -> None:
    """Periodically enqueue the same sample payload to every runner."""
    while True:
        for runner in runners:
            runner.enqueue_nowait(SAMPLE_RAW_DATA)
        await asyncio.sleep(interval)


async def main() -> None:
    from src.devices import build_runners

    runners = build_runners()
    if not runners:
        print("ERROR: No devices configured in .env", file=sys.stderr)
        sys.exit(1)

    print(f"Starting {len(runners)} device runner(s):")
    for r in runners:
        print(f"  [{r.device_id}]  n_valves={r.device.n_valves}")

    loop = asyncio.get_running_loop()

    # Graceful shutdown on Ctrl+C / SIGTERM
    stop_event = asyncio.Event()

    def _shutdown(*_):
        print("\nShutting down…")
        stop_event.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _shutdown)

    # One task per runner + the producer + the stop-event waiter
    tasks = [
        asyncio.create_task(r.run(), name=r.device_id) for r in runners
    ]
    tasks.append(asyncio.create_task(producer(runners, TELEMETRY_INTERVAL)))
    tasks.append(asyncio.create_task(stop_event.wait()))

    try:
        done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            exc = task.exception() if not task.cancelled() else None
            if exc:
                print(f"ERROR in {task.get_name()}: {exc}", file=sys.stderr)
    finally:
        for runner in runners:
            await runner.stop()
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)


if __name__ == "__main__":
    asyncio.run(main())
