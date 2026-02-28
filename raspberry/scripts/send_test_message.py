"""
send_test_message.py
--------------------
Integration script — sends a real sample payload to Azure IoT Hub.
Run from the raspberry/ directory with the .venv active:

    python -m scripts.send_test_message

Reads DEVICE_CONNECTION_STRINGS, DEVICE_IDS, and DEVICE_N_VALVES from .env.
Each device receives only its own valves (Valvula_V1 … Valvula_VN).
"""

import asyncio
import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

# Sample payload — contains the maximum number of valves across all devices.
# Each Device will slice only the valves it owns (determined by n_valves).
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


async def main() -> None:
    from src.devices import build_devices

    devices = build_devices()

    if not devices:
        print("ERROR: No devices found. Check DEVICE_IDS in .env", file=sys.stderr)
        sys.exit(1)

    print(f"Sending to {len(devices)} device(s):")
    for d in devices:
        print(f"  [{d.device_id}]  n_valves={d.n_valves}")

    # Send to all devices concurrently
    async def _send(device):
        async with device:
            await device.send(SAMPLE_RAW_DATA)

    await asyncio.gather(*(_send(d) for d in devices))

    print("Done — check Azure IoT Explorer for incoming telemetry.")


if __name__ == "__main__":
    asyncio.run(main())
