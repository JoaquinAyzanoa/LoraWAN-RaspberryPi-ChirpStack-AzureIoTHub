from __future__ import annotations

import json
from typing import Any


# ---------------------------------------------------------------------------
# Valve data model
# ---------------------------------------------------------------------------

def _build_valve(
    estado: bool,
    grasa_24h: float,
    grasa_dispensada_desde_ultimo_relleno: float,
    grasa_ultimo_ciclo: float,
    longitud_pulsos_ultimo_ciclo: int,
    pulsos_ultimo_ciclo: list[int],
) -> dict[str, Any]:
    """Return a single valve dict."""
    return {
        "Estado": estado,
        "Grasa_24h": grasa_24h,
        "Grasa_Dispensada_Desde_Ultimo_Relleno": grasa_dispensada_desde_ultimo_relleno,
        "Grasa_Ultimo_Ciclo": grasa_ultimo_ciclo,
        "Longitud_Pulsos_Ultimo_Ciclo": longitud_pulsos_ultimo_ciclo,
        "Pulsos_Ultimo_Ciclo": pulsos_ultimo_ciclo,
    }


# ---------------------------------------------------------------------------
# Required top-level keys
# ---------------------------------------------------------------------------

_REQUIRED_KEYS = ("Alarma_Bajo_Nivel", "Bomba", "Estado_Equipo")

_REQUIRED_VALVE_FIELDS = (
    "Estado",
    "Grasa_24h",
    "Grasa_Dispensada_Desde_Ultimo_Relleno",
    "Grasa_Ultimo_Ciclo",
    "Longitud_Pulsos_Ultimo_Ciclo",
    "Pulsos_Ultimo_Ciclo",
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_payload(raw_data: dict[str, Any], n_valves: int) -> str:
    """
    Build a JSON string ready to be sent to Azure IoT Hub.

    Parameters
    ----------
    raw_data:
        Dictionary with the full device reading.  Must contain:
        - "Alarma_Bajo_Nivel"  → dict with "Estado" and
          "Grasa_Dispensada_Desde_Ultimo_Relleno"
        - "Bomba"              → dict with "Falla_Presion"
        - "Estado_Equipo"      → bool
        - "Valvula_V{i}"       → dict for each valve i in 1..n_valves

    n_valves:
        Number of valves this device has.  Valve keys "Valvula_V1" …
        "Valvula_V{n_valves}" are extracted from raw_data.

    Returns
    -------
    str
        JSON-encoded payload.

    Raises
    ------
    ValueError
        If any required key or valve field is missing from *raw_data*.
    """
    # Validate top-level keys
    missing = [k for k in _REQUIRED_KEYS if k not in raw_data]
    if missing:
        raise ValueError(
            f"raw_data is missing required top-level key(s): {', '.join(missing)}"
        )

    payload: dict[str, Any] = {
        "Alarma_Bajo_Nivel": raw_data["Alarma_Bajo_Nivel"],
        "Bomba": raw_data["Bomba"],
        "Estado_Equipo": raw_data["Estado_Equipo"],
    }

    for i in range(1, n_valves + 1):
        key = f"Valvula_V{i}"
        if key not in raw_data:
            raise ValueError(
                f"raw_data is missing valve key '{key}' (expected {n_valves} valves)"
            )
        valve_raw = raw_data[key]

        # Validate valve fields
        missing_fields = [f for f in _REQUIRED_VALVE_FIELDS if f not in valve_raw]
        if missing_fields:
            raise ValueError(
                f"Valve '{key}' is missing field(s): {', '.join(missing_fields)}"
            )

        payload[key] = _build_valve(
            estado=valve_raw["Estado"],
            grasa_24h=valve_raw["Grasa_24h"],
            grasa_dispensada_desde_ultimo_relleno=valve_raw[
                "Grasa_Dispensada_Desde_Ultimo_Relleno"
            ],
            grasa_ultimo_ciclo=valve_raw["Grasa_Ultimo_Ciclo"],
            longitud_pulsos_ultimo_ciclo=valve_raw["Longitud_Pulsos_Ultimo_Ciclo"],
            pulsos_ultimo_ciclo=valve_raw["Pulsos_Ultimo_Ciclo"],
        )

    return json.dumps(payload)
