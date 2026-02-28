from __future__ import annotations

import dataclasses
import os
from dataclasses import dataclass, field
from typing import get_args, get_origin, get_type_hints

from dotenv import load_dotenv

load_dotenv()


# ---------------------------------------------------------------------------
# Base class – auto-discovers env vars from class name + field names
# ---------------------------------------------------------------------------

class BaseSettings:
    """
    Auto-loads env vars for every field declared in a subclass.

    Naming convention
    -----------------
    Class name  : {PREFIX}Settings   (e.g. DeviceSettings  → prefix DEVICE)
    Field name  : {VARIABLE}         (e.g. CONNECTION_STRINGS, IDS)
    Env var read: {PREFIX}_{VARIABLE} (e.g. DEVICE_CONNECTION_STRINGS, DEVICE_IDS)

    Supported field types
    ---------------------
    str        – value read as-is (default: "")
    int        – value cast to int (default: 0)
    bool       – "1" / "true" / "yes" → True (default: False)
    list[str]  – comma-separated string split into a list (default: [])
    """

    def __post_init__(self) -> None:
        cls = type(self)
        prefix = cls.__name__.removesuffix("Settings").upper()
        hints = get_type_hints(cls)

        for f in dataclasses.fields(self):  # type: ignore[arg-type]
            env_key = f"{prefix}_{f.name}"
            raw = os.getenv(env_key)
            field_type = hints.get(f.name)

            if get_origin(field_type) is list:
                # list[str] or list[int] → split on comma, strip whitespace
                inner = get_args(field_type)[0] if get_args(field_type) else str
                if raw:
                    parts = [s.strip() for s in raw.split(",") if s.strip()]
                    value: object = [inner(p) for p in parts]
                else:
                    value = []
            elif field_type is int:
                value = int(raw) if raw is not None else getattr(self, f.name)
            elif field_type is bool:
                value = raw.lower() in ("1", "true", "yes") if raw is not None else getattr(self, f.name)
            else:  # str (and anything else)
                value = raw if raw is not None else getattr(self, f.name)

            setattr(self, f.name, value)


# ---------------------------------------------------------------------------
# Sub-settings classes  (add new fields freely – env vars are auto-detected)
# ---------------------------------------------------------------------------

@dataclass
class DeviceSettings(BaseSettings):
    """
    Reads DEVICE_* env vars automatically.

    DEVICE_N_VALVES: comma-separated int per device, e.g. "4,2"
    """

    CONNECTION_STRINGS: list[str] = field(default_factory=list)
    IDS: list[str] = field(default_factory=list)
    N_VALVES: list[int] = field(default_factory=list)
    RECEIVE_DATA: bool = True


@dataclass
class ChirpStackSettings(BaseSettings):
    """
    Reads CHIRPSTACK_* env vars automatically.
    """

    API_KEY: str = ""
    SERVER_URL: str = ""


@dataclass
class IoTHubSettings(BaseSettings):
    """
    Reads IOTHUB_* env vars automatically.
    """

    SERVICE_CONNECTION_STRING: str = ""


@dataclass
class InfraSettings(BaseSettings):
    """
    Reads INFRA_* env vars automatically.
    """

    DB_PATH: str = "data/database.db"


# ---------------------------------------------------------------------------
# Root Settings class
# ---------------------------------------------------------------------------

@dataclass
class Settings:
    """
    Central settings object. Import the singleton: ``from src.settings import settings``.
    """

    DEVICE: DeviceSettings = field(default_factory=DeviceSettings)
    CHIRPSTACK: ChirpStackSettings = field(default_factory=ChirpStackSettings)
    IOTHUB: IoTHubSettings = field(default_factory=IoTHubSettings)
    INFRA: InfraSettings = field(default_factory=InfraSettings)


# Singleton – import this everywhere.
settings = Settings()
