"""
Tests for src/settings.py
Run from raspberry/ with: pytest tests/settingstest.py -v
"""

import pytest
from src.settings import settings


class TestDeviceSettings:
    def test_connection_strings_is_list(self):
        assert isinstance(settings.DEVICE.CONNECTION_STRINGS, list)

    def test_connection_strings_not_empty(self):
        assert len(settings.DEVICE.CONNECTION_STRINGS) > 0, (
            "DEVICE_CONNECTION_STRINGS must have at least one entry"
        )

    def test_connection_strings_are_strings(self):
        for cs in settings.DEVICE.CONNECTION_STRINGS:
            assert isinstance(cs, str) and cs, "Each connection string must be a non-empty str"

    def test_ids_is_list(self):
        assert isinstance(settings.DEVICE.IDS, list)

    def test_ids_not_empty(self):
        assert len(settings.DEVICE.IDS) > 0, "DEVICE_IDS must have at least one entry"

    def test_ids_are_strings(self):
        for device_id in settings.DEVICE.IDS:
            assert isinstance(device_id, str) and device_id

    def test_receive_data_is_bool(self):
        assert isinstance(settings.DEVICE.RECEIVE_DATA, bool)


class TestChirpStackSettings:
    def test_api_key_is_str(self):
        assert isinstance(settings.CHIRPSTACK.API_KEY, str)

    def test_server_url_is_str(self):
        assert isinstance(settings.CHIRPSTACK.SERVER_URL, str)
