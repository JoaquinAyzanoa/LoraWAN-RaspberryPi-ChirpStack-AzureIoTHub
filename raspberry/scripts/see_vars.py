"""
Print every settings variable and its current value.

Run from the project root:
    uv --directory raspberry run python scripts/see_vars.py
"""

import dataclasses
import sys
from pathlib import Path

# Make sure 'src' is importable when the script is run directly.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.settings import settings  # noqa: E402


def _print_section(label: str, obj: object) -> None:
    print(f"\n[{label}]")
    for f in dataclasses.fields(obj):  # type: ignore[arg-type]
        value = getattr(obj, f.name)
        print(f"  {f.name} = {value!r}")


def main() -> None:
    print("=" * 52)
    print("  Current settings")
    print("=" * 52)

    for f in dataclasses.fields(settings):
        section = getattr(settings, f.name)
        if dataclasses.is_dataclass(section):
            _print_section(f.name, section)
        else:
            print(f"  {f.name} = {section!r}")

    print()


if __name__ == "__main__":
    main()
