import json
import platform
import sys
from pathlib import Path

import pytest

from mq_bridge import plugin_library_path


def _tag() -> str:
    machine = platform.machine().lower()
    arch = {"aarch64": "arm64", "amd64": "x64", "x86_64": "x64"}.get(machine, machine)
    suffix = "-gnu" if sys.platform == "linux" else "-msvc" if sys.platform == "win32" else ""
    return f"{sys.platform}-{arch}{suffix}"


def _library_name() -> str:
    if sys.platform == "win32":
        return "reference.dll"
    if sys.platform == "darwin":
        return "libreference.dylib"
    return "libreference.so"


def test_plugin_library_path_selects_current_platform_prebuild(tmp_path: Path) -> None:
    (tmp_path / "mq-bridge-plugin.json").write_text(
        json.dumps({"name": "reference", "library": "reference"}), encoding="utf-8"
    )
    library = tmp_path / "prebuilds" / _tag() / _library_name()
    library.parent.mkdir(parents=True)
    library.write_bytes(b"fixture")

    assert plugin_library_path(tmp_path) == str(library)


def test_plugin_library_path_reports_missing_manifest(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="plugin manifest not found"):
        plugin_library_path(tmp_path)
