from __future__ import annotations

from pathlib import Path
import ast


def test_runtime_modules_do_not_import_vendor_package() -> None:
    runtime_files = [
        Path("src/x_atuo/core/twitter_client.py"),
        Path("src/x_atuo/core/x_native_client.py"),
        Path("src/x_atuo/core/x_parser.py"),
    ]
    for path in runtime_files:
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                assert not node.module.startswith("x_atuo.vendor.")
            elif isinstance(node, ast.Import):
                for alias in node.names:
                    assert not alias.name.startswith("x_atuo.vendor.")


def test_vendored_twitter_cli_package_removed() -> None:
    assert not Path("src/x_atuo/vendor/twitter_cli").exists()
