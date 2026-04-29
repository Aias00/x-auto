from __future__ import annotations

from pathlib import Path

import tomllib


def test_vendored_twitter_cli_surface_is_removed() -> None:
    assert not Path("src/x_atuo/vendor/twitter_cli").exists()


def test_pyproject_drops_cli_only_twitter_cli_dependencies() -> None:
    project = tomllib.loads(Path("pyproject.toml").read_text(encoding="utf-8"))
    deps = set(project["project"]["dependencies"])

    assert "curl_cffi>=0.7" in deps
    assert "xclienttransaction>=1.0.1" in deps
    assert "beautifulsoup4>=4.12" in deps
    assert "click>=8.0" not in deps
    assert "rich>=13.0" not in deps
    assert "browser-cookie3>=0.19" not in deps
