from __future__ import annotations

import pytest

import x_atuo.automation.api as api_module
import x_atuo.automation.graph as graph_module
from x_atuo.automation.config import AutomationConfig as BaseAutomationConfig


@pytest.fixture(autouse=True)
def isolate_local_dotenv(monkeypatch):
    def build_config(*args, **kwargs):
        kwargs.setdefault("_env_file", None)
        return BaseAutomationConfig(*args, **kwargs)

    monkeypatch.setattr(api_module, "AutomationConfig", build_config)
    monkeypatch.setattr(graph_module, "AutomationConfig", build_config)
