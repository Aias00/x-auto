from __future__ import annotations

import ast
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


def test_twitter_client_keeps_single_write_method_definition() -> None:
    source = (REPO_ROOT / "src/x_atuo/core/twitter_client.py").read_text(encoding="utf-8")
    module = ast.parse(source)
    client_class = next(
        node for node in module.body if isinstance(node, ast.ClassDef) and node.name == "TwitterClient"
    )
    counts: dict[str, int] = {}
    for node in client_class.body:
        if isinstance(node, ast.FunctionDef):
            counts[node.name] = counts.get(node.name, 0) + 1

    assert counts["like"] == 1
    assert counts["unlike"] == 1
    assert counts["bookmark"] == 1
    assert counts["unbookmark"] == 1
    assert counts["delete_tweet"] == 1


def test_docs_match_current_runtime_state() -> None:
    readme = (REPO_ROOT / "README.md").read_text(encoding="utf-8")
    progress = (REPO_ROOT / "progress.md").read_text(encoding="utf-8")

    assert "Scheduler is the only execution entrypoint" not in readme
    assert "same target post may burst up to `3` direct replies in one run, with `3` second spacing" not in readme
    assert "POST /feed-engage/execute" in readme
    assert "POST /author-alpha/execute" in readme
    assert "at most `2` direct replies per target burst" in readme
    assert "`INTER_REPLY_DELAY_SECONDS` defaults to `5`" in readme
    assert "`TARGET_SWITCH_DELAY_SECONDS` defaults to `10`" in readme

    assert "vendored native client" not in progress
    assert "vendored parser’s core parsing logic" not in progress
    assert "vendored client’s core transport/mutation implementation" not in progress
