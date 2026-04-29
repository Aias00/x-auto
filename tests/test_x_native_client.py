from __future__ import annotations


def test_core_native_client_is_importable() -> None:
    from x_atuo.core.x_native_client import TwitterClient as CoreNativeTwitterClient

    assert CoreNativeTwitterClient is not None
