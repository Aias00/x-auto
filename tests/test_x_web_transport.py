from __future__ import annotations

from x_atuo.core.twitter_client import TwitterCredentials


def test_x_web_transport_uses_shared_credentials_and_proxy() -> None:
    from x_atuo.core.x_web_transport import XWebTransport

    transport = XWebTransport(
        credentials=TwitterCredentials(auth_token="token", ct0="csrf"),
        proxy="http://127.0.0.1:7890",
        timeout_seconds=30,
    )

    assert transport.credentials.auth_token == "token"
    assert transport.credentials.ct0 == "csrf"
    assert transport.proxy == "http://127.0.0.1:7890"
    assert transport.timeout_seconds == 30


def test_x_web_transport_get_json_uses_shared_http_stack(monkeypatch) -> None:
    from x_atuo.core.x_web_transport import XWebTransport

    captured: dict[str, object] = {}

    class FakeResponse:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def read(self):
            return b'{\"ok\":true}'

    class FakeOpener:
        def open(self, request, timeout):
            captured["full_url"] = request.full_url
            captured["headers"] = dict(request.header_items())
            captured["timeout"] = timeout
            return FakeResponse()

    monkeypatch.setattr("x_atuo.core.x_web_transport.build_opener", lambda handler: FakeOpener())

    transport = XWebTransport(
        credentials=TwitterCredentials(auth_token="token", ct0="csrf"),
        proxy="http://127.0.0.1:7890",
        timeout_seconds=30,
    )

    payload = transport.get_json(
        "https://x.com/i/api/graphql/demo/Op?variables=%7B%7D",
        referer="https://x.com/notifications",
    )

    assert payload == {"ok": True}
    assert captured["full_url"] == "https://x.com/i/api/graphql/demo/Op?variables=%7B%7D"
    assert captured["timeout"] == 30
    headers = {k.lower(): v for k, v in captured["headers"].items()}
    assert headers["authorization"].startswith("Bearer ")
    assert headers["referer"] == "https://x.com/notifications"
