import asyncio

import pytest
from astrbot_plugin_github_observer.client import APIError, Client
from astrbot_plugin_github_observer.domain import Settings
from conftest import Clock, config


class Response:
    def __init__(self, value, status=200, headers=None):
        self.value, self.status, self.headers = value, status, headers or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def json(self, **kwargs):
        if isinstance(self.value, Exception):
            raise self.value
        return self.value


class Session:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.responses.pop(0)


@pytest.mark.asyncio
async def test_token_only_goes_to_github():
    session = Session(Response({}), Response({}))
    client = Client(session, Settings.parse(config(github_token="secret")))
    await client.get("/repos/a/b")
    await client.get("", market=True)
    assert session.calls[0][1]["headers"]["Authorization"] == "Bearer secret"
    assert "Authorization" not in session.calls[1][1]["headers"]
    assert session.calls[0][1]["allow_redirects"] is False


@pytest.mark.asyncio
async def test_pagination_and_page_cap_are_explicit():
    session = Session(
        Response([{"id": 1}], headers={"Link": '<ignored>; rel="next"'}), Response([{"id": 2}])
    )
    client = Client(session, Settings.parse(config()))
    data, _ = await client.listing("/test", {"state": "open"})
    assert [item["id"] for item in data] == [1, 2]
    assert session.calls[1][1]["params"] == {"state": "open", "per_page": 100, "page": 2}
    client = Client(
        Session(Response([], headers={"Link": '<ignored>; rel="next"'})),
        Settings.parse(config(max_pages=1)),
    )
    with pytest.raises(APIError, match="分页上限"):
        await client.listing("/test")


@pytest.mark.asyncio
async def test_rate_limit_stops_more_http_requests():
    clock = Clock()
    session = Session(Response({}, status=429, headers={"Retry-After": "120"}))
    client = Client(session, Settings.parse(config()), clock)
    with pytest.raises(APIError):
        await client.get("/test")
    with pytest.raises(APIError, match="预计恢复"):
        await client.get("/test")
    assert len(session.calls) == 1
    assert client.blocked_until == clock() + 120


@pytest.mark.asyncio
async def test_last_successful_rate_limited_request_is_preserved():
    clock = Clock()
    session = Session(
        Response(
            {"stars": 3},
            headers={"X-RateLimit-Remaining": "0", "X-RateLimit-Reset": str(int(clock() + 90))},
        )
    )
    client = Client(session, Settings.parse(config()), clock)
    result, _ = await client.get("/test")
    assert result == {"stars": 3}
    with pytest.raises(APIError):
        await client.get("/test")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response", [Response({}, 301), Response(ValueError()), Response({"bad": "shape"})]
)
async def test_redirect_invalid_json_and_invalid_list_are_errors(response):
    client = Client(Session(response), Settings.parse(config()))
    with pytest.raises(APIError):
        await client.listing("/test")


@pytest.mark.asyncio
async def test_timeout_is_safe_and_actionable():
    client = Client(
        Session(Response(asyncio.TimeoutError("private diagnostic"))), Settings.parse(config())
    )
    with pytest.raises(APIError, match="超时") as caught:
        await client.get("/test")
    assert "private diagnostic" not in str(caught.value)
