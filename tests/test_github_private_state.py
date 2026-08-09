import json

import httpx
import pytest

from github_sync import GitHubSync


STATE_KEY = "correct horse battery staple 1234567890"


def _write_oauth_state(vault, *, client_id="client-a"):
    clients = {
        client_id: {
            "client_id": client_id,
            "redirect_uris": ["https://example.test/callback"],
            "expires": 4_102_444_800,
        }
    }
    grants = {
        "access_tokens": {},
        "refresh_tokens": {
            "refresh-a": {
                "expires": 4_102_444_800,
                "client_id": client_id,
            }
        },
    }
    (vault / ".oauth_clients.json").write_text(
        json.dumps(clients), encoding="utf-8"
    )
    (vault / ".dashboard_mcp_tokens.json").write_text(
        json.dumps(grants), encoding="utf-8"
    )
    return clients, grants


def test_private_state_round_trip_and_non_overwrite(tmp_path):
    source = tmp_path / "source"
    restored = tmp_path / "restored"
    source.mkdir()
    restored.mkdir()
    clients, grants = _write_oauth_state(source)
    sync = GitHubSync(
        token="token",
        repo="owner/private-repo",
        state_key=STATE_KEY,
    )

    blob = sync._collect_private_state(str(source))
    assert blob is not None
    assert b"client-a" not in blob
    assert b"refresh-a" not in blob

    restored_count, skipped = sync._install_private_state(
        str(restored), blob, overwrite=False
    )
    assert (restored_count, skipped) == (2, 0)
    assert json.loads((restored / ".oauth_clients.json").read_text()) == clients
    assert json.loads(
        (restored / ".dashboard_mcp_tokens.json").read_text()
    ) == grants

    (restored / ".oauth_clients.json").write_text(
        json.dumps({"newer": {}}), encoding="utf-8"
    )
    restored_count, skipped = sync._install_private_state(
        str(restored), blob, overwrite=False
    )
    assert (restored_count, skipped) == (0, 2)
    assert json.loads((restored / ".oauth_clients.json").read_text()) == {
        "newer": {}
    }


def test_private_state_wrong_key_is_rejected(tmp_path):
    source = tmp_path / "source"
    destination = tmp_path / "destination"
    source.mkdir()
    destination.mkdir()
    _write_oauth_state(source)
    first = GitHubSync(
        token="token", repo="owner/private-repo", state_key=STATE_KEY
    )
    second = GitHubSync(
        token="token",
        repo="owner/private-repo",
        state_key="different secret key with at least 32 bytes",
    )

    blob = first._collect_private_state(str(source))
    with pytest.raises(RuntimeError, match="cannot be decrypted"):
        second._install_private_state(str(destination), blob, overwrite=False)
    assert list(destination.iterdir()) == []


def test_private_state_requires_a_long_secret():
    with pytest.raises(ValueError, match="at least 32"):
        GitHubSync(
            token="token", repo="owner/private-repo", state_key="too-short"
        )


@pytest.mark.asyncio
async def test_state_only_sync_checks_branch_then_uploads_ciphertext(
    monkeypatch, tmp_path
):
    _write_oauth_state(tmp_path)
    sync = GitHubSync(
        token="token", repo="owner/private-repo", state_key=STATE_KEY
    )
    events = []

    async def fake_ensure_branch():
        events.append(("branch", None))

    async def fake_upload(blob):
        events.append(("private", blob))

    monkeypatch.setattr(sync, "_ensure_branch_for_private_state", fake_ensure_branch)
    monkeypatch.setattr(sync, "_upload_private_state", fake_upload)

    result = await sync.sync(str(tmp_path))

    assert result == {
        "ok": True,
        "uploaded": 0,
        "private_state_backed_up": True,
    }
    assert events[0] == ("branch", None)
    assert events[1][0] == "private"
    assert b"client-a" not in events[1][1]


@pytest.mark.asyncio
async def test_private_state_upload_refuses_public_repository(monkeypatch):
    sync = GitHubSync(
        token="token", repo="owner/public-repo", state_key=STATE_KEY
    )

    async def fake_request(_client, method, url, **_kwargs):
        assert method == "GET"
        assert url.endswith("/repos/owner/public-repo")
        return httpx.Response(
            200,
            json={"private": False},
            request=httpx.Request(method, url),
        )

    monkeypatch.setattr(sync, "_request", fake_request)
    with pytest.raises(RuntimeError, match="not private"):
        await sync._upload_private_state(b"ciphertext")


@pytest.mark.asyncio
async def test_existing_remote_branch_is_not_replaced_with_empty_manifest(monkeypatch):
    sync = GitHubSync(
        token="token", repo="owner/private-repo", state_key=STATE_KEY
    )
    batch_calls = []

    async def fake_request(_client, method, url, **_kwargs):
        assert method == "GET"
        assert url.endswith("/git/ref/heads/main")
        return httpx.Response(
            200,
            json={"object": {"sha": "existing-head"}},
            request=httpx.Request(method, url),
        )

    async def fake_batch(files):
        batch_calls.append(files)
        return 0

    monkeypatch.setattr(sync, "_request", fake_request)
    monkeypatch.setattr(sync, "_batch_commit", fake_batch)

    await sync._ensure_branch_for_private_state()

    assert batch_calls == []
