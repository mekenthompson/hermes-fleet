"""Black-box tests for the Connect-backed Linear OAuth provider."""
from __future__ import annotations

import json
import os
import socket
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "plugins" / "linear-agent"))

import linear_oauth  # noqa: E402
from linear_oauth import ConnectItem, LinearOAuth, ReauthorizationRequired, load_connect_env  # noqa: E402
from tests.linear_fake_connect import FakeConnect, canonical_item, login_item  # noqa: E402

VAULT, ITEM = "vault-aaaaaaaaaaaaaaaaaaaaaa", "item-bbbbbbbbbbbbbbbbbbbbbbbb"


def _closed_port_url() -> str:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    return f"http://127.0.0.1:{port}/oauth/token"


def _write_private(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    path.chmod(0o600)
    return path


class _Case(unittest.TestCase):
    def setUp(self) -> None:
        self.fake = FakeConnect().__enter__()
        self.addCleanup(self.fake.__exit__, None, None, None)
        self.fake.items[(VAULT, ITEM)] = login_item(VAULT, ITEM)
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.home = Path(self.temp.name)
        self.cache = self.home / "secrets" / "linear-oauth.json"
        endpoint = mock.patch.object(linear_oauth, "_TOKEN_ENDPOINT", self.fake.token_endpoint)
        endpoint.start()
        self.addCleanup(endpoint.stop)
        self.clock = [1000.0]

    def item(self, vault: str = VAULT, item: str = ITEM) -> ConnectItem:
        return ConnectItem(self.fake.host, self.fake.token, vault, item)

    def oauth(self, **kwargs) -> LinearOAuth:
        return LinearOAuth(self.item(), self.cache, profile="alpha", now=lambda: self.clock[0], **kwargs)

    def _patch_count(self) -> int:
        return sum(1 for method, _ in self.fake.requests if method == "PATCH")

    def write_cache(self, value: dict) -> None:
        _write_private(self.cache, json.dumps(value))

    def read_cache(self) -> dict:
        return json.loads(self.cache.read_text())

    def stored_refresh(self) -> str:
        return self.fake.field_value(VAULT, ITEM, "refresh_token")


class LinearOAuthTokenTests(_Case):
    def test_cached_valid_token_returns_without_network(self) -> None:
        self.write_cache({"access_token": "current", "expires_at": 2000})
        self.assertEqual(self.oauth().token(), "current")
        self.assertEqual(self.fake.requests, [])

    def test_token_within_sixty_seconds_of_expiry_is_refreshed(self) -> None:
        self.write_cache({"access_token": "current", "expires_at": 1050})
        self.assertEqual(self.oauth().token(), "access-refresh-0")

    def test_expired_token_reads_connect_posts_patches_and_caches(self) -> None:
        self.write_cache({"access_token": "expired", "expires_at": 1})
        self.assertEqual(self.oauth().token(), "access-refresh-0")
        self.assertEqual(self.fake.token_requests[0]["refresh_token"], "refresh-0")
        self.assertEqual(self.fake.token_requests[0]["client_id"], "client")
        self.assertEqual(self.fake.token_requests[0]["client_secret"], "secret")
        self.assertEqual(self.stored_refresh(), "rotated-refresh-0")
        cache = self.read_cache()
        self.assertEqual(cache, {"access_token": "access-refresh-0", "expires_at": 4600})
        self.assertEqual(self.cache.stat().st_mode & 0o777, 0o600)
        methods = [method for method, _ in self.fake.requests]
        self.assertEqual(methods, ["GET", "POST", "GET", "PATCH", "GET"])

    def test_missing_cache_file_is_treated_as_expired(self) -> None:
        self.assertEqual(self.oauth().token(), "access-refresh-0")
        self.assertTrue(self.cache.exists())

    def test_patch_failure_keeps_rotated_token_and_still_returns_token(self) -> None:
        self.fake.fail_patch = True
        provider = self.oauth()
        self.assertEqual(provider.token(), "access-refresh-0")
        self.assertEqual(self.read_cache()["rotated_refresh_token"], "rotated-refresh-0")
        self.assertEqual(self.stored_refresh(), "refresh-0")
        # Retries are rate limited, so an immediate call must not touch Connect again.
        self.fake.fail_patch = False
        patches_before = self._patch_count()
        self.assertEqual(provider.token(), "access-refresh-0")
        self.assertEqual(self._patch_count(), patches_before)
        self.assertEqual(self.stored_refresh(), "refresh-0")
        # Once the retry floor passes, the rotation lands and the marker clears.
        self.clock[0] += linear_oauth._PENDING_RETRY_MIN_SECONDS + 1
        self.assertEqual(provider.token(), "access-refresh-0")
        self.assertEqual(self.stored_refresh(), "rotated-refresh-0")
        self.assertNotIn("rotated_refresh_token", self.read_cache())
        self.assertEqual(len(self.fake.token_requests), 1)

    def test_stuck_rotation_does_not_hit_connect_on_every_call(self) -> None:
        """A Connect outage must not put a PATCH in front of every Linear API call."""
        self.fake.fail_patch = True
        provider = self.oauth()
        provider.token()
        patches_after_refresh = self._patch_count()
        for _ in range(25):
            provider.token()
        self.assertEqual(self._patch_count(), patches_after_refresh)

    def test_legacy_pending_refresh_token_is_never_written_to_connect(self) -> None:
        """The old format staged pending_refresh_token BEFORE calling Linear, so a stale
        one may hold a token Linear never issued. Writing it would clobber a good
        credential in Connect. The new key name must make that impossible."""
        self.write_cache({"access_token": "current", "expires_at": 2000,
                          "pending_refresh_token": "never-issued-by-linear",
                          "pending_access_token": "stale", "pending_expires_at": 5})
        self.assertEqual(self.oauth().token(), "current")
        self.assertEqual(self.stored_refresh(), "refresh-0")
        self.assertEqual(self._patch_count(), 0)

    def test_pending_token_is_used_for_next_refresh_instead_of_connect_value(self) -> None:
        self.fake.fail_patch = True
        self.write_cache({"access_token": "expired", "expires_at": 1, "rotated_refresh_token": "newest"})
        self.assertEqual(self.oauth().token(), "access-newest")
        self.assertEqual(self.fake.token_requests[0]["refresh_token"], "newest")
        self.assertEqual(self.read_cache()["rotated_refresh_token"], "rotated-newest")

    def test_transport_error_raises_and_leaves_cache_untouched(self) -> None:
        original = {"access_token": "expired", "expires_at": 1}
        self.write_cache(original)
        with mock.patch.object(linear_oauth, "_TOKEN_ENDPOINT", _closed_port_url()):
            with self.assertRaisesRegex(RuntimeError, "unreachable"):
                self.oauth().token()
        self.assertEqual(self.read_cache(), original)
        self.assertEqual(self.stored_refresh(), "refresh-0")

    def test_http_400_and_401_raise_reauthorization_required(self) -> None:
        original = {"access_token": "expired", "expires_at": 1}
        for status in (400, 401):
            with self.subTest(status=status):
                self.write_cache(original)
                self.fake.token_responses = [(status, {"error": "invalid_grant"})]
                with self.assertRaisesRegex(ReauthorizationRequired, "alpha.*linear-reauth"):
                    self.oauth().token()
                self.assertEqual(self.read_cache(), original)

    def test_http_5xx_is_a_retryable_error_not_reauthorization(self) -> None:
        self.write_cache({"access_token": "expired", "expires_at": 1})
        self.fake.token_responses = [(503, {"error": "busy"})]
        with self.assertRaises(RuntimeError) as caught:
            self.oauth().token()
        self.assertNotIsInstance(caught.exception, ReauthorizationRequired)

    def test_invalid_expires_in_or_missing_fields_raise_without_cache_write(self) -> None:
        original = {"access_token": "expired", "expires_at": 1}
        bad = (
            {"access_token": "a", "refresh_token": "r", "expires_in": 60},
            {"access_token": "a", "refresh_token": "r", "expires_in": "3600"},
            {"access_token": "a", "refresh_token": "r", "expires_in": True},
            {"access_token": "a", "refresh_token": "r"},
            {"access_token": "a", "expires_in": 3600},
            {"refresh_token": "r", "expires_in": 3600},
        )
        for payload in bad:
            with self.subTest(payload=payload):
                self.write_cache(original)
                self.fake.token_responses = [(200, payload)]
                with self.assertRaises(RuntimeError):
                    self.oauth().token()
                self.assertEqual(self.read_cache(), original)
                self.assertEqual(self.stored_refresh(), "refresh-0")

    def test_forty_minutes_of_transport_failures_then_success_has_no_lockout(self) -> None:
        self.write_cache({"access_token": "expired", "expires_at": 1})
        provider = self.oauth()
        with mock.patch.object(linear_oauth, "_TOKEN_ENDPOINT", _closed_port_url()):
            for _ in range(5):
                with self.assertRaises(RuntimeError):
                    provider.token()
                self.clock[0] += 8 * 60
        self.assertGreater(self.clock[0] - 1000.0, 35 * 60)
        self.assertEqual(provider.token(), "access-refresh-0")
        self.assertEqual(self.stored_refresh(), "rotated-refresh-0")

    def test_invalidate_forces_refresh_on_next_call(self) -> None:
        self.write_cache({"access_token": "current", "expires_at": 2000})
        provider = self.oauth()
        provider.invalidate()
        self.assertEqual(self.read_cache()["expires_at"], 0)
        self.assertEqual(provider.token(), "access-refresh-0")

    def test_provider_is_callable_for_activity_client(self) -> None:
        self.write_cache({"access_token": "current", "expires_at": 2000})
        self.assertEqual(self.oauth()(), "current")

    def test_concurrent_threads_refresh_once(self) -> None:
        self.write_cache({"access_token": "expired", "expires_at": 1})
        provider = self.oauth()
        results: list[str] = []
        threads = [threading.Thread(target=lambda: results.append(provider.token())) for _ in range(5)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(results, ["access-refresh-0"] * 5)
        self.assertEqual(len(self.fake.token_requests), 1)

    def test_two_providers_on_same_cache_refresh_once_via_flock(self) -> None:
        self.write_cache({"access_token": "expired", "expires_at": 1})
        providers = [self.oauth() for _ in range(2)]
        results: list[str] = []
        threads = [threading.Thread(target=lambda p=p: results.append(p.token())) for p in providers]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        self.assertEqual(results, ["access-refresh-0"] * 2)
        self.assertEqual(len(self.fake.token_requests), 1)
        self.assertEqual((self.cache.with_suffix(".lock")).stat().st_mode & 0o777, 0o600)

    def test_group_readable_cache_is_refused(self) -> None:
        self.write_cache({"access_token": "current", "expires_at": 2000})
        self.cache.chmod(0o640)
        with self.assertRaisesRegex(RuntimeError, "chmod 600"):
            self.oauth().token()

    def test_symlinked_cache_is_refused(self) -> None:
        real = _write_private(self.home / "elsewhere.json", json.dumps({"access_token": "current", "expires_at": 2000}))
        self.cache.parent.mkdir(mode=0o700)
        self.cache.symlink_to(real)
        with self.assertRaisesRegex(RuntimeError, "unavailable"):
            self.oauth().token()

    def test_old_cache_layout_is_ignored_and_rewritten(self) -> None:
        self.write_cache({"access_token": "expired", "expires_at": 1, "client_id": "stale", "client_secret": "stale",
                          "refresh_token": "stale", "refresh_intent_at": 5, "refresh_intent_digest": "x"})
        self.assertEqual(self.oauth().token(), "access-refresh-0")
        self.assertEqual(set(self.read_cache()), {"access_token", "expires_at"})

    def test_install_refresh_token_stores_in_connect_and_drops_cache(self) -> None:
        self.write_cache({"access_token": "current", "expires_at": 2000})
        self.oauth().install_refresh_token("brand-new")
        self.assertEqual(self.stored_refresh(), "brand-new")
        self.assertFalse(self.cache.exists())
        self.assertEqual(self.oauth().token(), "access-brand-new")


class ConnectItemTests(_Case):
    def test_extra_vaults_and_any_title_are_accepted(self) -> None:
        self.fake.items[("other-vault", "other-item")] = login_item("other-vault", "other-item")
        self.fake.items[(VAULT, ITEM)]["title"] = "Whatever Ken renamed it to"
        self.assertEqual(self.item().credentials()["client_id"], "client")

    def test_login_and_canonical_layouts_resolve_case_insensitively(self) -> None:
        self.fake.items[(VAULT, ITEM)] = canonical_item(VAULT, ITEM, client_id="k-client", client_secret="k-secret", refresh_token="k-refresh")
        self.assertEqual(self.item().credentials(), {"client_id": "k-client", "client_secret": "k-secret", "refresh_token": "k-refresh"})
        self.fake.items[(VAULT, ITEM)] = login_item(VAULT, ITEM, client_id="l-client", client_secret="l-secret")
        self.assertEqual(self.item().credentials()["client_secret"], "l-secret")

    def test_password_alias_is_accepted_for_client_secret(self) -> None:
        item = self.fake.items[(VAULT, ITEM)]
        item["fields"][1] = {"id": "password", "label": "password", "value": "pw-secret"}
        self.assertEqual(self.item().credentials()["client_secret"], "pw-secret")

    def test_wrong_vault_or_item_id_is_refused(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "failed"):
            self.item(vault="vault-zzzzzzzzzzzzzzzzzzzzzz").credentials()
        with self.assertRaisesRegex(RuntimeError, "failed"):
            self.item(item="item-zzzzzzzzzzzzzzzzzzzzzzzz").credentials()
        # Connect answering with a different item than requested is refused too.
        self.fake.items[(VAULT, ITEM)]["vault"] = {"id": "somewhere-else"}
        with self.assertRaisesRegex(RuntimeError, "does not match"):
            self.item().credentials()

    def test_missing_or_empty_field_error_names_the_field(self) -> None:
        item = self.fake.items[(VAULT, ITEM)]
        item["fields"] = [field for field in item["fields"] if field["label"] != "refresh_token"]
        with self.assertRaisesRegex(RuntimeError, "no refresh_token field"):
            self.item().credentials()
        self.fake.items[(VAULT, ITEM)] = login_item(VAULT, ITEM, client_secret="")
        with self.assertRaisesRegex(RuntimeError, "client_secret is empty"):
            self.item().credentials()

    def test_duplicate_fields_for_one_name_are_ambiguous(self) -> None:
        self.fake.items[(VAULT, ITEM)]["fields"].append({"id": "dup", "label": "refresh_token", "value": "other"})
        with self.assertRaisesRegex(RuntimeError, "2 fields named 'refresh_token'"):
            self.item().credentials()

    def test_write_refresh_token_patches_and_reads_back_once(self) -> None:
        self.item().write_refresh_token("next")
        self.assertEqual(self.stored_refresh(), "next")
        self.assertEqual([m for m, _ in self.fake.requests], ["GET", "PATCH", "GET"])

    def test_bad_connect_token_is_a_failed_request(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "GET .* failed"):
            ConnectItem(self.fake.host, "wrong", VAULT, ITEM).credentials()


class LoadConnectEnvTests(unittest.TestCase):
    def test_crlf_quotes_and_whitespace_are_stripped(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = _write_private(Path(temp) / ".op.env", '# comment\r\nOP_CONNECT_HOST = "http://onepassword-connect:8080/" \r\nexport OP_CONNECT_TOKEN=\'tok en\'\r\n\r\n')
            self.assertEqual(load_connect_env(path), ("http://onepassword-connect:8080", "tok en"))

    def test_any_http_or_https_host_is_accepted_but_other_schemes_are_not(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = _write_private(Path(temp) / ".op.env", "OP_CONNECT_HOST=https://connect.example:8443\nOP_CONNECT_TOKEN=t\n")
            self.assertEqual(load_connect_env(path)[0], "https://connect.example:8443")
            _write_private(path, "OP_CONNECT_HOST=ftp://x\nOP_CONNECT_TOKEN=t\n")
            with self.assertRaisesRegex(RuntimeError, "http"):
                load_connect_env(path)

    def test_missing_values_are_refused(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = _write_private(Path(temp) / ".op.env", "OP_CONNECT_HOST=http://x\n")
            with self.assertRaisesRegex(RuntimeError, "OP_CONNECT_TOKEN"):
                load_connect_env(path)

    def test_group_readable_or_symlinked_env_is_refused(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            path = _write_private(Path(temp) / ".op.env", "OP_CONNECT_HOST=http://x\nOP_CONNECT_TOKEN=t\n")
            path.chmod(0o640)
            with self.assertRaisesRegex(RuntimeError, "chmod 600"):
                load_connect_env(path)
            path.chmod(0o600)
            link = Path(temp) / "link.env"
            link.symlink_to(path)
            with self.assertRaisesRegex(RuntimeError, "unavailable"):
                load_connect_env(link)
            with self.assertRaisesRegex(RuntimeError, "unavailable"):
                load_connect_env(Path(temp) / "missing.env")


class PrivateDirectoryTests(unittest.TestCase):
    def test_validate_private_directory_only_warns(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            open_dir = Path(temp) / "open"
            open_dir.mkdir()
            open_dir.chmod(0o755)
            with self.assertLogs("linear-agent.oauth", level="WARNING") as logs:
                linear_oauth.validate_private_directory(open_dir)
                linear_oauth.validate_private_directory(Path(temp) / "missing")
            self.assertEqual(len(logs.output), 2)

    def test_make_oauth_wires_env_cache_and_binding(self) -> None:
        with tempfile.TemporaryDirectory() as temp:
            home = Path(temp)
            _write_private(home / ".op.env", "OP_CONNECT_HOST=http://127.0.0.1:1\nOP_CONNECT_TOKEN=t\n")
            provider = linear_oauth.make_oauth("alpha", home, VAULT, ITEM)
            self.assertEqual(provider.cache_path, home / "secrets" / "linear-oauth.json")
            self.assertEqual((provider.connect_item.vault_id, provider.connect_item.item_id), (VAULT, ITEM))
            self.assertEqual(provider.profile, "alpha")


if __name__ == "__main__":
    unittest.main()
