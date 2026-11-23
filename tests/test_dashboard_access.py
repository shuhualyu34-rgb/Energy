import base64
import json
import secrets
import unittest

from fastapi import Request

from cdp.dashboard_access import authenticate, load_users, protected_resource, token_verifier, visible_stations


class DashboardAccessTest(unittest.TestCase):
    def test_generated_token_authenticates_only_the_configured_viewer(self):
        token = secrets.token_urlsafe(32)
        users = load_users(json.dumps({"viewer": token_verifier(token)}))

        def basic(user, secret):
            return "Basic " + base64.b64encode(f"{user}:{secret}".encode()).decode()

        self.assertEqual(authenticate(basic("viewer", token), users), "viewer")
        self.assertIsNone(authenticate(basic("viewer", secrets.token_urlsafe(32)), users))
        self.assertIsNone(authenticate(basic("other", token), users))
        self.assertIsNone(authenticate("Basic not-base64!", users))
        self.assertIsNone(authenticate("Basic café", users))

    def test_configuration_is_open_only_when_variable_is_absent(self):
        self.assertIsNone(load_users(None))
        for invalid in ("", "{}", '[]', '{"viewer":"clear token"}'):
            with self.subTest(value=invalid), self.assertRaisesRegex(ValueError, "invalid dashboard verifier"):
                load_users(invalid)

    def test_station_scope_never_falls_back_to_unrestricted_access(self):
        digest = token_verifier(secrets.token_urlsafe(32))
        users = load_users(json.dumps({
            "all": digest,
            "scoped": {"verifier": digest, "stations": ["ST_SH_001"]},
            "empty": {"verifier": digest, "stations": []},
        }))
        rows = [{"station_id": "ST_SH_001"}, {"station_id": "ST_SH_002"}]
        request = Request({"type": "http"})
        for user, expected in [("all", rows), ("scoped", rows[:1]), ("empty", [])]:
            request.state.dashboard_account = users[user]
            self.assertEqual(visible_stations(request, rows), expected)
        for stations in (None, "ST_SH_001", ["*"]):
            with self.subTest(stations=stations), self.assertRaises(ValueError):
                load_users(json.dumps({"scoped": {"verifier": digest, "stations": stations}}))

    def test_dashboard_and_data_routes_are_gated_but_metrics_remain_internal(self):
        self.assertEqual(protected_resource("/dashboard"), "dashboard")
        self.assertEqual(protected_resource("/dashboard/assets/dashboard.js"), "dashboard")
        self.assertEqual(protected_resource("/v1/demo/station-daily"), "data_api")
        self.assertIsNone(protected_resource("/metrics"))
        self.assertIsNone(protected_resource("/health/ready"))
        self.assertIsNone(protected_resource("/dashboard-helper"))


if __name__ == "__main__":
    unittest.main()
