"""Optional local dashboard access using generated high-entropy tokens."""

import argparse
import base64
import hashlib
import hmac
import json
import logging
import re
import secrets
from dataclasses import dataclass

from fastapi import Request, Response
from fastapi.responses import PlainTextResponse

USER = re.compile(r"[A-Za-z][A-Za-z0-9_-]{0,63}")
TOKEN = re.compile(r"[A-Za-z0-9_-]{43}")
STATION = re.compile(r"[A-Za-z0-9_-]{1,64}")
DIGEST = re.compile(r"[0-9a-f]{64}")
AUTH_HEADER = 'Basic realm="CDP dashboard", charset="UTF-8"'
AUDIT = logging.getLogger("uvicorn.error")


@dataclass(frozen=True)
class DashboardAccount:
    verifier: str
    stations: frozenset[str] | None = None


def load_users(value: str | None) -> dict[str, DashboardAccount] | None:
    if value is None:
        return None
    try:
        users = json.loads(value)
    except ValueError:
        raise ValueError("invalid dashboard verifier configuration") from None
    if not isinstance(users, dict) or not users:
        raise ValueError("invalid dashboard verifier configuration")
    accounts = {}
    for user, entry in users.items():
        if not USER.fullmatch(user):
            raise ValueError("invalid dashboard verifier configuration")
        if isinstance(entry, str) and DIGEST.fullmatch(entry):
            accounts[user] = DashboardAccount(entry)
            continue
        if not isinstance(entry, dict) or entry.keys() != {"verifier", "stations"}:
            raise ValueError("invalid dashboard verifier configuration")
        verifier, stations = entry["verifier"], entry["stations"]
        if not isinstance(verifier, str) or not DIGEST.fullmatch(verifier) or not isinstance(stations, list):
            raise ValueError("invalid dashboard verifier configuration")
        if any(not isinstance(station, str) or not STATION.fullmatch(station) for station in stations):
            raise ValueError("invalid dashboard station scope")
        accounts[user] = DashboardAccount(verifier, frozenset(stations))
    return accounts


def station_scope(request: Request) -> frozenset[str] | None:
    account = getattr(request.state, "dashboard_account", None)
    return account.stations if account else None


def visible_stations(request: Request, rows: list[dict]) -> list[dict]:
    stations = station_scope(request)
    return rows if stations is None else [row for row in rows if row["station_id"] in stations]



def token_verifier(token: str) -> str:
    return hashlib.sha256(token.encode("ascii")).hexdigest()


def authenticate(header: str, users: dict[str, DashboardAccount]) -> str | None:
    if len(header) > 256:
        return None
    scheme, _, credential = header.partition(" ")
    if scheme.lower() != "basic":
        return None
    try:
        principal, _, token = base64.b64decode(credential, validate=True).decode("ascii").partition(":")
    except ValueError:
        return None
    if not USER.fullmatch(principal) or not TOKEN.fullmatch(token):
        return None
    account = users.get(principal)
    valid = hmac.compare_digest(token_verifier(token), account.verifier if account else "0" * 64)
    return principal if account and valid else None


def protected_resource(path: str) -> str | None:
    if path == "/dashboard" or path.startswith("/dashboard/"):
        return "dashboard"
    if path == "/v1/demo" or path.startswith("/v1/demo/"):
        return "data_api"
    return None


async def enforce_dashboard_access(request: Request, call_next) -> Response:
    users = request.app.state.dashboard_users
    resource = protected_resource(request.url.path)
    if users is None or resource is None:
        return await call_next(request)

    principal = authenticate(request.headers.get("Authorization", ""), users)
    if principal is None:
        response = PlainTextResponse("Dashboard credentials required.", status_code=401,
                                     headers={"WWW-Authenticate": AUTH_HEADER})
    else:
        request.state.dashboard_account = users[principal]
        response = await call_next(request)
    response.headers["Cache-Control"] = "private, no-store"
    AUDIT.info("%s", json.dumps({
        "event": "dashboard_access", "principal": principal, "resource": resource,
        "method": request.method, "status": response.status_code,
    }, separators=(",", ":")))
    return response


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate a dashboard login token and its verifier.")
    parser.add_argument("username")
    args = parser.parse_args()
    if not USER.fullmatch(args.username):
        parser.error("invalid dashboard username")
    token = secrets.token_urlsafe(32)
    print(json.dumps({"username": args.username, "token": token,
                      "verifiers": {args.username: token_verifier(token)}}, indent=2))


if __name__ == "__main__":
    main()
