"""Thin local Nest protocol adapter. No dashboard, MQTT writer or policy engine."""

import argparse
import asyncio
import hmac
import ipaddress
import json
import logging
import math
import os
import signal
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from aiohttp import web
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from nolongerevil.config import settings
from nolongerevil.lib.serial_parser import (
    extract_basic_auth_password,
    extract_serial_from_request,
)
from nolongerevil.lib.types import DeviceObject, DeviceOwner, UserInfo
from nolongerevil.main import create_proxy_app
from nolongerevil.middleware.url_normalizer import normalize_url
from nolongerevil.routes.control.command import execute_command
from nolongerevil.routes.nest.transport import parse_put_body, parse_subscribe_body
from nolongerevil.services.device_availability import DeviceAvailability
from nolongerevil.services.device_state_service import DeviceStateService
from nolongerevil.services.sqlmodel_service import SQLModelService
from nolongerevil.services.subscription_manager import SubscriptionManager
from nolongerevil.services.weather_service import WeatherService
from nolongerevil.utils.temperature_safety import get_safety_bounds

# Deliberately exclude credentials, location, occupancy and complete device buckets.
FIELDS = {
    "device": {"current_temperature", "current_humidity", "battery_level", "can_heat", "can_cool"},
    "shared": {
        "current_temperature",
        "target_temperature",
        "target_temperature_low",
        "target_temperature_high",
        "target_temperature_type",
        "hvac_heater_state",
        "hvac_ac_state",
        "hvac_fan_state",
    },
}


class DeviceConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    serial: str = Field(pattern=r"^[A-Z0-9]{10,32}$")
    api_key_env: str = Field(pattern=r"^[A-Z][A-Z0-9_]+$")
    control_enabled: bool = False


class ProxyConfig(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    host: str
    port: int = Field(ge=1024, le=65535)
    api_origin: str
    devices: list[DeviceConfig] = Field(default_factory=list)
    controller_token_env: str = "NEST_PROXY_CONTROLLER_TOKEN"
    observation_ttl_seconds: int = Field(default=90, ge=15, le=300)

    @model_validator(mode="after")
    def private_origin(self) -> "ProxyConfig":
        address = ipaddress.IPv4Address(self.host)
        private = address.is_loopback or any(
            address in ipaddress.ip_network(net)
            for net in ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16")
        )
        origin = urlsplit(self.api_origin)
        if (
            not private
            or origin.scheme != "http"
            or origin.hostname != self.host
            or origin.port != self.port
            or origin.path not in ("", "/")
            or origin.username
            or origin.password
            or origin.query
            or origin.fragment
        ):
            raise ValueError("Origin must match an explicit private IPv4 HTTP bind address/port")
        return self

    @field_validator("devices")
    @classmethod
    def unique_serials(cls, value: list[DeviceConfig]) -> list[DeviceConfig]:
        if len({device.serial for device in value}) != len(value):
            raise ValueError("Duplicate device serial")
        return value


class Command(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    id: str = Field(pattern=r"^[a-zA-Z0-9:._-]{1,120}$")
    serial: str = Field(pattern=r"^[A-Z0-9]{10,32}$")
    command: str
    value: float | str
    expires_at: float


class NoCloudWeather(WeatherService):
    """Never instantiate upstream's external weather HTTP client."""

    async def get_weather(
        self,
        postal_code: str | None = None,
        country: str | None = None,
        query_string: str | None = None,
    ) -> None:
        del postal_code, country, query_string
        return None


class NoReplaySubscriptions(SubscriptionManager):
    async def store_pending_push(self, serial: str, objects: list[dict[str, Any]]) -> None:
        # A broken connection must not enqueue actuator state for a future
        # connection. The controller can issue a new, fresh request instead.
        del serial, objects


class Observations:
    """Separate actual device reports from upstream's merged desired-state cache."""

    def __init__(self, path: Path, ttl: int) -> None:
        self.db = sqlite3.connect(path)
        self.db.execute("PRAGMA journal_mode=WAL")
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS observations(serial TEXT, field TEXT, value TEXT, at REAL, PRIMARY KEY(serial,field))"
        )
        self.db.execute(
            "CREATE TABLE IF NOT EXISTS commands(id TEXT PRIMARY KEY, payload TEXT, status TEXT, result TEXT, sent_at REAL)"
        )
        self.db.commit()
        self.ttl = ttl

    def record(self, serial: str, objects: list[dict[str, Any]], at: float) -> None:
        with self.db:
            for obj in objects:
                key = obj.get("object_key", "")
                kind, _, identity = key.partition(".") if isinstance(key, str) else ("", "", "")
                value = obj.get("value")
                if identity != serial or kind not in FIELDS or not isinstance(value, dict):
                    continue
                timestamp = obj.get("object_timestamp")
                reported_at = (
                    min(at, timestamp / 1000)
                    if isinstance(timestamp, (int, float)) and timestamp > 0
                    else at
                )
                if not math.isfinite(reported_at):
                    continue
                for field, item in value.items():
                    if field not in FIELDS[kind] or not isinstance(item, (float, int, str, bool)):
                        continue
                    if isinstance(item, float) and not math.isfinite(item):
                        continue
                    if isinstance(item, str) and len(item) > 80:
                        continue
                    name = f"{kind}_{field}"
                    self.db.execute(
                        "INSERT INTO observations VALUES(?,?,?,?) ON CONFLICT(serial,field) DO UPDATE SET value=excluded.value,at=excluded.at",
                        (serial, name, json.dumps(item), reported_at),
                    )
                # Confirmation requires the field in a subsequent DEVICE report,
                # never a value written into the server cache by execute_command.
                for command_id, payload, sent_at in self.db.execute(
                    "SELECT id,payload,sent_at FROM commands WHERE status='dispatched'"
                ).fetchall():
                    command = json.loads(payload)
                    field = (
                        "target_temperature"
                        if command["command"] == "set_temperature"
                        else "target_temperature_type"
                    )
                    if (
                        command["serial"] == serial
                        and kind == "shared"
                        and reported_at > sent_at
                        and field in value
                        and value[field] == command["value"]
                    ):
                        self.db.execute(
                            "UPDATE commands SET status='confirmed',result=? WHERE id=?",
                            (
                                json.dumps(
                                    {"observed_at": at, "field": field, "value": value[field]}
                                ),
                                command_id,
                            ),
                        )

    def snapshot(self, serial: str) -> dict[str, Any]:
        rows = self.db.execute(
            "SELECT field,value,at FROM observations WHERE serial=?", (serial,)
        ).fetchall()
        return {
            "serial": serial,
            "fields": {
                field: {"value": json.loads(value), "observed_at": at, "valid_until": at + self.ttl}
                for field, value, at in rows
            },
        }

    def receipt(self, command_id: str) -> dict[str, Any] | None:
        row = self.db.execute(
            "SELECT status,result FROM commands WHERE id=?", (command_id,)
        ).fetchone()
        return {"id": command_id, "status": row[0], "result": json.loads(row[1])} if row else None


class ThinProxy:
    def __init__(
        self,
        config: ProxyConfig,
        state: DeviceStateService,
        subscriptions: SubscriptionManager,
        observations: Observations,
    ) -> None:
        self.config, self.state, self.subscriptions, self.observations = (
            config,
            state,
            subscriptions,
            observations,
        )
        self.devices = {device.serial: device for device in config.devices}
        self.keys: dict[str, str] = {}
        for device in config.devices:
            key = os.environ.get(device.api_key_env)
            if not key:
                raise ValueError(f"Missing credential environment variable {device.api_key_env}")
            self.keys[device.serial] = key
        self.controller_token = os.environ.get(config.controller_token_env)
        if any(device.control_enabled for device in config.devices) and not self.controller_token:
            raise ValueError("Control-enabled device requires a private controller token")
        self.command_lock = asyncio.Lock()

    @web.middleware
    async def device_boundary(self, request: web.Request, handler: Any) -> web.StreamResponse:
        path = normalize_url(request.path)
        if request.path == "/czfe":
            path = "/nest/transport"
        if request.path == "/info":
            return await handler(request)
        serial = extract_serial_from_request(request)
        if serial not in self.devices:
            raise web.HTTPForbidden(text="Device not enrolled")
        # Discovery accepts a configured serial; every data-bearing request needs
        # the provisioned device key, not just knowledge of its public serial.
        discovery = path.endswith(("/entry", "/passphrase", "/pro_info"))
        password = extract_basic_auth_password(request.headers.get("Authorization"))
        if not discovery and (not password or not hmac.compare_digest(password, self.keys[serial])):
            raise web.HTTPUnauthorized(text="Invalid device credential")
        await self.discard_expired_desires(serial)
        objects: list[dict[str, Any]] = []
        if request.method == "POST" and "/transport" in path:
            try:
                body = await request.json()
                if not isinstance(body, dict):
                    raise ValueError("Object required")
                objects = (
                    parse_put_body(body)[1]
                    if path.endswith("/put")
                    else parse_subscribe_body(body)[2]
                )
                if not isinstance(objects, list) or not all(
                    isinstance(obj, dict) for obj in objects
                ):
                    raise ValueError("Objects required")
            except (ValueError, TypeError, AttributeError) as error:
                raise web.HTTPBadRequest(text="Invalid transport payload") from error
            # A CAS-rejected payload must not qualify a pending write's readback.
            objects = [obj for obj in objects if self.cas_matches(serial, obj)]
            if not path.endswith("/put"):
                self.observations.record(serial, objects, time.time())
                # A new subscription may carry a manual change. Never replay an
                # unconfirmed old target from the previous connection's cache.
                await self.discard_expired_desires(serial, restart=True)
        response = await handler(request)
        if response.status == 200 and path.endswith("/put"):
            self.observations.record(serial, objects, time.time())
        return response

    async def discard_expired_desires(self, serial: str, restart: bool = False) -> None:
        rows = self.observations.db.execute(
            "SELECT id,payload,sent_at FROM commands WHERE status='dispatched'"
        ).fetchall()
        for command_id, payload, _sent_at in rows:
            command = json.loads(payload)
            if command["serial"] != serial or (not restart and command["expires_at"] > time.time()):
                continue
            with self.observations.db:
                self.observations.db.execute(
                    "UPDATE commands SET status='unconfirmed',result=? WHERE id=?",
                    ('{"error":"Dispatch expired or worker restarted; not replayed"}', command_id),
                )
            key = f"shared.{serial}"
            obj = self.state.get_object(serial, key)
            if obj:
                value = {
                    name: item
                    for name, item in obj.value.items()
                    if name
                    not in {
                        "target_temperature",
                        "target_temperature_low",
                        "target_temperature_high",
                        "target_temperature_type",
                        "target_change_pending",
                    }
                }
                await self.state.upsert_object(
                    DeviceObject(serial, key, 0, 0, value, datetime.now())
                )

    def cas_matches(self, serial: str, obj: dict[str, Any]) -> bool:
        key = obj.get("object_key")
        if not isinstance(key, str):
            return False
        existing = self.state.get_object(serial, key)
        conditional = obj.get("if_object_revision")
        return conditional is None or conditional == (existing.object_revision if existing else 0)

    async def clear_failed_desired(self, serial: str) -> None:
        key = f"shared.{serial}"
        obj = self.state.get_object(serial, key)
        if obj:
            value = {
                name: item for name, item in obj.value.items() if not name.startswith("target_")
            }
            await self.state.upsert_object(DeviceObject(serial, key, 0, 0, value, datetime.now()))

    async def dispatch(self, command: Command, now: float | None = None) -> dict[str, Any]:
        now = time.time() if now is None else now
        payload = command.model_dump_json()
        async with self.command_lock:
            previous = self.observations.db.execute(
                "SELECT payload FROM commands WHERE id=?", (command.id,)
            ).fetchone()
            if previous:
                if previous[0] != payload:
                    raise ValueError("Command ID reused with different payload")
                return self.observations.receipt(command.id) or {}
            device = self.devices.get(command.serial)
            if not device or not device.control_enabled:
                raise ValueError("Device control is not enabled")
            if not math.isfinite(command.expires_at) or not now < command.expires_at <= now + 30:
                raise ValueError("Command expired or lifetime exceeds 30 seconds")
            fields = self.observations.snapshot(command.serial)["fields"]
            mode = fields.get("shared_target_temperature_type")
            if not mode or mode["valid_until"] < now:
                raise ValueError("Fresh device-reported mode required")
            if not self.subscriptions.has_active_subscription(command.serial):
                raise ValueError(
                    "Device has no active subscription; commands are never queued offline"
                )
            if command.command == "set_temperature":
                if (
                    isinstance(command.value, str)
                    or not math.isfinite(command.value)
                    or not 7.222 <= command.value <= 32.222
                ):
                    raise ValueError("Temperature must be finite Celsius, 45–90°F")
                if mode["value"] not in ("heat", "cool"):
                    raise ValueError("Single target requires heat/cool mode")
                current_device = self.state.get_object(command.serial, f"device.{command.serial}")
                current_shared = self.state.get_object(command.serial, f"shared.{command.serial}")
                bounds = get_safety_bounds(
                    current_device.value if current_device else None,
                    current_shared.value if current_shared else None,
                )
                if (
                    not math.isfinite(bounds.min_celsius)
                    or not math.isfinite(bounds.max_celsius)
                    or not bounds.min_celsius <= command.value <= bounds.max_celsius
                ):
                    raise ValueError("Request outside device bounds; no silent clamping")
            elif command.command == "set_mode":
                if command.value not in ("off", "heat", "cool"):
                    raise ValueError("Unsupported mode")
                if command.value != "off":
                    capability = fields.get(f"device_can_{command.value}")
                    if (
                        not capability
                        or capability["valid_until"] < now
                        or capability["value"] is not True
                    ):
                        raise ValueError("Fresh equipment capability required")
            else:
                raise ValueError("Unsupported command")
            # Commit the intent before a side effect. An interrupted intent is
            # never reexecuted automatically after a restart.
            with self.observations.db:
                self.observations.db.execute(
                    "INSERT INTO commands VALUES(?,?,?,?,?)",
                    (command.id, payload, "dispatched", "{}", now),
                )
            try:
                await execute_command(
                    self.state, self.subscriptions, command.serial, command.command, command.value
                )
            except Exception:
                with self.observations.db:
                    self.observations.db.execute(
                        "UPDATE commands SET status='failed',result=? WHERE id=?",
                        ('{"error":"Protocol dispatch failed; no automatic replay"}', command.id),
                    )
                await self.clear_failed_desired(command.serial)
                raise
            return self.observations.receipt(command.id) or {}

    def control_app(self) -> web.Application:
        app = web.Application(client_max_size=16384)

        async def health(_request: web.Request) -> web.Response:
            return web.json_response(
                {
                    "service": "nest-proxy",
                    "enrolled_devices": len(self.devices),
                    "control_enabled_devices": sum(
                        d.control_enabled for d in self.devices.values()
                    ),
                    "weather": "unavailable-no-cloud",
                    "status": "ready" if self.devices else "awaiting-enrollment",
                }
            )

        async def states(_request: web.Request) -> web.Response:
            return web.json_response(
                {"devices": [self.observations.snapshot(serial) for serial in self.devices]}
            )

        async def receipt(request: web.Request) -> web.Response:
            result = self.observations.receipt(request.match_info["id"])
            return (
                web.json_response(result)
                if result
                else web.json_response({"error": "Unknown command"}, status=404)
            )

        async def command(request: web.Request) -> web.Response:
            token = request.headers.get("X-Controller-Token", "")
            if not self.controller_token or not hmac.compare_digest(token, self.controller_token):
                raise web.HTTPForbidden(text="Controller token required")
            try:
                value = Command.model_validate(await request.json())
                return web.json_response(await self.dispatch(value), status=202)
            except (ValueError, TypeError):
                return web.json_response(
                    {"error": "Invalid, stale, conflicting or disabled command"}, status=400
                )

        app.router.add_get("/health", health)
        app.router.add_get("/devices", states)
        app.router.add_get("/commands/{id}", receipt)
        app.router.add_post("/command", command)
        return app


def native_app(proxy: ThinProxy, availability: DeviceAvailability) -> web.Application:
    storage = proxy.state.storage
    if not isinstance(storage, SQLModelService):
        raise TypeError("Thin proxy requires its local SQLModel storage backend")
    app = create_proxy_app(
        proxy.state,
        proxy.subscriptions,
        NoCloudWeather(proxy.state.storage),
        availability,
        storage,
    )
    # Upstream's legacy normalizer invokes match_info.handler directly, bypassing
    # the remaining middleware. Register real aliases instead: every native and
    # legacy route then traverses precisely the same authentication boundary.
    app.middlewares.pop(0)
    app.middlewares.insert(0, proxy.device_boundary)
    for route in list(app.router.routes()):
        resource = route.resource
        if resource is None or not resource.canonical.startswith("/nest/"):
            continue
        path = resource.canonical
        aliases = [path.removeprefix("/nest")]
        if path.startswith("/nest/transport"):
            aliases.append("/czfe" + path.removeprefix("/nest/transport"))
        for alias in aliases:
            app.router.add_route(route.method, alias, route.handler)
    return app


async def run(config: ProxyConfig, state_dir: Path, socket_path: Path) -> None:
    state_dir.mkdir(parents=True, exist_ok=True, mode=0o700)
    socket_path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    settings.api_origin = config.api_origin
    settings.require_device_pairing = True
    settings.debug_logging = False
    settings.store_device_logs = False
    storage = SQLModelService(f"sqlite+aiosqlite:///{state_dir / 'protocol.sqlite'}")
    state = DeviceStateService(storage)
    await state.initialize()
    await storage.create_user(
        UserInfo("nest-proxy-controller", "controller@localhost", datetime.now())
    )
    for device in config.devices:
        await storage.set_device_owner(
            DeviceOwner(device.serial, "nest-proxy-controller", datetime.now())
        )
    subscriptions = NoReplaySubscriptions()
    observations = Observations(state_dir / "observations.sqlite", config.observation_ttl_seconds)
    proxy = ThinProxy(config, state, subscriptions, observations)
    availability = DeviceAvailability(subscriptions)
    for device in config.devices:
        await proxy.discard_expired_desires(device.serial, restart=True)
    app = native_app(proxy, availability)
    native_runner = web.AppRunner(app, keepalive_timeout=settings.connection_hold_timeout + 60)
    private_runner = web.AppRunner(proxy.control_app())
    await native_runner.setup()
    await private_runner.setup()
    if socket_path.exists():
        socket_path.unlink()
    await web.UnixSite(private_runner, str(socket_path)).start()
    socket_path.chmod(0o600)
    await web.TCPSite(native_runner, config.host, config.port).start()
    await availability.start()
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop.set)
    try:
        await stop.wait()
    finally:
        await availability.stop()
        await native_runner.cleanup()
        await private_runner.cleanup()
        await state.close()
        observations.db.close()
        socket_path.unlink(missing_ok=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--state-dir", required=True, type=Path)
    parser.add_argument("--socket", required=True, type=Path)
    args = parser.parse_args()
    os.umask(0o077)
    logging.getLogger().setLevel(logging.WARNING)
    asyncio.run(
        run(ProxyConfig.model_validate_json(args.config.read_text()), args.state_dir, args.socket)
    )


if __name__ == "__main__":
    main()
