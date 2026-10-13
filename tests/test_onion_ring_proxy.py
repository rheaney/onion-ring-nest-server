"""Boundary tests: no device/controller identity can be inferred from a serial."""

import asyncio
import json
import time
from base64 import b64encode
from datetime import datetime
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from nolongerevil.config import settings
from nolongerevil.lib.types import DeviceObject, DeviceOwner, UserInfo
from nolongerevil.onion_ring_proxy import (
    Command,
    NoReplaySubscriptions,
    Observations,
    ProxyConfig,
    ThinProxy,
    native_app,
)
from nolongerevil.services.device_availability import DeviceAvailability

SERIAL = "02AA01TESTDEVICE"
KEY = "test-only-device-key"


def config(control=False):
    return ProxyConfig.model_validate(
        {
            "host": "127.0.0.1",
            "port": 14710,
            "api_origin": "http://127.0.0.1:14710",
            "devices": [
                {"serial": SERIAL, "api_key_env": "TEST_NEST_KEY", "control_enabled": control}
            ],
        }
    )


@pytest.fixture
def proxy(state_service, tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_NEST_KEY", KEY)
    monkeypatch.setenv("NEST_PROXY_CONTROLLER_TOKEN", "test-only-controller-token")
    observations = Observations(tmp_path / "observations.sqlite", 90)
    result = ThinProxy(config(True), state_service, NoReplaySubscriptions(), observations)
    yield result
    observations.db.close()


def auth(password=KEY, serial=SERIAL):
    return {"Authorization": "Basic " + b64encode(f"{serial}:{password}".encode()).decode()}


async def native(proxy, aiohttp_client, monkeypatch):
    monkeypatch.setattr(settings, "require_device_pairing", True)
    monkeypatch.setattr(settings, "api_origin", "http://127.0.0.1:14710")
    storage = proxy.state.storage
    await storage.create_user(UserInfo("test-owner", "test@localhost", datetime.now()))
    await storage.set_device_owner(DeviceOwner(SERIAL, "test-owner", datetime.now()))
    availability = DeviceAvailability(proxy.subscriptions)
    app = native_app(proxy, availability)
    return await aiohttp_client(app)


async def reported(proxy, at=None):
    at = time.time() if at is None else at
    objects = [
        {
            "object_key": f"shared.{SERIAL}",
            "value": {"target_temperature_type": "heat", "target_temperature": 20.0},
        },
        {
            "object_key": f"device.{SERIAL}",
            "value": {"can_heat": True, "current_temperature": 19.5},
        },
    ]
    proxy.observations.record(SERIAL, objects, at)
    for obj in objects:
        await proxy.state.upsert_object(
            DeviceObject(SERIAL, obj["object_key"], 1, int(at * 1000), obj["value"], datetime.now())
        )


def command(identifier="test:1", value=21.0, lifetime=20):
    return Command(
        id=identifier,
        serial=SERIAL,
        command="set_temperature",
        value=value,
        expires_at=time.time() + lifetime,
    )


@pytest.mark.parametrize(
    "password,serial,status", [("wrong", SERIAL, 401), (KEY, "02AA01UNKNOWNDEV", 403)]
)
async def test_native_device_key_required(
    proxy, aiohttp_client, monkeypatch, password, serial, status
):
    client = await native(proxy, aiohttp_client, monkeypatch)
    response = await client.post(
        "/nest/transport/put", json={"objects": []}, headers=auth(password, serial)
    )
    assert response.status == status


async def test_no_device_enrollment_by_discovery(proxy, aiohttp_client, monkeypatch):
    proxy.devices.clear()
    client = await native(proxy, aiohttp_client, monkeypatch)
    response = await client.get("/entry", headers=auth())
    assert response.status == 403
    assert await proxy.state.storage.get_device_owner("02AA01UNKNOWNDEV") is None


async def test_native_alias_observed_no_secrets_or_value_echo(proxy, aiohttp_client, monkeypatch):
    client = await native(proxy, aiohttp_client, monkeypatch)
    response = await client.post(
        "/transport/v5/put",
        headers=auth(),
        json={
            "objects": [
                {
                    "object_key": f"device.{SERIAL}",
                    "value": {
                        "current_temperature": 19.5,
                        "api_key": "must-not-export",
                        "postal_code": "private-location",
                    },
                }
            ]
        },
    )
    assert response.status == 200
    assert all("value" not in obj for obj in (await response.json())["objects"])
    snapshot = proxy.observations.snapshot(SERIAL)
    assert snapshot["fields"]["device_current_temperature"]["value"] == 19.5
    assert "must-not-export" not in json.dumps(snapshot)
    assert "private-location" not in json.dumps(snapshot)


async def test_weather_does_not_contact_google(proxy, aiohttp_client, monkeypatch):
    client = await native(proxy, aiohttp_client, monkeypatch)
    response = await client.get("/nest/weather/v1", headers=auth())
    assert response.status == 502


async def test_private_command_requires_controller_token(proxy, aiohttp_client):
    client = await aiohttp_client(proxy.control_app())
    response = await client.post("/command", json=command().model_dump())
    assert response.status == 403


async def test_disabled_device_cannot_actuate(proxy):
    proxy.devices[SERIAL].control_enabled = False
    with pytest.raises(ValueError, match="not enabled"):
        await proxy.dispatch(command())


@pytest.mark.parametrize(
    "value,lifetime", [(float("nan"), 20), (float("inf"), 20), (40.0, 20), (21.0, -1), (21.0, 120)]
)
async def test_unsafe_expired_or_unbounded_command_rejected(proxy, monkeypatch, value, lifetime):
    await reported(proxy)
    monkeypatch.setattr(proxy.subscriptions, "has_active_subscription", lambda _serial: True)
    with pytest.raises(ValueError):
        await proxy.dispatch(command(value=value, lifetime=lifetime))
    assert proxy.observations.receipt("test:1") is None


async def test_fresh_readback_and_live_subscription_required(proxy, monkeypatch):
    with pytest.raises(ValueError, match="Fresh"):
        await proxy.dispatch(command())
    await reported(proxy, time.time() - 100)
    with pytest.raises(ValueError, match="Fresh"):
        await proxy.dispatch(command())
    await reported(proxy)
    with pytest.raises(ValueError, match="active subscription"):
        await proxy.dispatch(command())
    monkeypatch.setattr(proxy.subscriptions, "has_active_subscription", lambda _serial: True)
    assert (await proxy.dispatch(command()))["status"] == "dispatched"


async def test_dispatch_is_not_confirmation_and_duplicate_is_not_reexecuted(proxy, monkeypatch):
    await reported(proxy)
    monkeypatch.setattr(proxy.subscriptions, "has_active_subscription", lambda _serial: True)
    notify = AsyncMock()
    monkeypatch.setattr(proxy.subscriptions, "notify_all_subscribers", notify)
    request = command()
    result = await proxy.dispatch(request)
    assert result["status"] == "dispatched"
    assert proxy.state.get_object(SERIAL, f"shared.{SERIAL}").value["target_temperature"] == 21.0
    assert (
        proxy.observations.snapshot(SERIAL)["fields"]["shared_target_temperature"]["value"] == 20.0
    )
    assert (await proxy.dispatch(request))["status"] == "dispatched"
    assert notify.await_count == 1
    with pytest.raises(ValueError, match="reused"):
        await proxy.dispatch(request.model_copy(update={"value": 22.0}))
    proxy.observations.record(
        SERIAL,
        [{"object_key": f"shared.{SERIAL}", "value": {"target_temperature": 21.0}}],
        time.time(),
    )
    assert proxy.observations.receipt(request.id)["status"] == "confirmed"


async def test_old_device_timestamp_cannot_confirm_new_dispatch(proxy, monkeypatch):
    await reported(proxy)
    monkeypatch.setattr(proxy.subscriptions, "has_active_subscription", lambda _serial: True)
    request = command()
    await proxy.dispatch(request)
    proxy.observations.record(
        SERIAL,
        [
            {
                "object_key": f"shared.{SERIAL}",
                "object_timestamp": int((time.time() - 100) * 1000),
                "value": {"target_temperature": 21.0},
            }
        ],
        time.time(),
    )
    assert proxy.observations.receipt(request.id)["status"] == "dispatched"


async def test_dispatch_failure_removes_cached_actuator_desire(proxy, monkeypatch):
    await reported(proxy)
    monkeypatch.setattr(proxy.subscriptions, "has_active_subscription", lambda _serial: True)
    monkeypatch.setattr(
        proxy.subscriptions,
        "notify_all_subscribers",
        AsyncMock(side_effect=RuntimeError("test protocol failure")),
    )
    request = command()
    with pytest.raises(RuntimeError, match="test protocol failure"):
        await proxy.dispatch(request)
    assert proxy.observations.receipt(request.id)["status"] == "failed"
    assert "target_temperature" not in proxy.state.get_object(SERIAL, f"shared.{SERIAL}").value
    assert (await proxy.dispatch(request))["status"] == "failed"


async def test_device_bounds_never_silently_clamp_controller_request(proxy, monkeypatch):
    await reported(proxy)
    monkeypatch.setattr(proxy.subscriptions, "has_active_subscription", lambda _serial: True)
    shared = proxy.state.get_object(SERIAL, f"shared.{SERIAL}")
    shared.value["safety_temp_min"] = 23.0
    with pytest.raises(ValueError, match="no silent clamping"):
        await proxy.dispatch(command(value=21.0))
    assert proxy.observations.receipt("test:1") is None


async def test_restart_invalidates_desired_state_instead_of_replaying(proxy, monkeypatch):
    await reported(proxy)
    monkeypatch.setattr(proxy.subscriptions, "has_active_subscription", lambda _serial: True)
    request = command()
    await proxy.dispatch(request)
    await proxy.discard_expired_desires(SERIAL, restart=True)
    assert proxy.observations.receipt(request.id)["status"] == "unconfirmed"
    shared = proxy.state.get_object(SERIAL, f"shared.{SERIAL}")
    assert "target_temperature" not in shared.value
    assert shared.object_timestamp == 0
    assert (await proxy.dispatch(request))["status"] == "unconfirmed"


async def test_cas_rejected_report_does_not_qualify_readback(proxy, aiohttp_client, monkeypatch):
    await reported(proxy)
    client = await native(proxy, aiohttp_client, monkeypatch)
    response = await client.post(
        "/nest/transport/put",
        headers=auth(),
        json={
            "objects": [
                {
                    "object_key": f"shared.{SERIAL}",
                    "if_object_revision": 999,
                    "value": {"target_temperature": 29.0},
                }
            ]
        },
    )
    assert response.status == 200
    assert (
        proxy.observations.snapshot(SERIAL)["fields"]["shared_target_temperature"]["value"] == 20.0
    )


async def test_receipts_and_observations_survive_reopen(proxy, tmp_path):
    await reported(proxy)
    path = tmp_path / "observations.sqlite"
    other = Observations(path, 90)
    try:
        assert other.snapshot(SERIAL)["fields"]["shared_target_temperature"]["value"] == 20.0
    finally:
        other.db.close()


async def test_broken_connection_push_is_not_buffered(proxy):
    await proxy.subscriptions.store_pending_push(SERIAL, [{"target_temperature": 29.0}])
    subscription = await proxy.subscriptions.add_long_poll_subscription(SERIAL, "test-session")
    assert subscription.notify_queue.empty()
    await proxy.subscriptions.remove_long_poll_subscription(subscription)


@pytest.mark.parametrize("path", ["/nest/transport/put", "/transport/v5/put", "/czfe/v5/put"])
async def test_every_transport_alias_enforces_credentials(proxy, aiohttp_client, monkeypatch, path):
    client = await native(proxy, aiohttp_client, monkeypatch)
    response = await client.post(path, headers=auth("wrong"), json={"objects": []})
    assert response.status == 401


async def test_command_delivered_over_native_long_poll_then_confirmed_by_put(
    proxy, aiohttp_client, monkeypatch
):
    client = await native(proxy, aiohttp_client, monkeypatch)
    await reported(proxy)
    # First subscribe creates the owner/pairing buckets. The next one is synced.
    first = await client.post(
        "/nest/transport", headers=auth(), json={"chunked": False, "objects": []}
    )
    assert first.status == 200
    await first.read()
    objects = [
        {
            "object_key": obj.object_key,
            "object_revision": obj.object_revision,
            "object_timestamp": obj.object_timestamp,
        }
        for obj in proxy.state.get_objects_by_serial(SERIAL)
    ]
    registered = asyncio.Event()
    add = proxy.subscriptions.add_long_poll_subscription

    async def register(*args):
        result = await add(*args)
        registered.set()
        return result

    monkeypatch.setattr(proxy.subscriptions, "add_long_poll_subscription", register)
    stream = await client.post(
        "/nest/transport",
        headers=auth(),
        json={"chunked": True, "session": "test-session", "objects": objects},
    )
    assert stream.headers["Transfer-Encoding"] == "chunked"
    await asyncio.wait_for(registered.wait(), 5)
    private = await aiohttp_client(proxy.control_app())
    request = command("test:wire")
    response = await private.post(
        "/command",
        headers={"X-Controller-Token": "test-only-controller-token"},
        json=request.model_dump(),
    )
    assert response.status == 202
    assert (await response.json())["status"] == "dispatched"
    pushed = json.loads(await asyncio.wait_for(stream.content.readany(), 5))
    assert any(obj.get("value", {}).get("target_temperature") == 21.0 for obj in pushed["objects"])
    # Receiving a push is still not confirmation. Only a later device report is.
    assert proxy.observations.receipt(request.id)["status"] == "dispatched"
    report = await client.post(
        "/nest/transport/put",
        headers=auth(),
        json={
            "objects": [{"object_key": f"shared.{SERIAL}", "value": {"target_temperature": 21.0}}]
        },
    )
    assert report.status == 200
    receipt = await private.get("/commands/test:wire")
    assert (await receipt.json())["status"] == "confirmed"
    await stream.read()


@pytest.mark.parametrize(
    "origin",
    [
        "http://example.com:14710",
        "http://127.0.0.1:8000",
        "https://127.0.0.1:14710",
        "http://user:pass@127.0.0.1:14710",
        "http://127.0.0.1:14710/entry",
    ],
)
def test_origin_is_explicit_private_bind(origin):
    with pytest.raises(ValidationError):
        ProxyConfig(host="127.0.0.1", port=14710, api_origin=origin)
