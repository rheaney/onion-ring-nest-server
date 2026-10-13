# Onion Ring Nest server

Companion fork of [Cody Kociemba's MIT-licensed NoLongerEvil SelfHosted](https://github.com/codykociemba/NoLongerEvil-SelfHosted), pinned initially to `bca8781012639c1cefc4a08c103fc892158fb630`. Upstream license/attribution are retained. Device patch and installer live in [onion-ring-nest](https://github.com/rheaney/onion-ring-nest).

This branch adds an optional thin runtime in `src/nolongerevil/onion_ring_proxy.py`; the original upstream entrypoint remains unchanged. The initial upstream baseline passed 217 tests. The thin adapter is separately tested with synthetic loopback devices; those tests qualify protocol/software behavior, not real thermostat/HVAC safety.

## Local-first integration boundary

Treat this server as a **device protocol adapter**, not another automation policy owner. An existing single-writer HVAC controller should validate commands, hold/manual overrides, deduplicate request IDs and verify fresh physical telemetry before reporting confirmation. The server's `/command` response updates its stored bucket and notifies subscriptions; it does not itself prove physical thermostat readback.

Do not connect upstream MQTT climate command subscriptions directly to an existing home's controller-owned thermostat topics. That creates a competing writer and bypasses its durable request receipts. Prefer controller-to-server local Control API plus state ingestion into that controller; publish normalized broker state through the home's existing publisher. Disable the server's optional MQTT integration until a deliberate topic/ACL/read-only telemetry boundary is commissioned. Upstream raw MQTT field writes and HA-native writes are separate paths and both need review.

The firmware uses the original Nest application for ring, schedules, hardware interaction and safety behavior. Preserve that boundary. A local application UI should use shared device status/command components, not embed this server's dashboard or add a Nest-only parallel policy engine.

## Source-backed caveats

- `config/environment.py` defaults device and control listeners to all interfaces, and device pairing to false. Use explicit private bindings and enrollment during commissioning; do not expose Control API publicly.
- `main.create_control_app()` has debug/CORS middleware but no authentication middleware. The Control API is **not protected merely because the device protocol has auth**. Its permissive CORS is not authorization.
- `services/weather_service.py` still proxies `weather.nest.com` with TLS verification disabled. Self-hosted control is not a claim of zero external traffic. For a genuinely local-first variant, feed the existing local weather owner through an adapter and remove this optional vendor dependency.
- `routes/control/command.py` mutates server state before pushing subscribers. An API `success` is acceptance, not physical confirmation. Raw MQTT writes need separate safety/validation review.
- `utils/temperature_safety.py` clamps known temperature fields to reported/default bounds. This is server input validation, not independently proven compressor/furnace safety or a substitute for the retained thermostat firmware.
- Offline physical controls, offline schedules, loss of server/Wi-Fi, battery drain, reconnection and recovery remain bench qualification items. No hardware was connected during source review.

## Updates

Keep `main` as the upstream baseline; our delta lives on `onion-ring`. Fetch from `upstream`, inspect changes, merge a chosen commit, and rerun the focused suite plus physical qualification when hardware use is authorized. Never roll an unqualified `latest` container onto an installed thermostat. Keep LAN endpoints, serials, credentials and home policy outside this public fork.

## Optional thin runtime

`python -m nolongerevil.onion_ring_proxy --config <private-config.json> --state-dir <private-state-directory> --socket <private-control.sock>` reuses the existing device protocol but omits the dashboard, public Control API listener, optional MQTT command integration and vendor-weather HTTP client. A host-owned telemetry publisher can consume the private socket separately; this module does not implement policy or publish canonical thermostat state.

The configuration requires an explicit matching private IPv4 HTTP origin/bind, enrolled serials and per-device credential environment-variable names; an empty device list is a valid awaiting-enrollment mode. Never commit the actual configuration, credentials, serials or infrastructure addresses here. Device control is disabled by default. POST commands additionally require a controller token, fresh observed mode/capability, an active native subscription, a bounded TTL and a durable request ID. Observations are independent from the server's desired-state cache; dispatch is not reported as confirmation without a subsequent fresh matching device report. Failed/expired/interrupted commands are not replayed. No silent temperature clamping.

Legacy route aliases are registered explicitly so they traverse the same auth/observation middleware as canonical paths. This avoids the upstream URL normalizer's direct-handler middleware bypass. Broken-connection push buffering is disabled in this runtime. Vendor weather is deliberately unavailable rather than silently calling Google. Full physical/offline/power/recovery qualification is still required before connecting a thermostat or enabling writes.

October12 qualification: full suite **247 passed**, including **30 thin-proxy boundary/emulator tests**; Ruff, formatting and mypy pass. No real thermostat was used in these tests.
