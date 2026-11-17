"""DataUpdateCoordinator that owns the long-lived OmniClient connection.

Lifecycle
---------
1. ``async_config_entry_first_refresh`` connects, runs a one-time
   *discovery* pass that enumerates every named zone / unit / area /
   thermostat / button / program on the panel, and seeds ``self.data``
   with a populated :class:`OmniData`.
2. ``_async_update_data`` is then called every :data:`SCAN_INTERVAL` to
   re-poll *live state only* (extended status for zones / units /
   thermostats, basic status for areas).
3. A background task (:meth:`_run_event_listener`) consumes
   :meth:`OmniClient.events` for the lifetime of the entry; whenever a
   typed :class:`SystemEvent` arrives, the relevant slice of state is
   patched in-place and ``async_set_updated_data`` fires so HA pushes
   updates to subscribed entities without waiting for the next poll.

The library's :class:`OmniClient` is the *only* thing that talks to the
wire. We keep one client per coordinator and close it on shutdown; on a
recoverable :class:`OmniConnectionError` we drop and recreate it on the
next refresh, preserving the existing :class:`OmniData` so entities don't
flicker to "unavailable" between attempts.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from dataclasses import dataclass, field, replace
from typing import Final

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryAuthFailed
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.update_coordinator import DataUpdateCoordinator, UpdateFailed

from omni_pca.client import ObjectType as ClientObjectType
from omni_pca.client import OmniClient
from omni_pca.connection import (
    ConnectionError as OmniConnectionError,
)
from omni_pca.connection import (
    HandshakeError,
    InvalidEncryptionKeyError,
    RequestTimeoutError,
)
from omni_pca.events import (
    AcLost,
    AcRestored,
    AlarmActivated,
    AlarmCleared,
    ArmingChanged,
    BatteryLow,
    BatteryRestored,
    SystemEvent,
    UnitStateChanged,
    ZoneStateChanged,
)
from omni_pca.models import (
    OBJECT_TYPE_TO_PROPERTIES,
    AreaProperties,
    AreaStatus,
    AuxSensorStatus,
    ButtonProperties,
    ObjectType,
    SystemInformation,
    SystemStatus,
    StatusReply,
    ThermostatProperties,
    ThermostatStatus,
    UnitProperties,
    UnitStatus,
    ZoneProperties,
    ZoneStatus,
)
from omni_pca.opcodes import OmniLink2MessageType
from omni_pca.programs import Program

from .const import (
    DOMAIN,
    EVENT_TASK_NAME,
    LOGGER,
    MANUFACTURER,
    MAX_OBJECT_INDEX,
    SCAN_INTERVAL,
    STATUS_PUSH_TASK_NAME,
)

# If the panel has pushed at least once but then goes silent this long while
# polling still succeeds, the push stream is presumed dead — force a
# reconnect, which re-sends EnableNotifications. The panel's AuxSensor pushes
# are bursty (a few at 30 s spacing, then ~270 s of silence, observed on a
# live Omni Pro II), so this must sit well above that gap. Never armed on a
# panel that has never pushed.
PUSH_WATCHDOG_SECS: Final = 900

# Entity the Health dashboard's Logbook card is filtered on; entries written
# via logbook.log attach to it. Best-effort: never raises.
HEALTH_LOG_ENTITY: Final = "input_text.health_log"

# --------------------------------------------------------------------------
# Public data shape exposed to entities
# --------------------------------------------------------------------------


@dataclass(slots=True)
class OmniData:
    """Snapshot of everything a coordinator's entities can read.

    Discovery dictionaries (``zones``, ``units``, ``areas``,
    ``thermostats``, ``buttons``, ``programs``) are populated once on
    first refresh and never re-walked — they describe panel topology,
    which only changes when the installer reprograms the controller and
    the user reloads the integration.

    Live ``*_status`` dictionaries are re-populated on every poll *and*
    patched in-place from the event listener.
    """

    system_info: SystemInformation
    zones: dict[int, ZoneProperties] = field(default_factory=dict)
    units: dict[int, UnitProperties] = field(default_factory=dict)
    areas: dict[int, AreaProperties] = field(default_factory=dict)
    thermostats: dict[int, ThermostatProperties] = field(default_factory=dict)
    buttons: dict[int, ButtonProperties] = field(default_factory=dict)
    programs: dict[int, Program] = field(default_factory=dict)

    zone_status: dict[int, ZoneStatus] = field(default_factory=dict)
    unit_status: dict[int, UnitStatus] = field(default_factory=dict)
    area_status: dict[int, AreaStatus] = field(default_factory=dict)
    thermostat_status: dict[int, ThermostatStatus] = field(default_factory=dict)
    aux_sensor_status: dict[int, AuxSensorStatus] = field(default_factory=dict)

    system_status: SystemStatus | None = None
    last_event: SystemEvent | None = None


# --------------------------------------------------------------------------
# Coordinator
# --------------------------------------------------------------------------


class OmniDataUpdateCoordinator(DataUpdateCoordinator[OmniData]):
    """Coordinator that owns one :class:`OmniClient` and one panel device."""

    config_entry: ConfigEntry

    def __init__(
        self,
        hass: HomeAssistant,
        entry: ConfigEntry,
        *,
        host: str,
        port: int,
        controller_key: bytes,
        transport: str = "tcp",
        pca_path: str | None = None,
        pca_key: int = 0,
    ) -> None:
        from datetime import timedelta

        from .const import CONF_SCAN_INTERVAL

        scan_seconds = entry.options.get(
            CONF_SCAN_INTERVAL, int(SCAN_INTERVAL.total_seconds())
        )
        super().__init__(
            hass,
            LOGGER,
            name=f"{DOMAIN} {host}:{port}",
            update_interval=timedelta(seconds=scan_seconds),
            config_entry=entry,
        )
        self._host = host
        self._port = port
        self._controller_key = controller_key
        self._transport = transport
        self._pca_path = pca_path
        self._pca_key = pca_key
        self._client: OmniClient | None = None
        self._discovery_done = False
        self._discovered: OmniData | None = None
        self._event_task: asyncio.Task[None] | None = None
        self._status_push_task: asyncio.Task[None] | None = None
        self._push_seen = False
        self._unreachable = False
        self._last_push = time.monotonic()

    # ---- public surface --------------------------------------------------

    @property
    def unique_id(self) -> str:
        """Stable identifier for this panel (host:port)."""
        return f"{self._host}:{self._port}"

    @property
    def client(self) -> OmniClient:
        """The live OmniClient. Raises if the coordinator hasn't connected yet."""
        if self._client is None:
            raise RuntimeError("OmniClient is not connected")
        return self._client

    @property
    def device_info(self) -> DeviceInfo:
        """DeviceInfo for the single hub device this coordinator represents."""
        info = self._discovered.system_info if self._discovered is not None else None
        return DeviceInfo(
            identifiers={(DOMAIN, self.unique_id)},
            name=info.model_name if info is not None else "Omni Panel",
            manufacturer=MANUFACTURER,
            model=info.model_name if info is not None else None,
            sw_version=info.firmware_version if info is not None else None,
            configuration_url=None,
        )

    async def async_shutdown(self) -> None:
        """Tear down the event task and the client connection on unload."""
        await self._cancel_event_task()
        await self._cancel_status_push_task()
        await self._drop_client()
        await super().async_shutdown()

    # ---- DataUpdateCoordinator hook -------------------------------------

    async def _async_update_data(self) -> OmniData:
        await self._check_push_watchdog()
        try:
            client = await self._ensure_connected()
            if not self._discovery_done:
                self._discovered = await self._run_discovery(client)
                self._discovery_done = True
                # NOTE: _start_event_task() (SystemEvents/opcode-55 listener)
                # is intentionally NOT started. Against a real OmniPro II,
                # opcode 55 never occurs — the panel's actual push mechanism
                # is unsolicited ExtendedStatus (opcode 59), handled below.
                # Running both listeners concurrently means they race to
                # drain the same unsolicited() queue: whichever task's
                # queue.get() wins a given item gets it, and the other
                # loses it permanently — this was silently dropping roughly
                # half of every real push (see docs/JOURNEY.md).

            # Idempotent, and run EVERY cycle (not just after discovery): the
            # listener is bound to one client, and a dropped/recreated client
            # (e.g. panel connection reset) needs a fresh listener or pushes
            # stop forever and we silently fall back to polling.
            self._start_status_push_task(client)

            assert self._discovered is not None
            base = self._discovered
            zone_status = await self._poll_zone_status(client, base.zones)
            unit_status = await self._poll_unit_status(client, base.units)
            area_status = await self._poll_area_status(client, base.areas)
            thermostat_status = await self._poll_thermostat_status(
                client, base.thermostats
            )
            system_status = await self._safe_system_status(client)
        except (InvalidEncryptionKeyError, HandshakeError) as err:
            # Surface as auth failure so HA triggers the reauth flow.
            await self._drop_client()
            raise ConfigEntryAuthFailed(str(err)) from err
        except (OmniConnectionError, RequestTimeoutError, OSError) as err:
            await self._drop_client()
            if not self._unreachable:  # log the first failure, not every retry
                self._unreachable = True
                self._health_log(f"panel unreachable: {err}")
            raise UpdateFailed(f"panel unreachable: {err}") from err

        if self._unreachable:
            self._unreachable = False
            self._health_log("panel reachable again")

        # Preserve any last_event already captured by the event task; the
        # poll path doesn't see push events so it must not overwrite it.
        last_event = self.data.last_event if self.data is not None else None

        # Same problem, same fix, for aux_sensor_status: this poll cycle
        # rebuilds from self._discovered (always an empty dict here, since
        # discovery never populates it) rather than self.data, so without
        # this it silently wiped out whatever the push listener had just
        # set — producing an ~30s alternating real-value/stale-fallback
        # flicker on every analog-zone-as-aux-sensor reading (confirmed
        # against a live panel's outdoor temp probe).
        aux_sensor_status = (
            self.data.aux_sensor_status if self.data is not None else {}
        )

        return replace(
            self._discovered,
            zone_status=zone_status,
            unit_status=unit_status,
            area_status=area_status,
            thermostat_status=thermostat_status,
            system_status=system_status,
            last_event=last_event,
            aux_sensor_status=aux_sensor_status,
        )

    # ---- connection management ------------------------------------------

    async def _ensure_connected(self) -> OmniClient:
        if self._client is not None:
            return self._client
        if self._transport == "udp":
            # Panels listening UDP-only speak the v1 wire protocol, not
            # v2. The adapter exposes the OmniClient API surface this
            # coordinator was written against, but underneath it drives
            # an OmniConnectionV1 + the typed v1 status/command opcodes.
            from omni_pca.v1 import OmniClientV1Adapter

            client: OmniClient = OmniClientV1Adapter(  # type: ignore[assignment]
                self._host,
                port=self._port,
                controller_key=self._controller_key,
            )
        else:
            client = OmniClient(
                self._host,
                port=self._port,
                controller_key=self._controller_key,
                transport=self._transport,  # type: ignore[arg-type]
            )
        # Drive __aenter__ manually so the client survives across update
        # cycles; we close it explicitly on shutdown / failure.
        await client.__aenter__()
        self._client = client
        return client

    async def _drop_client(self) -> None:
        if self._client is None:
            return
        client = self._client
        self._client = None
        # The push listener is parked on this client's unsolicited queue,
        # which never signals close — it must be cancelled explicitly.
        await self._cancel_status_push_task()
        try:
            await client.__aexit__(None, None, None)
        except Exception:  # pragma: no cover - best-effort cleanup
            LOGGER.debug("error during client cleanup", exc_info=True)

    # ---- discovery -------------------------------------------------------

    async def _run_discovery(self, client: OmniClient) -> OmniData:
        """Walk every object type once and stash the static topology."""
        system_info = await client.get_system_information()

        zones = await self._discover_zones(client)
        units = await self._discover_units(client)
        areas = await self._discover_areas(client)
        thermostats = await self._discover_thermostats(client)
        buttons = await self._discover_buttons(client)
        programs = await self._discover_programs(client)

        LOGGER.info(
            "omni_pca discovery: %d zones, %d units, %d areas, "
            "%d thermostats, %d buttons, %d programs",
            len(zones),
            len(units),
            len(areas),
            len(thermostats),
            len(buttons),
            len(programs),
        )
        return OmniData(
            system_info=system_info,
            zones=zones,
            units=units,
            areas=areas,
            thermostats=thermostats,
            buttons=buttons,
            programs=programs,
        )

    # Stopgap until RequestCapacities (opcode 30/31 — defined in
    # opcodes.py but never implemented/parsed anywhere in this library)
    # is wired up to bound discovery by the panel's actual configured
    # capacity: list_zone_names() walks the full protocol index space
    # (1..MAX_OBJECT_INDEX) looking for anything with a non-empty name,
    # and on a panel that's been reconfigured/downsized over its life
    # (16 physically-wired zones here), leftover garbage bytes in unused
    # EEPROM slots at much higher indices can decode to a non-empty
    # (garbled) "name" — creating real, un-deletable HA entities for
    # objects that don't exist in PC Access's own UI at all (its Zones
    # table caps at the panel's real 16). Deliberately NOT applied to
    # Units — those legitimately run much higher (Outputs commonly sit
    # in the 300s+ on this hardware), so a blanket per-object-type cap
    # would hide real, working objects.
    _MAX_REALISTIC_ZONE_INDEX: Final = 64

    async def _discover_zones(
        self, client: OmniClient
    ) -> dict[int, ZoneProperties]:
        names = await self._best_effort(client.list_zone_names, default={})
        names = {
            i: n for i, n in names.items() if i <= self._MAX_REALISTIC_ZONE_INDEX
        }
        out: dict[int, ZoneProperties] = {}
        for index in sorted(names):
            try:
                props = await client.get_object_properties(
                    ClientObjectType.ZONE, index
                )
            except (OmniConnectionError, RequestTimeoutError):
                raise
            except Exception:
                LOGGER.debug("zone %d properties fetch failed", index, exc_info=True)
                continue
            if isinstance(props, ZoneProperties):
                out[index] = props
        return out

    async def _discover_units(
        self, client: OmniClient
    ) -> dict[int, UnitProperties]:
        names = await self._best_effort(client.list_unit_names, default={})
        out: dict[int, UnitProperties] = {}
        for index in sorted(names):
            try:
                props = await client.get_object_properties(
                    ClientObjectType.UNIT, index
                )
            except (OmniConnectionError, RequestTimeoutError):
                raise
            except Exception:
                LOGGER.debug("unit %d properties fetch failed", index, exc_info=True)
                continue
            if isinstance(props, UnitProperties):
                out[index] = props
        return out

    async def _discover_areas(
        self, client: OmniClient
    ) -> dict[int, AreaProperties]:
        names = await self._best_effort(client.list_area_names, default={})
        out: dict[int, AreaProperties] = {}
        for index in sorted(names):
            try:
                props = await client.get_object_properties(
                    ClientObjectType.AREA, index
                )
            except (OmniConnectionError, RequestTimeoutError):
                raise
            except Exception:
                LOGGER.debug("area %d properties fetch failed", index, exc_info=True)
                continue
            if isinstance(props, AreaProperties):
                out[index] = props
        return out

    async def _discover_thermostats(
        self, client: OmniClient
    ) -> dict[int, ThermostatProperties]:
        """Walk thermostat properties via the low-level connection.

        The high-level :meth:`OmniClient.get_object_properties` only knows
        zone/unit/area parsers in v1.0 of the library; thermostats are in
        :data:`OBJECT_TYPE_TO_PROPERTIES` on the model side, so we drive
        the wire ourselves and parse with the model's class.
        """
        return await self._walk_properties(
            client, ObjectType.THERMOSTAT, ThermostatProperties
        )

    async def _discover_buttons(
        self, client: OmniClient
    ) -> dict[int, ButtonProperties]:
        return await self._walk_properties(
            client, ObjectType.BUTTON, ButtonProperties
        )

    async def _discover_programs(
        self, client: OmniClient
    ) -> dict[int, Program]:
        """Enumerate defined panel programs.

        Two sources, in order of preference:

        1. ``CONF_PCA_PATH`` is configured → parse the .pca file and
           extract the programs block. Avoids streaming 1500 records on
           every entry refresh and works against an offline snapshot.
        2. Otherwise → enumerate over the wire:
           * v2 (TCP): ``client.iter_programs()`` drives UploadProgram
             with request_reason=1 ("next defined after slot").
           * v1 (UDP): adapter forwards to OmniClientV1.iter_programs(),
             a bare UploadPrograms stream ack-walked to EOD.

        Both paths yield :class:`omni_pca.programs.Program` and skip
        empty slots. Errors are logged and swallowed — programs are
        non-critical discovery, so a partial list beats blocking setup.
        """
        if self._pca_path:
            return await self._discover_programs_from_pca()
        out: dict[int, Program] = {}
        try:
            async for prog in client.iter_programs():
                if prog.slot is not None:
                    out[prog.slot] = prog
        except (OmniConnectionError, RequestTimeoutError):
            raise
        except Exception:
            LOGGER.debug(
                "program enumeration interrupted (kept %d)", len(out), exc_info=True
            )
        return out

    async def _discover_programs_from_pca(self) -> dict[int, Program]:
        """Parse the configured .pca file and pull out its programs block.

        Runs the disk I/O on the executor since :mod:`pca_file` does
        sync reads. Any failure (missing file, bad key, malformed block)
        is logged and downgraded to an empty dict — the rest of
        discovery still works.
        """
        from pathlib import Path

        from omni_pca.pca_file import parse_pca_file

        try:
            data = await self.hass.async_add_executor_job(
                Path(self._pca_path).read_bytes
            )
            acct = await self.hass.async_add_executor_job(
                lambda: parse_pca_file(data, key=self._pca_key)
            )
        except Exception:
            LOGGER.warning(
                "failed to load programs from %s — falling back to empty list",
                self._pca_path, exc_info=True,
            )
            return {}
        out: dict[int, Program] = {}
        for prog in acct.programs:
            if prog.slot is None or prog.is_empty():
                continue
            out[prog.slot] = prog
        return out

    async def _walk_properties(
        self,
        client: OmniClient,
        object_type: ObjectType,
        parser: type,
    ) -> dict[int, object]:
        """Walk every defined object of ``object_type`` and parse with ``parser``.

        Mirrors the strategy used by ``OmniClient._walk_named_objects`` but
        works for any model in :data:`OBJECT_TYPE_TO_PROPERTIES` (the
        client's internal parser table only covers zones/units/areas in
        v1.0). We drive ``RequestProperties`` directly on the connection
        so we don't have to monkey-patch the library.

        On UDP/v1 panels there is no ``RequestProperties`` opcode at all,
        so we fall back to the v1 adapter's name-stream-based discovery
        (each object's ``Properties`` is synthesized from its name).
        """
        if parser is None or OBJECT_TYPE_TO_PROPERTIES.get(int(object_type)) is None:
            return {}
        if self._transport == "udp":
            return await self._walk_properties_v1(client, object_type)
        out: dict[int, object] = {}
        cursor = 0
        conn = client.connection
        # Manual request/reply loop with relative_direction=1 (=next).
        for _ in range(MAX_OBJECT_INDEX):
            payload = bytes(
                [
                    int(object_type),
                    (cursor >> 8) & 0xFF,
                    cursor & 0xFF,
                    1,        # relative_direction = next
                    0, 0, 0,  # filter1..3
                ]
            )
            try:
                reply = await conn.request(
                    OmniLink2MessageType.RequestProperties, payload
                )
            except RequestTimeoutError:
                break
            if reply.opcode == int(OmniLink2MessageType.EOD):
                break
            if reply.opcode != int(OmniLink2MessageType.Properties):
                break
            try:
                obj = parser.parse(reply.payload)
            except Exception:
                LOGGER.debug(
                    "parse failed for %s past index %d",
                    object_type.name,
                    cursor,
                    exc_info=True,
                )
                break
            # Object name being empty is OK for buttons/programs but the
            # spec says "named only" — we still keep the entry as a
            # candidate; entity setup filters by truthiness.
            index_attr = getattr(obj, "index", None)
            name_attr = getattr(obj, "name", "")
            if index_attr is None:
                break
            if name_attr:
                out[index_attr] = obj
            cursor = index_attr
            if cursor >= MAX_OBJECT_INDEX:
                break
        return out

    async def _walk_properties_v1(
        self, client: OmniClient, object_type: ObjectType
    ) -> dict[int, object]:
        """V1 fallback for :meth:`_walk_properties`.

        v1 has no RequestProperties opcode — names come from streaming
        UploadNames and the rest of the Properties fields can't be
        recovered from the wire. We delegate to the adapter's
        ``get_object_properties`` (which synthesizes a minimal record
        from the cached name list) and skip anything it returns ``None``
        for.
        """
        # Pick the right per-type name lister. The adapter caches the
        # UploadNames stream output so these are nearly free after the
        # first call this discovery pass.
        if object_type == ObjectType.THERMOSTAT:
            names = await client.list_thermostat_names()  # type: ignore[attr-defined]
        elif object_type == ObjectType.BUTTON:
            names = await client.list_button_names()  # type: ignore[attr-defined]
        else:
            # Programs / Messages / etc — nothing to walk.
            return {}
        out: dict[int, object] = {}
        for idx in sorted(names):
            try:
                props = await client.get_object_properties(object_type, idx)
            except Exception:
                LOGGER.debug(
                    "v1 properties synth failed for %s #%d",
                    object_type.name, idx, exc_info=True,
                )
                continue
            if props is not None:
                out[idx] = props
        return out

    @staticmethod
    async def _best_effort(coro_fn, *, default):
        """Call ``coro_fn()`` and swallow non-transport errors, returning ``default``.

        We let :class:`OmniConnectionError` / :class:`RequestTimeoutError`
        propagate so the coordinator can drop the client and reconnect;
        anything else (a parse failure on a particular reply, NAK on a
        feature the panel doesn't support) is downgraded to a debug log.
        """
        try:
            return await coro_fn()
        except (OmniConnectionError, RequestTimeoutError):
            raise
        except Exception:
            LOGGER.debug("best-effort %s failed", coro_fn.__name__, exc_info=True)
            return default

    # ---- live polling ----------------------------------------------------

    # Panels with sparse object numbering (real-world Omni Pro II panels
    # routinely have zones/units scattered across a wide index space —
    # e.g. 39 zones living somewhere inside 1..214, or 17 units scattered
    # across 1..392 because Outputs/other object types share the same
    # numbering range) will NAK a single RequestExtendedStatus spanning
    # the whole 1..max(index) range: most of that span is undefined
    # slots, and real firmware rejects it outright rather than returning
    # sparse/partial data. We instead walk only the *actually defined*
    # indices, in small contiguous runs, so every request maps to real
    # objects the panel knows about.
    _STATUS_CHUNK_SIZE = 16

    @staticmethod
    def _contiguous_chunks(indices: list[int], chunk_size: int) -> list[tuple[int, int]]:
        """Group sorted indices into (start, end) runs of consecutive ints.

        A run is further split so no single request spans more than
        ``chunk_size`` indices, since even a real, fully-defined range
        may exceed whatever the panel's reply buffer can hold in one
        RequestExtendedStatus/RequestStatus round trip.
        """
        if not indices:
            return []
        chunks: list[tuple[int, int]] = []
        run_start = run_prev = indices[0]
        for idx in indices[1:]:
            if idx == run_prev + 1 and (idx - run_start) < chunk_size:
                run_prev = idx
                continue
            chunks.append((run_start, run_prev))
            run_start = run_prev = idx
        chunks.append((run_start, run_prev))
        return chunks

    async def _poll_status_chunked(
        self,
        client: OmniClient,
        object_type: ObjectType,
        defined: dict[int, object],
        status_cls: type,
        *,
        label: str,
    ) -> dict[int, object]:
        """Poll ExtendedStatus for ``defined``'s indices only, chunk by chunk.

        Each chunk failure is caught independently so one bad/oversized
        run doesn't blank out status for every other object of this
        type; we keep whatever the previous poll had for that chunk.
        """
        if not defined:
            return {}
        previous = getattr(self.data, f"{label}_status", {}) if self.data else {}
        out: dict[int, object] = dict(previous)
        for start, end in self._contiguous_chunks(
            sorted(defined), self._STATUS_CHUNK_SIZE
        ):
            try:
                records = await client.get_extended_status(object_type, start, end)
            except (OmniConnectionError, RequestTimeoutError):
                raise
            except Exception:
                LOGGER.debug(
                    "%s extended_status poll failed for range %d..%d",
                    label, start, end, exc_info=True,
                )
                continue
            for r in records:
                if isinstance(r, status_cls) and r.index in defined:
                    out[r.index] = r
        return {k: v for k, v in out.items() if k in defined}

    async def _poll_zone_status(
        self, client: OmniClient, zones: dict[int, ZoneProperties]
    ) -> dict[int, ZoneStatus]:
        return await self._poll_status_chunked(
            client, ObjectType.ZONE, zones, ZoneStatus, label="zone"
        )

    async def _poll_unit_status(
        self, client: OmniClient, units: dict[int, UnitProperties]
    ) -> dict[int, UnitStatus]:
        return await self._poll_status_chunked(
            client, ObjectType.UNIT, units, UnitStatus, label="unit"
        )

    async def _poll_area_status(
        self, client: OmniClient, areas: dict[int, AreaProperties]
    ) -> dict[int, AreaStatus]:
        if not areas:
            return {}
        end = max(areas)
        try:
            records = await client.get_object_status(ObjectType.AREA, 1, end)
        except (OmniConnectionError, RequestTimeoutError):
            raise
        except Exception:
            LOGGER.debug("area status poll failed", exc_info=True)
            return self.data.area_status if self.data is not None else {}
        return {
            r.index: r
            for r in records
            if isinstance(r, AreaStatus) and r.index in areas
        }

    async def _poll_thermostat_status(
        self, client: OmniClient, thermostats: dict[int, ThermostatProperties]
    ) -> dict[int, ThermostatStatus]:
        if not thermostats:
            return {}
        end = max(thermostats)
        try:
            records = await client.get_extended_status(
                ObjectType.THERMOSTAT, 1, end
            )
        except (OmniConnectionError, RequestTimeoutError):
            raise
        except Exception:
            LOGGER.debug("thermostat extended_status poll failed", exc_info=True)
            return (
                self.data.thermostat_status if self.data is not None else {}
            )
        return {
            r.index: r
            for r in records
            if isinstance(r, ThermostatStatus) and r.index in thermostats
        }

    async def _safe_system_status(
        self, client: OmniClient
    ) -> SystemStatus | None:
        try:
            return await client.get_system_status()
        except (OmniConnectionError, RequestTimeoutError):
            raise
        except Exception:
            LOGGER.debug("get_system_status failed", exc_info=True)
            return None

    # ---- event listener --------------------------------------------------

    def _start_event_task(self) -> None:
        if self._event_task is not None and not self._event_task.done():
            return
        self._event_task = self.config_entry.async_create_background_task(
            self.hass,
            self._run_event_listener(),
            EVENT_TASK_NAME,
        )

    async def _cancel_event_task(self) -> None:
        if self._event_task is None:
            return
        task = self._event_task
        self._event_task = None
        if not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

    # ---- status-push listener ---------------------------------------------
    #
    # Discovered against a live OmniPro II (see docs/JOURNEY.md): the
    # panel's actual push mechanism is *not* SystemEvents (opcode 55,
    # what _run_event_listener/EventStream above assumes) — it's an
    # unsolicited, single-record ExtendedStatus message (opcode 59),
    # wire-identical to a polled reply. This fires for Zones, Units
    # (including "Extended"/UPB-addressed ones with no real UPB bus —
    # the panel tracks and broadcasts their commanded state regardless),
    # and Aux Sensors. Running this as a second, independent listener
    # task means live state reaches entities immediately rather than
    # waiting for the next SCAN_INTERVAL poll.

    def _start_status_push_task(self, client: OmniClient) -> None:
        if self._status_push_task is not None and not self._status_push_task.done():
            return
        self._last_push = time.monotonic()
        self._status_push_task = self.config_entry.async_create_background_task(
            self.hass,
            self._run_status_push_listener(client),
            STATUS_PUSH_TASK_NAME,
        )

    def _health_log(self, message: str) -> None:
        """Write one Logbook entry for the Health dashboard (best effort)."""

        async def _write() -> None:
            try:
                await self.hass.services.async_call(
                    "logbook",
                    "log",
                    {
                        "name": "Omni panel",
                        "message": message,
                        "entity_id": HEALTH_LOG_ENTITY,
                    },
                )
            except Exception:  # pragma: no cover - logging must never break polling
                LOGGER.debug("health log write failed", exc_info=True)

        self.hass.async_create_task(_write())

    async def _check_push_watchdog(self) -> None:
        """Reconnect if pushes were flowing but have gone silent."""
        if self._client is None or not self._push_seen:
            return
        silent = time.monotonic() - self._last_push
        if silent > PUSH_WATCHDOG_SECS:
            LOGGER.warning(
                "no status push from panel for %.0f s while polling succeeds; "
                "reconnecting to re-arm push notifications",
                silent,
            )
            self._health_log(
                f"no status push for {silent:.0f} s; reconnecting to re-arm push notifications"
            )
            self._last_push = time.monotonic()  # no reconnect storm
            await self._drop_client()

    async def _cancel_status_push_task(self) -> None:
        if self._status_push_task is None:
            return
        task = self._status_push_task
        self._status_push_task = None
        if not task.done():
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task

    async def _run_status_push_listener(self, client: OmniClient) -> None:
        """Background loop: consume pushed ExtendedStatus records live.

        Bound to the ``client`` it was started for; ``_drop_client`` cancels
        it, and ``_async_update_data`` starts a fresh one for the new client.
        """
        try:
            async for record in client.status_pushes():
                self._apply_status_push(record)
        except asyncio.CancelledError:
            raise
        except (OmniConnectionError, RequestTimeoutError, OSError):
            LOGGER.debug(
                "status push listener exited on transport error", exc_info=True
            )
        except Exception:  # pragma: no cover - defensive
            LOGGER.exception("status push listener crashed")

    def _apply_status_push(self, record: StatusReply) -> None:
        """Merge one pushed status record into ``self.data`` immediately."""
        LOGGER.info("status push received: %r", record)
        self._push_seen = True
        self._last_push = time.monotonic()
        data = self.data
        if data is None:
            return
        if isinstance(record, ZoneStatus) and record.index in data.zones:
            new_zone_status = dict(data.zone_status)
            new_zone_status[record.index] = record
            self.async_set_updated_data(replace(data, zone_status=new_zone_status))
        elif isinstance(record, UnitStatus) and record.index in data.units:
            new_unit_status = dict(data.unit_status)
            new_unit_status[record.index] = record
            self.async_set_updated_data(replace(data, unit_status=new_unit_status))
        elif isinstance(record, AuxSensorStatus):
            # raw=0 is the panel's documented "not available / unknown"
            # sentinel (see omni_temp_to_fahrenheit's docstring in the
            # library) and arrives interspersed between real readings —
            # confirmed against a live panel's outdoor temp probe, which
            # alternates a real value with this sentinel on roughly every
            # other push. Skip merging it entirely rather than overwrite
            # a good last-known reading with "no data this cycle" — the
            # entity should hold its last real value through the noise,
            # not flicker to Unknown every other update.
            if record.value_raw == 0:
                pass
            else:
                # Not gated on a discovered-objects dict the way Zone/Unit
                # are: Aux Sensors (e.g. an "Extended Range" analog zone
                # wired as an outdoor temp/humidity probe) aren't
                # separately walked during discovery today, so there's no
                # `data.aux_sensors` set to check membership against yet.
                # Accept any pushed index.
                new_aux_status = dict(data.aux_sensor_status)
                new_aux_status[record.index] = record
                self.async_set_updated_data(
                    replace(data, aux_sensor_status=new_aux_status)
                )
        # Other pushed types are logged above but not yet merged — OmniData
        # has no matching dict for them today.

    async def _run_event_listener(self) -> None:
        """Background loop: consume typed events and push state to entities.

        Re-establishes the iterator on each connection cycle. If the
        client gets dropped (transport error during a poll), we exit; the
        next ``_async_update_data`` will reconnect and respawn this task.
        """
        client = self._client
        if client is None:
            return
        try:
            async for event in client.events():
                self._apply_event(event)
        except asyncio.CancelledError:
            raise
        except (OmniConnectionError, RequestTimeoutError, OSError):
            LOGGER.debug("event listener exited on transport error", exc_info=True)
        except Exception:  # pragma: no cover - defensive
            LOGGER.exception("event listener crashed")

    def _apply_event(self, event: SystemEvent) -> None:
        """Patch ``self.data`` in place for the relevant event subclass."""
        LOGGER.info("push event received: %r", event)
        data = self.data
        if data is None:
            return
        new_data = self._patched_for_event(data, event)
        if new_data is not None:
            self.async_set_updated_data(new_data)

    def _patched_for_event(
        self, data: OmniData, event: SystemEvent
    ) -> OmniData | None:
        """Return a new OmniData reflecting ``event``, or ``None`` to skip.

        Pure-ish (mutates only the dict members of the returned snapshot).
        Split out so it stays unit-testable without HA.
        """
        if isinstance(event, ZoneStateChanged):
            existing = data.zone_status.get(event.zone_index)
            if existing is None:
                # We saw a zone the discovery missed — synthesize a record
                # so entities at least see the open/closed flip.
                new_status = ZoneStatus(
                    index=event.zone_index,
                    raw_status=0x01 if event.is_open else 0x00,
                    loop=0,
                )
            else:
                # Toggle low-2-bit current condition; preserve the rest.
                base = existing.raw_status & ~0x03
                new_raw = base | (0x01 if event.is_open else 0x00)
                new_status = ZoneStatus(
                    index=existing.index,
                    raw_status=new_raw,
                    loop=existing.loop,
                )
            patched = dict(data.zone_status)
            patched[event.zone_index] = new_status
            return replace(data, zone_status=patched, last_event=event)

        if isinstance(event, UnitStateChanged):
            existing = data.unit_status.get(event.unit_index)
            new_state = 1 if event.is_on else 0
            if existing is None:
                new_status = UnitStatus(
                    index=event.unit_index,
                    state=new_state,
                    time_remaining_secs=0,
                )
            else:
                # Preserve a brightness level if we have one — the event
                # only carries on/off.
                if existing.state >= 100 and event.is_on:
                    new_status = existing
                else:
                    new_status = UnitStatus(
                        index=existing.index,
                        state=new_state,
                        time_remaining_secs=existing.time_remaining_secs,
                    )
            patched = dict(data.unit_status)
            patched[event.unit_index] = new_status
            return replace(data, unit_status=patched, last_event=event)

        if isinstance(event, ArmingChanged):
            existing = data.area_status.get(event.area_index)
            if existing is None:
                if event.area_index == 0:
                    # System-wide arming change with no specific area —
                    # let the next poll resync.
                    return replace(data, last_event=event)
                new_status = AreaStatus(
                    index=event.area_index,
                    mode=event.new_mode,
                    last_user=event.user_index,
                    entry_timer_secs=0,
                    exit_timer_secs=0,
                    alarms=0,
                )
            else:
                new_status = AreaStatus(
                    index=existing.index,
                    mode=event.new_mode,
                    last_user=event.user_index,
                    entry_timer_secs=existing.entry_timer_secs,
                    exit_timer_secs=existing.exit_timer_secs,
                    alarms=existing.alarms,
                )
            patched = dict(data.area_status)
            patched[new_status.index] = new_status
            return replace(data, area_status=patched, last_event=event)

        if isinstance(event, AlarmActivated | AlarmCleared):
            # Force a poll so AreaStatus.alarms picks up the current bits.
            self.hass.async_create_task(self.async_request_refresh())
            return replace(data, last_event=event)

        if isinstance(event, AcLost | AcRestored | BatteryLow | BatteryRestored):
            # Just stash the event; the system_* binary sensors derive
            # their state from `last_event` alone.
            return replace(data, last_event=event)

        # Other event families are interesting but don't move any
        # currently-modeled state — record them for diagnostics so
        # subscribers can still react via the last_event attribute.
        return replace(data, last_event=event)

