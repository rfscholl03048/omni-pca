"""Sensor platform — analog zones, thermostat readings, panel telemetry.

We deliberately re-expose thermostat current_temperature / humidity as
diagnostic sensors (in addition to the climate entity) so users can
plot history. The climate entity remains the canonical control surface.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from homeassistant.components.sensor import (
    RestoreSensor,
    SensorDeviceClass,
    SensorEntity,
    SensorStateClass,
)
from homeassistant.const import PERCENTAGE, UnitOfTemperature
from homeassistant.helpers.entity import EntityCategory
from homeassistant.helpers.update_coordinator import CoordinatorEntity

from .const import DOMAIN
from .coordinator import OmniDataUpdateCoordinator
from .helpers import (
    SENSOR_DEVICE_CLASS_HUMIDITY,
    SENSOR_DEVICE_CLASS_TEMPERATURE,
    analog_zone_device_class,
    is_binary_zone_type,
    prettify_name,
)

if TYPE_CHECKING:
    from homeassistant.config_entries import ConfigEntry
    from homeassistant.core import HomeAssistant
    from homeassistant.helpers.entity_platform import AddEntitiesCallback


_DEVICE_CLASS_STR_TO_ENUM: dict[str, SensorDeviceClass] = {
    SENSOR_DEVICE_CLASS_TEMPERATURE: SensorDeviceClass.TEMPERATURE,
    SENSOR_DEVICE_CLASS_HUMIDITY: SensorDeviceClass.HUMIDITY,
    "power": SensorDeviceClass.POWER,
}


async def async_setup_entry(
    hass: HomeAssistant,
    entry: ConfigEntry,
    async_add_entities: AddEntitiesCallback,
) -> None:
    coordinator: OmniDataUpdateCoordinator = hass.data[DOMAIN][entry.entry_id]
    entities: list[SensorEntity] = []

    # Analog zones (temperature / humidity / energy)
    for index in sorted(coordinator.data.zones):
        props = coordinator.data.zones[index]
        if is_binary_zone_type(props.zone_type):
            continue
        device_class_str = analog_zone_device_class(props.zone_type)
        if device_class_str is None:
            continue
        entities.append(
            OmniAnalogZoneSensor(coordinator, index, device_class_str)
        )

    # Per-thermostat diagnostic sensors
    for index in sorted(coordinator.data.thermostats):
        entities.append(OmniThermostatTempSensor(coordinator, index))
        entities.append(OmniThermostatHumiditySensor(coordinator, index))
        entities.append(OmniThermostatOutdoorTempSensor(coordinator, index))

    entities.append(OmniSystemModelSensor(coordinator))
    entities.append(OmniLastEventSensor(coordinator))
    entities.append(OmniProgramsSensor(coordinator))

    async_add_entities(entities)


# --------------------------------------------------------------------------
# Analog zones
# --------------------------------------------------------------------------


class OmniAnalogZoneSensor(
    CoordinatorEntity[OmniDataUpdateCoordinator], RestoreSensor
):
    _attr_has_entity_name = True
    _attr_state_class = SensorStateClass.MEASUREMENT

    def __init__(
        self,
        coordinator: OmniDataUpdateCoordinator,
        index: int,
        device_class_str: str,
    ) -> None:
        super().__init__(coordinator)
        self._index = index
        self._attr_unique_id = f"{coordinator.unique_id}-zone-{index}-analog"
        props = coordinator.data.zones[index]
        self._attr_name = prettify_name(props.name) or f"Zone {index}"
        self._attr_device_info = coordinator.device_info
        self._attr_device_class = _DEVICE_CLASS_STR_TO_ENUM.get(device_class_str)
        if device_class_str == SENSOR_DEVICE_CLASS_TEMPERATURE:
            self._attr_native_unit_of_measurement = UnitOfTemperature.FAHRENHEIT
        elif device_class_str == SENSOR_DEVICE_CLASS_HUMIDITY:
            self._attr_native_unit_of_measurement = PERCENTAGE
        # Last real (non-sentinel) reading, held across both "no data this
        # cycle" pushes and full HA restarts. Populated from restored state
        # in async_added_to_hass, then kept current in native_value below.
        self._last_value: float | int | None = None

    async def async_added_to_hass(self) -> None:
        """Restore the last real reading so a restart doesn't show -40/Unknown.

        Runs once, before the coordinator has necessarily received any
        push for this index yet. Without this, every HA restart re-opens
        the same window that let ``zone_status.loop == 0`` render as a
        nonsense -40 (see native_value below) — the entity would otherwise
        sit on that until the panel's next real push happens to arrive.
        """
        await super().async_added_to_hass()
        last_data = await self.async_get_last_sensor_data()
        if last_data is not None and last_data.native_value is not None:
            self._last_value = last_data.native_value

    @property
    def native_value(self) -> float | int | None:
        # "Extended Range" analog zones (e.g. an outdoor temp/humidity
        # probe wired as zone_type 85) report their live readings through
        # the Aux Sensor status channel (ObjectType=8), not ZoneStatus,
        # even though the object itself is defined/named as a Zone in
        # PC Access. Confirmed against a live panel: PC Access shows this
        # as a Zone, but every push/poll of its value arrives as an
        # AuxSensorStatus(index=<same zone index>, ...) record. Check
        # aux_sensor_status first; fall back to zone_status for analog
        # zone types that genuinely do report via ZoneStatus.loop.
        aux = self.coordinator.data.aux_sensor_status.get(self._index)
        if aux is not None:
            # raw=0 is the panel's documented "not available / unknown"
            # sentinel (see omni_temp_to_fahrenheit's docstring) — it
            # legitimately arrives interspersed between real readings.
            # Converting it verbatim renders a nonsense -40°, flickering
            # against otherwise-plausible values; hold the last real
            # reading instead of dropping to Unknown for that cycle.
            if aux.value_raw == 0:
                return self._last_value
            if self._attr_device_class == SensorDeviceClass.TEMPERATURE:
                self._last_value = round(aux.temperature_f, 1)
            else:
                self._last_value = aux.value_raw
            return self._last_value
        status = self.coordinator.data.zone_status.get(self._index)
        if status is None:
            return self._last_value
        # Same sentinel applies here: a zone that has never had a real
        # AuxSensorStatus push yet (e.g. right after a restart, before the
        # panel's next update) still has a zone_status record from the
        # initial poll with loop == 0 — that is *also* the "no data"
        # sentinel, not a real reading of -40°F. Only humidity's 0% is a
        # value the loop byte can legitimately report as zero, so gate the
        # sentinel check to temperature.
        if (
            self._attr_device_class == SensorDeviceClass.TEMPERATURE
            and status.loop == 0
        ):
            return self._last_value
        # Reuse the linear temp formula for temperature zones; humidity
        # zones report the loop byte as the percentage directly.
        if self._attr_device_class == SensorDeviceClass.TEMPERATURE:
            self._last_value = round(status.loop * 9 / 10) - 40
        else:
            self._last_value = status.loop
        return self._last_value


# --------------------------------------------------------------------------
# Thermostat diagnostic sensors
# --------------------------------------------------------------------------


class _ThermostatBase(CoordinatorEntity[OmniDataUpdateCoordinator], SensorEntity):
    _attr_has_entity_name = True
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_state_class = SensorStateClass.MEASUREMENT

    def __init__(
        self, coordinator: OmniDataUpdateCoordinator, index: int
    ) -> None:
        super().__init__(coordinator)
        self._index = index
        self._attr_device_info = coordinator.device_info

    @property
    def _status(self):  # type: ignore[no-untyped-def]
        return self.coordinator.data.thermostat_status.get(self._index)


class OmniThermostatTempSensor(_ThermostatBase):
    _attr_device_class = SensorDeviceClass.TEMPERATURE
    _attr_native_unit_of_measurement = UnitOfTemperature.FAHRENHEIT

    def __init__(
        self, coordinator: OmniDataUpdateCoordinator, index: int
    ) -> None:
        super().__init__(coordinator, index)
        self._attr_unique_id = f"{coordinator.unique_id}-thermostat-{index}-temp"
        props = coordinator.data.thermostats[index]
        base = prettify_name(props.name) or f"Thermostat {index}"
        self._attr_name = f"{base} Temperature"

    @property
    def native_value(self) -> float | None:
        s = self._status
        if s is None or s.temperature_raw == 0:
            return None
        return round(s.temperature_f, 1)


class OmniThermostatHumiditySensor(_ThermostatBase):
    _attr_device_class = SensorDeviceClass.HUMIDITY
    _attr_native_unit_of_measurement = PERCENTAGE

    def __init__(
        self, coordinator: OmniDataUpdateCoordinator, index: int
    ) -> None:
        super().__init__(coordinator, index)
        self._attr_unique_id = f"{coordinator.unique_id}-thermostat-{index}-humidity"
        props = coordinator.data.thermostats[index]
        base = prettify_name(props.name) or f"Thermostat {index}"
        self._attr_name = f"{base} Humidity"

    @property
    def native_value(self) -> int | None:
        s = self._status
        if s is None or s.humidity_raw == 0:
            return None
        return int(s.humidity_raw)


class OmniThermostatOutdoorTempSensor(_ThermostatBase):
    _attr_device_class = SensorDeviceClass.TEMPERATURE
    _attr_native_unit_of_measurement = UnitOfTemperature.FAHRENHEIT

    def __init__(
        self, coordinator: OmniDataUpdateCoordinator, index: int
    ) -> None:
        super().__init__(coordinator, index)
        self._attr_unique_id = f"{coordinator.unique_id}-thermostat-{index}-outdoor"
        props = coordinator.data.thermostats[index]
        base = prettify_name(props.name) or f"Thermostat {index}"
        self._attr_name = f"{base} Outdoor Temperature"

    @property
    def native_value(self) -> float | None:
        s = self._status
        if s is None or s.outdoor_temperature_raw == 0:
            return None
        return round(s.outdoor_temperature_f, 1)


# --------------------------------------------------------------------------
# Panel telemetry
# --------------------------------------------------------------------------


class OmniSystemModelSensor(
    CoordinatorEntity[OmniDataUpdateCoordinator], SensorEntity
):
    """Static text sensor: model + firmware. Helps confirm the integration
    talked to the panel without needing to dig into Devices & Services."""

    _attr_has_entity_name = True
    _attr_entity_category = EntityCategory.DIAGNOSTIC

    def __init__(self, coordinator: OmniDataUpdateCoordinator) -> None:
        super().__init__(coordinator)
        self._attr_unique_id = f"{coordinator.unique_id}-system-model"
        self._attr_name = "Panel Model"
        self._attr_device_info = coordinator.device_info

    @property
    def native_value(self) -> str | None:
        info = self.coordinator.data.system_info
        if info is None:
            return None
        return f"{info.model_name} {info.firmware_version}"


class OmniLastEventSensor(
    CoordinatorEntity[OmniDataUpdateCoordinator], SensorEntity
):
    """Diagnostic text sensor showing the most recent push event class name."""

    _attr_has_entity_name = True
    _attr_entity_category = EntityCategory.DIAGNOSTIC

    def __init__(self, coordinator: OmniDataUpdateCoordinator) -> None:
        super().__init__(coordinator)
        self._attr_unique_id = f"{coordinator.unique_id}-last-event"
        self._attr_name = "Last Panel Event"
        self._attr_device_info = coordinator.device_info

    @property
    def native_value(self) -> str | None:
        ev = self.coordinator.data.last_event
        if ev is None:
            return None
        return type(ev).__name__

    @property
    def extra_state_attributes(self) -> dict[str, Any] | None:
        ev = self.coordinator.data.last_event
        if ev is None:
            return None
        result: dict[str, Any] = {"event_class": type(ev).__name__}
        for key in (
            "zone_index", "unit_index", "area_index", "user_index",
            "new_state", "new_mode", "alarm_type",
        ):
            if hasattr(ev, key):
                result[key] = getattr(ev, key)
        return result


class OmniProgramsSensor(
    CoordinatorEntity[OmniDataUpdateCoordinator], SensorEntity
):
    """Diagnostic sensor exposing the panel's automation programs.

    State value is the count of defined programs. The ``programs``
    attribute carries a list of per-program summaries — a stable,
    JSON-serializable view automations and template sensors can read.
    """

    _attr_has_entity_name = True
    _attr_entity_category = EntityCategory.DIAGNOSTIC
    _attr_state_class = SensorStateClass.MEASUREMENT

    def __init__(self, coordinator: OmniDataUpdateCoordinator) -> None:
        super().__init__(coordinator)
        self._attr_unique_id = f"{coordinator.unique_id}-programs"
        self._attr_name = "Panel Programs"
        self._attr_device_info = coordinator.device_info

    @property
    def native_value(self) -> int:
        return len(self.coordinator.data.programs)

    @property
    def extra_state_attributes(self) -> dict[str, Any]:
        from omni_pca.programs import ProgramType

        summaries: list[dict[str, Any]] = []
        for slot in sorted(self.coordinator.data.programs):
            p = self.coordinator.data.programs[slot]
            try:
                type_name = ProgramType(p.prog_type).name
            except ValueError:
                type_name = f"UNKNOWN({p.prog_type})"
            summaries.append(
                {
                    "slot": slot,
                    "type": type_name,
                    "cmd": p.cmd,
                    "par": p.par,
                    "pr2": p.pr2,
                    "month": p.month,
                    "day": p.day,
                    "days": p.days,
                    "hour": p.hour,
                    "minute": p.minute,
                }
            )
        return {"programs": summaries}
