"""Generador de telemetría industrial en tiempo real con inyección de anomalías y evaluación de umbrales.

Permite emitir eventos continuos con timestamp UTC actual (now()) y evaluar
en tiempo real si se activan umbrales de alerta (AMARILLO o ROJO) para su persistencia
inmediata en Azure Cosmos DB (Cassandra API) y consumo en el frontend de monitoreo.
"""

from dataclasses import dataclass, field
from datetime import datetime, timezone
import math
import random
from typing import Any

from src.generator.catalog import MachineProfile, build_machine_catalog
from src.processing.risk_rules import RiskThresholds, SensorThresholds


def current_utc_moment() -> datetime:
    """Devolver el instante actual consciente en UTC."""
    return datetime.now(timezone.utc)


def format_utc_timestamp(moment: datetime) -> str:
    """Formatear un datetime en UTC con sufijo Z según Contrato A."""
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


@dataclass
class MachineLiveState:
    """Estado operativo en memoria de una máquina durante la transmisión en vivo."""

    profile: MachineProfile
    step_idx: int = 0
    operating_hours: float = 0.0
    active_anomaly: str | None = None
    anomaly_progression: int = 0
    current_state: str = "operating"

    def __post_init__(self) -> None:
        if self.operating_hours == 0.0:
            self.operating_hours = self.profile.initial_operating_hours


class LiveTelemetryGenerator:
    """Simulador continuo de telemetría con física suave y eventos anómalos estocásticos."""

    def __init__(
        self,
        machines: int = 5,
        anomaly_rate: float = 0.15,
        seed: int | None = None,
        thresholds: RiskThresholds | None = None,
    ) -> None:
        if machines <= 0:
            raise ValueError("La cantidad de máquinas debe ser mayor a 0.")
        if not 0.0 <= anomaly_rate <= 1.0:
            raise ValueError("anomaly_rate debe estar entre 0.0 y 1.0.")

        self.machines_count = machines
        self.anomaly_rate = anomaly_rate
        self.rng = random.Random(seed)
        self.thresholds = thresholds or RiskThresholds()

        catalog = build_machine_catalog(machines)
        self.states: dict[str, MachineLiveState] = {
            m.machine_id: MachineLiveState(profile=m) for m in catalog
        }
        self.total_ticks = 0

    def trigger_manual_anomaly(self, machine_id: str, anomaly_type: str = "overheating_spike") -> bool:
        """Forzar manualmente una anomalía para una máquina específica (útil para la demo)."""
        if machine_id in self.states:
            state = self.states[machine_id]
            state.active_anomaly = anomaly_type
            state.anomaly_progression = 0
            return True
        return False

    def generate_tick(self, timestamp: datetime | None = None) -> list[dict[str, Any]]:
        """Generar una lectura por máquina para el instante dado (por defecto now())."""
        now = timestamp or current_utc_moment()
        now_str = format_utc_timestamp(now)
        self.total_ticks += 1

        readings: list[dict[str, Any]] = []

        for machine_id, state in self.states.items():
            state.step_idx += 1
            profile = state.profile

            # Evolución del estado operativo nominal
            if state.active_anomaly is not None:
                state.current_state = "operating"
            else:
                roll = self.rng.random()
                if roll < 0.90:
                    state.current_state = "operating"
                elif roll < 0.97:
                    state.current_state = "idle"
                else:
                    state.current_state = "stopped"

            # Ciclo diario armónico
            daily_phase = (2.0 * math.pi * ((state.step_idx * 5) % 1440)) / 1440.0
            harmonic = math.sin(daily_phase)

            # Generar valores físicos base
            if state.current_state == "stopped":
                temp = round(25.0 + self.rng.uniform(-1.0, 1.5), 2)
                vib = round(self.rng.uniform(0.02, 0.12), 2)
                pres = round(1.0 + self.rng.uniform(-0.05, 0.05), 2)
                rpm = 0
                power = round(self.rng.uniform(0.1, 0.4), 2)
            elif state.current_state == "idle":
                temp = round(profile.base_temperature_c * 0.78 + self.rng.gauss(0.0, 0.6), 2)
                vib = round(max(0.2, profile.base_vibration_mm_s * 0.45 + self.rng.gauss(0.0, 0.08)), 2)
                pres = round(profile.base_pressure_bar * 0.70 + self.rng.gauss(0.0, 0.10), 2)
                rpm = int(round(profile.base_rpm * 0.35 + self.rng.uniform(-20, 20)))
                power = round(profile.base_power_kw * 0.30 + self.rng.uniform(-0.5, 0.5), 2)
            else:  # operating
                temp = round(profile.base_temperature_c + 2.0 * harmonic + self.rng.gauss(0.0, 0.9), 2)
                vib = round(max(0.5, profile.base_vibration_mm_s + 0.15 * harmonic + self.rng.gauss(0.0, 0.12)), 2)
                pres = round(max(2.0, profile.base_pressure_bar + 0.12 * harmonic + self.rng.gauss(0.0, 0.10)), 2)
                rpm = int(round(profile.base_rpm + 20 * harmonic + self.rng.uniform(-25, 25)))
                power = round(profile.base_power_kw + 1.2 * harmonic + self.rng.gauss(0.0, 0.6), 2)

            # Verificar si se dispara o continúa una anomalía
            is_anomaly = False
            anomaly_type = None

            if state.active_anomaly is None and self.rng.random() < self.anomaly_rate:
                chosen = self.rng.choice(
                    ["overheating_spike", "vibration_spike", "pressure_anomaly", "sustained_degradation"]
                )
                state.active_anomaly = chosen
                state.anomaly_progression = 0

            if state.active_anomaly is not None:
                is_anomaly = True
                anomaly_type = state.active_anomaly

                if anomaly_type == "overheating_spike":
                    temp = round(self.rng.uniform(94.5, 110.0), 2)
                    power = round(power * self.rng.uniform(1.2, 1.35), 2)
                    # Las anomalías puntuales duran 1 paso
                    state.active_anomaly = None
                elif anomaly_type == "vibration_spike":
                    vib = round(self.rng.uniform(6.9, 11.0), 2)
                    rpm = int(round(rpm * self.rng.uniform(0.9, 1.1)))
                    state.active_anomaly = None
                elif anomaly_type == "pressure_anomaly":
                    if self.rng.random() < 0.5:
                        pres = round(self.rng.uniform(1.2, 2.0), 2)
                    else:
                        pres = round(self.rng.uniform(8.7, 10.5), 2)
                    state.active_anomaly = None
                elif anomaly_type == "sustained_degradation":
                    prog = state.anomaly_progression
                    temp = round(89.5 + prog * 2.5 + self.rng.uniform(0.5, 2.0), 2)
                    vib = round(5.9 + prog * 0.7 + self.rng.uniform(0.1, 0.5), 2)
                    pres = round(7.9 + prog * 0.35 + self.rng.uniform(0.1, 0.3), 2)
                    state.anomaly_progression += 1
                    if state.anomaly_progression >= 4:
                        state.active_anomaly = None

            # Actualizar horas operativas suavemente
            state.operating_hours = round(state.operating_hours + 0.01, 2)

            reading = {
                "machine_id": machine_id,
                "timestamp": now_str,
                "temperature": temp,
                "vibration": vib,
                "pressure": pres,
                "temperature_c": temp,
                "vibration_mm_s": vib,
                "pressure_bar": pres,
                "rpm": rpm,
                "power_kw": power,
                "operating_hours": state.operating_hours,
                "machine_state": state.current_state,
                "is_anomaly": is_anomaly,
                "anomaly_type": anomaly_type,
            }
            readings.append(reading)

        return readings

    def evaluate_reading(self, reading: dict[str, Any]) -> dict[str, Any] | None:
        """Evaluar si la lectura supera umbrales de riesgo y generar una alerta formateada."""
        temp = float(reading["temperature"])
        vib = float(reading["vibration"])
        pres = float(reading["pressure"])

        reasons: list[str] = []
        is_red = False
        is_yellow = False

        # Temperatura
        if temp >= self.thresholds.temperature.red:
            reasons.append(f"Temperatura: {temp:.2f} °C; ROJO (>= {self.thresholds.temperature.red} °C)")
            is_red = True
        elif temp >= self.thresholds.temperature.yellow:
            reasons.append(f"Temperatura: {temp:.2f} °C; AMARILLO (>= {self.thresholds.temperature.yellow} °C)")
            is_yellow = True

        # Vibración
        if vib >= self.thresholds.vibration.red:
            reasons.append(f"Vibración: {vib:.2f} mm/s; ROJO (>= {self.thresholds.vibration.red} mm/s)")
            is_red = True
        elif vib >= self.thresholds.vibration.yellow:
            reasons.append(f"Vibración: {vib:.2f} mm/s; AMARILLO (>= {self.thresholds.vibration.yellow} mm/s)")
            is_yellow = True

        # Presión alta
        if pres >= self.thresholds.pressure.red:
            reasons.append(f"Presión alta: {pres:.2f} bar; ROJO (>= {self.thresholds.pressure.red} bar)")
            is_red = True
        elif pres >= self.thresholds.pressure.yellow:
            reasons.append(f"Presión alta: {pres:.2f} bar; AMARILLO (>= {self.thresholds.pressure.yellow} bar)")
            is_yellow = True

        if not (is_red or is_yellow):
            return None

        level = "ROJO" if is_red else "AMARILLO"
        return {
            "machine_id": reading["machine_id"],
            "timestamp": reading["timestamp"],
            "level": level,
            "description": "; ".join(reasons),
            "temperature": temp,
            "vibration": vib,
            "pressure": pres,
        }
