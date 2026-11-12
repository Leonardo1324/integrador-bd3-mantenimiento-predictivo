"""Pruebas unitarias para el generador de telemetría en tiempo real (Fase 3)."""

from datetime import datetime, timezone
import unittest

from src.generator.live_generator import LiveTelemetryGenerator, current_utc_moment, format_utc_timestamp
from src.processing.risk_rules import RiskThresholds, SensorThresholds


class TestLiveGenerator(unittest.TestCase):
    def setUp(self):
        self.generator = LiveTelemetryGenerator(machines=3, anomaly_rate=0.0, seed=42)

    def test_utc_helpers(self):
        now = current_utc_moment()
        self.assertIsNotNone(now.tzinfo)
        formatted = format_utc_timestamp(now)
        self.assertTrue(formatted.endswith("Z"))
        self.assertIn("T", formatted)

    def test_generator_init_validation(self):
        with self.assertRaises(ValueError):
            LiveTelemetryGenerator(machines=0)
        with self.assertRaises(ValueError):
            LiveTelemetryGenerator(machines=5, anomaly_rate=-0.1)
        with self.assertRaises(ValueError):
            LiveTelemetryGenerator(machines=5, anomaly_rate=1.5)

    def test_generate_tick_fields_and_types(self):
        readings = self.generator.generate_tick()
        self.assertEqual(len(readings), 3)

        expected_ids = {"MAQ-001", "MAQ-002", "MAQ-003"}
        self.assertEqual({r["machine_id"] for r in readings}, expected_ids)

        for r in readings:
            self.assertTrue(r["timestamp"].endswith("Z"))
            self.assertIsInstance(r["temperature"], float)
            self.assertIsInstance(r["vibration"], float)
            self.assertIsInstance(r["pressure"], float)
            self.assertIsInstance(r["rpm"], int)
            self.assertIsInstance(r["power_kw"], float)
            self.assertIsInstance(r["operating_hours"], float)
            self.assertIn(r["machine_state"], ("operating", "idle", "stopped"))
            self.assertIsInstance(r["is_anomaly"], bool)

    def test_evaluate_reading_green_gives_none(self):
        normal_reading = {
            "machine_id": "MAQ-001",
            "timestamp": "2026-11-12T03:00:00Z",
            "temperature": 65.0,
            "vibration": 2.0,
            "pressure": 5.0,
        }
        alert = self.generator.evaluate_reading(normal_reading)
        self.assertIsNone(alert)

    def test_evaluate_reading_yellow_and_red_thresholds(self):
        # Temperatura roja (>= 94.0)
        red_reading = {
            "machine_id": "MAQ-001",
            "timestamp": "2026-11-12T03:00:00Z",
            "temperature": 96.5,
            "vibration": 2.0,
            "pressure": 5.0,
        }
        alert = self.generator.evaluate_reading(red_reading)
        self.assertIsNotNone(alert)
        self.assertEqual(alert["level"], "ROJO")
        self.assertIn("Temperatura", alert["description"])
        self.assertIn("ROJO", alert["description"])

        # Vibración amarilla (>= 6.0, < 6.8)
        yellow_reading = {
            "machine_id": "MAQ-002",
            "timestamp": "2026-11-12T03:00:00Z",
            "temperature": 70.0,
            "vibration": 6.3,
            "pressure": 5.0,
        }
        alert_y = self.generator.evaluate_reading(yellow_reading)
        self.assertIsNotNone(alert_y)
        self.assertEqual(alert_y["level"], "AMARILLO")
        self.assertIn("Vibración", alert_y["description"])

    def test_trigger_manual_anomaly(self):
        self.assertTrue(self.generator.trigger_manual_anomaly("MAQ-002", "overheating_spike"))
        readings = self.generator.generate_tick()
        maq2 = next(r for r in readings if r["machine_id"] == "MAQ-002")
        self.assertTrue(maq2["is_anomaly"])
        self.assertEqual(maq2["anomaly_type"], "overheating_spike")
        self.assertGreaterEqual(maq2["temperature"], 94.0)

        # La evaluación debe detectarla como alerta ROJA
        alert = self.generator.evaluate_reading(maq2)
        self.assertIsNotNone(alert)
        self.assertEqual(alert["level"], "ROJO")


if __name__ == "__main__":
    unittest.main()
