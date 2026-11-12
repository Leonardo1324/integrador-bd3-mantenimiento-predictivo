"""Script de demostración en tiempo real (Fase 3).

Genera lecturas continuas con timestamp actual en UTC (now()), evalúa umbrales
analíticos de riesgo y persiste las alertas anómalas en vivo directamente en
Azure Cosmos DB (Cassandra API) y en el búfer local para el frontend de monitoreo.

Uso:
    python scripts/run_live_demo.py
    python scripts/run_live_demo.py --interval 1.5 --machines 5 --anomaly-rate 0.20
    python scripts/run_live_demo.py --max-ticks 10
"""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sys
import time

# Asegurar que la raíz del proyecto esté en sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from dotenv import load_dotenv

from src.generator.live_generator import LiveTelemetryGenerator, current_utc_moment
from src.storage.load_results import (
    get_cassandra_session,
    init_cassandra_alerts_table,
    insert_cassandra_alert_live,
)

load_dotenv()

DATA_LIVE_DIR = Path("data/live")
LATEST_METRICS_PATH = DATA_LIVE_DIR / "latest_metrics.json"
LIVE_ALERTS_PATH = DATA_LIVE_DIR / "live_alerts.jsonl"
LIVE_TELEMETRY_PATH = DATA_LIVE_DIR / "telemetry_stream.jsonl"


def print_banner(machines: int, interval: float, anomaly_rate: float, azure_active: bool) -> None:
    print("\n" + "=" * 76)
    print("   SISTEMA BIG DATA: GENERADOR Y PIPELINE EN TIEMPO REAL (FASE 3)")
    print("=" * 76)
    print(f"  * Intervalo entre emisiones: {interval} segundos")
    print(f"  * Máquinas monitoreadas:     {machines} (MAQ-001 .. MAQ-{machines:03d})")
    print(f"  * Tasa de anomalías:         {anomaly_rate * 100:.1f}%")
    if azure_active:
        host = os.getenv("CASSANDRA_HOST", "basededatos3.cassandra.cosmos.azure.com")
        ks = os.getenv("CASSANDRA_KEYSPACE", "bd3_mantenimiento")
        print(f"  * Nube de alertas:           Azure Cosmos DB (Cassandra API)")
        print(f"  * Host / Keyspace:           {host} | {ks}.alerts")
    else:
        print("  * Nube de alertas:           MODO LOCAL (sin conexión a Azure)")
    print(f"  * Directorio búfer en vivo:  {DATA_LIVE_DIR.resolve()}")
    print("=" * 76)
    print("  [PRESIONA CTRL+C PARA DETENER LA TRANSMISIÓN EN CUALQUIER MOMENTO]\n")


def update_live_buffer(
    readings: list[dict],
    alerts: list[dict],
    latest_state: dict[str, dict],
) -> None:
    """Actualizar archivos de búfer para que el frontend pueda consumirlos en vivo."""
    DATA_LIVE_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Actualizar estado instantáneo por máquina
    for r in readings:
        m_id = r["machine_id"]
        latest_state[m_id] = {
            "machine_id": m_id,
            "timestamp": r["timestamp"],
            "temperature": r["temperature"],
            "vibration": r["vibration"],
            "pressure": r["pressure"],
            "rpm": r["rpm"],
            "power_kw": r["power_kw"],
            "operating_hours": r["operating_hours"],
            "machine_state": r["machine_state"],
            "is_anomaly": r["is_anomaly"],
            "anomaly_type": r["anomaly_type"],
            "risk_level": "VERDE",
            "risk_reason": "Operación nominal",
        }

    for a in alerts:
        m_id = a["machine_id"]
        if m_id in latest_state:
            latest_state[m_id]["risk_level"] = a["level"]
            latest_state[m_id]["risk_reason"] = a["description"]

    temp_metrics = DATA_LIVE_DIR / "latest_metrics.tmp"
    temp_metrics.write_text(json.dumps(list(latest_state.values()), indent=2), encoding="utf-8")
    temp_metrics.replace(LATEST_METRICS_PATH)

    # 2. Agregar alertas al archivo histórico local
    if alerts:
        with LIVE_ALERTS_PATH.open("a", encoding="utf-8") as f:
            for a in alerts:
                f.write(json.dumps(a) + "\n")

    # 3. Sliding window de telemetría (hasta 500 registros)
    existing_lines = []
    if LIVE_TELEMETRY_PATH.exists():
        existing_lines = LIVE_TELEMETRY_PATH.read_text(encoding="utf-8").splitlines()
    new_lines = [json.dumps(r) for r in readings]
    combined = (existing_lines + new_lines)[-500:]
    LIVE_TELEMETRY_PATH.write_text("\n".join(combined) + "\n", encoding="utf-8")


def run_live_stream(
    interval: float = 2.0,
    machines: int = 5,
    anomaly_rate: float = 0.15,
    max_ticks: int | None = None,
    no_azure: bool = False,
) -> int:
    """Bucle continuo de generación, detección de anomalías y persistencia en Cassandra."""
    generator = LiveTelemetryGenerator(machines=machines, anomaly_rate=anomaly_rate)

    cluster = None
    session = None
    prepared_insert = None
    azure_connected = False

    if not no_azure and os.getenv("CASSANDRA_HOST") and os.getenv("CASSANDRA_PASSWORD"):
        print("[Iniciando] Conectando con Azure Cosmos DB (Cassandra API)...")
        try:
            cluster, session = get_cassandra_session()
            prepared_insert = init_cassandra_alerts_table(session)
            azure_connected = True
            print("[Conectado] Sesión con Azure Cosmos DB establecida exitosamente.")
        except Exception as exc:
            print(f"[Aviso] No se pudo conectar a Azure Cassandra: {exc}")
            print("[Modo alternativo] Continuando en modo búfer local.")

    print_banner(machines, interval, anomaly_rate, azure_connected)

    DATA_LIVE_DIR.mkdir(parents=True, exist_ok=True)
    latest_state: dict[str, dict] = {}
    total_emitted = 0
    total_alerts_detected = 0
    total_alerts_stored = 0
    ticks = 0

    try:
        while max_ticks is None or ticks < max_ticks:
            ticks += 1
            tick_now = current_utc_moment()
            now_iso = tick_now.strftime("%H:%M:%S")

            readings = generator.generate_tick(tick_now)
            total_emitted += len(readings)

            # Evaluar alertas en este tick
            tick_alerts = []
            for r in readings:
                alert = generator.evaluate_reading(r)
                if alert:
                    tick_alerts.append(alert)

            total_alerts_detected += len(tick_alerts)

            # Insertar alertas en Azure Cosmos DB Cassandra
            for alert in tick_alerts:
                level_tag = "[🚨 ALERTA ROJA]" if alert["level"] == "ROJO" else "[⚠️ ALERTA AMARILLA]"
                azure_tag = ""
                if azure_connected and session and prepared_insert:
                    try:
                        ts_dt = pd.to_datetime(alert["timestamp"], utc=True).to_pydatetime()
                        insert_cassandra_alert_live(
                            session,
                            prepared_insert,
                            alert["machine_id"],
                            ts_dt,
                            alert["level"],
                            alert["description"],
                        )
                        total_alerts_stored += 1
                        azure_tag = " -> [☁️ AZURE CASSANDRA PERSISTIDA]"
                    except Exception as err:
                        azure_tag = f" -> [ERROR AZURE: {err}]"

                print(
                    f"[{now_iso} UTC] {level_tag} {alert['machine_id']}: {alert['description']}{azure_tag}"
                )

            # Resumen de telemetría de este paso
            summary_items = [
                f"{r['machine_id']}:{r['temperature']}°C/{r['vibration']}mm/s"
                for r in readings[:3]
            ]
            if len(readings) > 3:
                summary_items.append(f"...(+{len(readings) - 3} más)")
            telemetry_str = " | ".join(summary_items)
            print(f"[{now_iso} UTC] [📡 TELEMETRÍA #{ticks:03d}] {len(readings)} lecturas: {telemetry_str}")

            # Guardar en búfer local para frontend
            update_live_buffer(readings, tick_alerts, latest_state)

            if max_ticks is not None and ticks >= max_ticks:
                break

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\n[Detención] Señal de interrupción (Ctrl+C) recibida por el usuario.")

    finally:
        if cluster:
            print("[Cierre] Cerrando conexión con Azure Cosmos DB...")
            cluster.shutdown()

        print("\n" + "=" * 60)
        print("          RESUMEN DE TRANSMISIÓN EN TIEMPO REAL")
        print("=" * 60)
        print(f"  * Ciclos ejecutados (ticks):     {ticks}")
        print(f"  * Lecturas de telemetría:        {total_emitted}")
        print(f"  * Alertas detectadas:            {total_alerts_detected}")
        print(f"  * Alertas insertadas en Azure:   {total_alerts_stored}")
        print("=" * 60 + "\n")

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generador continuo en tiempo real con persistencia de alertas en Azure Cosmos DB (Cassandra API)."
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=2.0,
        help="Segundos entre ciclos de emisión (por defecto: 2.0).",
    )
    parser.add_argument(
        "--machines",
        type=int,
        default=5,
        help="Cantidad de máquinas a simular (por defecto: 5).",
    )
    parser.add_argument(
        "--anomaly-rate",
        "--anomaly_rate",
        type=float,
        default=0.15,
        help="Probabilidad de anomalía por ciclo (por defecto: 0.15).",
    )
    parser.add_argument(
        "--max-ticks",
        "--max_ticks",
        type=int,
        default=None,
        help="Cantidad máxima de ciclos a ejecutar (por defecto: infinito hasta Ctrl+C).",
    )
    parser.add_argument(
        "--no-azure",
        "--no_azure",
        action="store_true",
        help="Ejecutar únicamente en modo local sin conectar a Azure Cassandra.",
    )
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    return run_live_stream(
        interval=args.interval,
        machines=args.machines,
        anomaly_rate=args.anomaly_rate,
        max_ticks=args.max_ticks,
        no_azure=args.no_azure,
    )


if __name__ == "__main__":
    sys.exit(main())
