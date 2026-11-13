"""Script de demostración en tiempo real (Fase 3 / Fase 5 Cloud).

Genera lecturas continuas con timestamp actual en UTC (now()), evalúa umbrales
analíticos de riesgo y persiste tanto las alertas en Azure Cosmos DB (Cassandra API)
como el estado y telemetría instantánea en Azure Database for PostgreSQL y el búfer local.

Esto permite que el Dashboard desplegado en Streamlit Community Cloud se actualice
en vivo desde cualquier dispositivo mientras este script corre en una PC local.

Uso:
    python scripts/run_live_demo.py
    python scripts/run_live_demo.py --interval 2.0 --machines 5 --anomaly-rate 0.20
    python scripts/run_live_demo.py --max-ticks 10
"""

import argparse
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time

# Asegurar que la raíz del proyecto esté en sys.path
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pandas as pd
from dotenv import load_dotenv

from src.generator.catalog import build_machine_catalog
from src.generator.live_generator import LiveTelemetryGenerator, current_utc_moment
from src.storage.load_results import (
    ensure_pg_machines,
    get_cassandra_session,
    init_cassandra_alerts_table,
    init_cassandra_telemetry_table,
    insert_cassandra_alert_live,
    insert_cassandra_telemetry_batch,
)

load_dotenv()

DATA_LIVE_DIR = Path("data/live")
LATEST_METRICS_PATH = DATA_LIVE_DIR / "latest_metrics.json"
LIVE_ALERTS_PATH = DATA_LIVE_DIR / "live_alerts.jsonl"
LIVE_TELEMETRY_PATH = DATA_LIVE_DIR / "telemetry_stream.jsonl"
LIVE_ARCHIVE_PATH = DATA_LIVE_DIR / "telemetry_archive.jsonl"
SPARK_SNAPSHOT_PATH = DATA_LIVE_DIR / "spark_microbatch.jsonl"
SPARK_LOG_PATH = DATA_LIVE_DIR / "spark_last.log"


def print_banner(
    machines: int,
    interval: float,
    anomaly_rate: float,
    cassandra_active: bool,
    postgres_active: bool,
) -> None:
    print("\n" + "=" * 76)
    print("   SISTEMA BIG DATA: GENERADOR Y PIPELINE EN TIEMPO REAL (CLOUD SYNC)")
    print("=" * 76)
    print(f"  * Intervalo entre emisiones: {interval} segundos")
    print(f"  * Máquinas monitoreadas:     {machines} (MAQ-001 .. MAQ-{machines:03d})")
    print(f"  * Tasa de anomalías:         {anomaly_rate * 100:.1f}%")
    if cassandra_active:
        host = os.getenv("CASSANDRA_HOST", "basededatos3.cassandra.cosmos.azure.com")
        ks = os.getenv("CASSANDRA_KEYSPACE", "bd3_mantenimiento")
        print(f"  * Nube de alertas (NoSQL):   Azure Cosmos DB Cassandra ({host} | {ks}.alerts)")
    else:
        print("  * Nube de alertas (NoSQL):   MODO LOCAL (sin conexión a Azure Cassandra)")

    if postgres_active:
        pg_host = os.getenv("PG_HOST") or os.getenv("POSTGRES_HOST", "azure.com")
        print(f"  * Nube de estado (SQL):      Azure PostgreSQL ({pg_host})")
    else:
        print("  * Nube de estado (SQL):      MODO LOCAL (sin conexión a Azure PostgreSQL)")

    print(f"  * Directorio búfer en vivo:  {DATA_LIVE_DIR.resolve()}")
    print("=" * 76)
    print("  [PRESIONA CTRL+C PARA DETENER LA TRANSMISIÓN EN CUALQUIER MOMENTO]\n")


def update_live_buffer(
    readings: list[dict],
    alerts: list[dict],
    latest_state: dict[str, dict],
) -> None:
    """Actualizar archivos de búfer local y el diccionario latest_state."""
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

    # 4. Archivo crudo acumulativo de la sesión para PySpark
    with LIVE_ARCHIVE_PATH.open("a", encoding="utf-8") as f:
        for line in new_lines:
            f.write(line + "\n")


def launch_spark_microbatch(no_azure: bool = False) -> subprocess.Popen | None:
    """Lanzar PySpark en segundo plano sobre una instantánea del archivo acumulado."""
    if not LIVE_ARCHIVE_PATH.exists():
        return None
    shutil.copyfile(LIVE_ARCHIVE_PATH, SPARK_SNAPSHOT_PATH)

    env = os.environ.copy()
    corretto_21 = Path(r"C:\Users\musimundo\.jdks\corretto-21.0.5")
    if corretto_21.is_dir():
        env["JAVA_HOME"] = str(corretto_21)
        env["PATH"] = f"{corretto_21 / 'bin'};{env.get('PATH', '')}"
    env["PYSPARK_PYTHON"] = sys.executable
    env["PYSPARK_DRIVER_PYTHON"] = sys.executable

    cmd = [
        sys.executable,
        "-m",
        "scripts.run_demo",
        "--input",
        str(SPARK_SNAPSHOT_PATH),
        "--allow-window-update",
    ]
    if not no_azure:
        cmd.append("--load")

    log_handle = SPARK_LOG_PATH.open("w", encoding="utf-8")
    proc = subprocess.Popen(
        cmd,
        cwd=str(PROJECT_ROOT),
        env=env,
        stdout=log_handle,
        stderr=subprocess.STDOUT,
    )
    proc._log_handle = log_handle  # type: ignore[attr-defined]
    return proc


def run_live_stream(
    interval: float = 2.0,
    machines: int = 5,
    anomaly_rate: float = 0.15,
    max_ticks: int | None = None,
    no_azure: bool = False,
    spark_every: int = 10,
) -> int:
    """Bucle continuo: escribe telemetría y alertas en Azure Cassandra y usa Azure Postgres para catálogo."""
    generator = LiveTelemetryGenerator(machines=machines, anomaly_rate=anomaly_rate)

    cluster = None
    session = None
    prepared_alerts = None
    prepared_telemetry = None
    cassandra_connected = False
    postgres_connected = False

    if not no_azure:
        if os.getenv("CASSANDRA_HOST") and os.getenv("CASSANDRA_PASSWORD"):
            print("[Iniciando] Conectando con Azure Cosmos DB (Cassandra API)...")
            try:
                cluster, session = get_cassandra_session()
                prepared_alerts = init_cassandra_alerts_table(session)
                prepared_telemetry = init_cassandra_telemetry_table(session)
                cassandra_connected = True
                print("[Conectado] Sesión con Azure Cosmos DB (alerts + telemetry_live) lista.")
            except Exception as exc:
                print(f"[Aviso] No se pudo conectar a Azure Cassandra: {exc}")

        if os.getenv("PG_PASSWORD") or os.getenv("POSTGRES_PASSWORD"):
            print("[Iniciando] Verificando catálogo maestro en Azure PostgreSQL...")
            try:
                catalog = build_machine_catalog(machines)
                ensure_pg_machines([(m.machine_id, m.name, m.sector) for m in catalog])
                postgres_connected = True
                print(f"[Conectado] Catálogo de {len(catalog)} máquinas verificado en Azure PostgreSQL.")
            except Exception as exc:
                print(f"[Aviso] No se pudo sincronizar catálogo en Azure PostgreSQL: {exc}")

    print_banner(machines, interval, anomaly_rate, cassandra_connected, postgres_connected)

    DATA_LIVE_DIR.mkdir(parents=True, exist_ok=True)
    latest_state: dict[str, dict] = {}
    total_emitted = 0
    total_alerts_detected = 0
    total_alerts_stored = 0
    total_telemetry_stored = 0
    spark_runs = 0
    spark_proc: subprocess.Popen | None = None
    ticks = 0

    try:
        while max_ticks is None or ticks < max_ticks:
            ticks += 1
            tick_now = current_utc_moment()
            now_iso = tick_now.strftime("%H:%M:%S")

            # Verificar si un proceso PySpark previo terminó
            if spark_proc is not None and spark_proc.poll() is not None:
                rc = spark_proc.returncode
                if hasattr(spark_proc, "_log_handle"):
                    spark_proc._log_handle.close()
                if rc == 0:
                    print(f"[{now_iso} UTC] [✅ PYSPARK COMPLETADO] Métricas 5m actualizadas en Azure Postgres + Parquet en Data Lake.")
                else:
                    print(f"[{now_iso} UTC] [⚠️ PYSPARK AVISO] Terminó con código {rc}. Ver {SPARK_LOG_PATH}")
                spark_proc = None

            readings = generator.generate_tick(tick_now)
            total_emitted += len(readings)

            # Evaluar alertas en este tick y preparar lote de telemetría para Cassandra
            tick_alerts = []
            alerts_by_machine: dict[str, str] = {}
            for r in readings:
                alert = generator.evaluate_reading(r)
                if alert:
                    tick_alerts.append(alert)
                    alerts_by_machine[r["machine_id"]] = alert["level"]

            total_alerts_detected += len(tick_alerts)

            # 1. Insertar TODAS las lecturas en Azure Cassandra (telemetry_live)
            cas_tel_tag = ""
            if cassandra_connected and session and prepared_telemetry:
                try:
                    telemetry_rows = [
                        (
                            r["machine_id"],
                            pd.to_datetime(r["timestamp"], utc=True).to_pydatetime(),
                            float(r["temperature"]),
                            float(r["vibration"]),
                            float(r["pressure"]),
                            int(r["rpm"]),
                            float(r["power_kw"]),
                            str(r["machine_state"]),
                            alerts_by_machine.get(r["machine_id"], "VERDE"),
                        )
                        for r in readings
                    ]
                    insert_cassandra_telemetry_batch(session, prepared_telemetry, telemetry_rows)
                    total_telemetry_stored += len(telemetry_rows)
                    cas_tel_tag = f" | [☁️ CASSANDRA +{len(telemetry_rows)} lecturas]"
                except Exception as err:
                    cas_tel_tag = f" | [ERROR TELEMETRÍA CASSANDRA: {err}]"

            # 2. Insertar alertas detectadas en Azure Cassandra (alerts)
            for alert in tick_alerts:
                level_tag = "[🚨 ALERTA ROJA]" if alert["level"] == "ROJO" else "[⚠️ ALERTA AMARILLA]"
                azure_tag = ""
                if cassandra_connected and session and prepared_alerts:
                    try:
                        ts_dt = pd.to_datetime(alert["timestamp"], utc=True).to_pydatetime()
                        insert_cassandra_alert_live(
                            session,
                            prepared_alerts,
                            alert["machine_id"],
                            ts_dt,
                            alert["level"],
                            alert["description"],
                        )
                        total_alerts_stored += 1
                        azure_tag = " -> [☁️ ALERTA GUARDADA EN CASSANDRA]"
                    except Exception as err:
                        azure_tag = f" -> [ERROR CASSANDRA: {err}]"

                print(
                    f"[{now_iso} UTC] {level_tag} {alert['machine_id']}: {alert['description']}{azure_tag}"
                )

            # Guardar respaldo en búfer local (incluye telemetry_archive.jsonl)
            update_live_buffer(readings, tick_alerts, latest_state)

            # Lanzar PySpark micro-batch cada N ciclos sin frenar el streaming
            if spark_every > 0 and ticks % spark_every == 0:
                if spark_proc is None:
                    spark_proc = launch_spark_microbatch(no_azure=no_azure)
                    if spark_proc is not None:
                        spark_runs += 1
                        print(
                            f"[{now_iso} UTC] [⚡ PYSPARK MICRO-BATCH #{spark_runs}] "
                            f"Procesando {total_emitted} lecturas acumuladas hacia Azure Postgres + Data Lake..."
                        )
                else:
                    print(f"[{now_iso} UTC] [⏳ PYSPARK] Aún ejecutando lote anterior; se omite este disparo.")

            # Resumen de telemetría de este paso
            summary_items = [
                f"{r['machine_id']}:{r['temperature']}°C/{r['vibration']}mm/s"
                for r in readings[:3]
            ]
            if len(readings) > 3:
                summary_items.append(f"...(+{len(readings) - 3} más)")
            telemetry_str = " | ".join(summary_items)
            print(f"[{now_iso} UTC] [📡 TICK #{ticks:03d}] {len(readings)} maq: {telemetry_str}{cas_tel_tag}")

            if max_ticks is not None and ticks >= max_ticks:
                break

            time.sleep(interval)

    except KeyboardInterrupt:
        print("\n[Detención] Señal de interrupción (Ctrl+C) recibida por el usuario.")

    finally:
        if spark_proc is not None:
            if spark_proc.poll() is None and max_ticks is not None:
                print("[Espera] Esperando que finalice el micro-batch de PySpark en curso...")
                try:
                    spark_proc.wait(timeout=120)
                except subprocess.TimeoutExpired:
                    spark_proc.terminate()
            if hasattr(spark_proc, "_log_handle"):
                spark_proc._log_handle.close()
            if spark_proc.poll() == 0:
                print("[✅ PYSPARK COMPLETADO] Métricas 5m actualizadas en Azure Postgres + Parquet en Data Lake.")
            elif spark_proc.poll() is not None:
                print(f"[⚠️ PYSPARK AVISO] Terminó con código {spark_proc.returncode}. Ver {SPARK_LOG_PATH}")

        if cluster:
            print("[Cierre] Cerrando conexión con Azure Cosmos DB...")
            cluster.shutdown()

        print("\n" + "=" * 60)
        print("           RESUMEN DE TRANSMISIÓN EN TIEMPO REAL")
        print("=" * 60)
        print(f"  * Ciclos ejecutados (ticks):     {ticks}")
        print(f"  * Lecturas de telemetría:        {total_emitted}")
        print(f"  * Telemetría en Cassandra:       {total_telemetry_stored}")
        print(f"  * Alertas detectadas:            {total_alerts_detected}")
        print(f"  * Alertas insertadas en Azure:   {total_alerts_stored}")
        print(f"  * Micro-batches PySpark:         {spark_runs}")
        print("=" * 60 + "\n")

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generador continuo en tiempo real con persistencia en Azure Cassandra y Azure PostgreSQL."
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
        help="Ejecutar únicamente en modo local sin conectar a Azure.",
    )
    parser.add_argument(
        "--spark-every",
        "--spark_every",
        type=int,
        default=10,
        help="Disparar micro-batch de PySpark cada N ciclos (0 para desactivar; por defecto: 10).",
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
        spark_every=args.spark_every,
    )


if __name__ == "__main__":
    sys.exit(main())