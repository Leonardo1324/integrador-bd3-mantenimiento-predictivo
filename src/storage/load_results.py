import os
import json
import math
from numbers import Integral
from pathlib import Path
import ssl
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pymongo import MongoClient, UpdateOne
from dotenv import load_dotenv

try:
    import asyncore  # Compatibilidad Python 3.12 para cassandra-driver
except ImportError:
    pass
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

load_dotenv()


def required_env(name: str) -> str:
    """Leer una variable obligatoria del entorno."""
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Falta configurar {name} en el entorno o en .env.")
    return value


def get_pg_connection():
    """Abrir una conexión a PostgreSQL (soporta Azure con SSL y entorno local)."""
    host = os.getenv("PG_HOST") or os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("PG_PORT") or os.getenv("POSTGRES_PORT", "5432")
    user = os.getenv("PG_USER") or os.getenv("POSTGRES_USER", "admin_bd3")
    password = os.getenv("PG_PASSWORD") or os.getenv("POSTGRES_PASSWORD")
    if not password:
        raise ValueError("Falta configurar PG_PASSWORD o POSTGRES_PASSWORD en el entorno o en .env.")
    dbname = os.getenv("PG_DB") or os.getenv("POSTGRES_DB", "mantenimiento_predictivo")

    default_ssl = "require" if "azure.com" in host else "prefer"
    sslmode = os.getenv("PG_SSLMODE") or os.getenv("POSTGRES_SSLMODE", default_ssl)

    return psycopg2.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        dbname=dbname,
        sslmode=sslmode,
        connect_timeout=15,
    )


def fetch_pg_machines() -> list[dict[str, str]]:
    """Consultar el catálogo maestro de máquinas desde PostgreSQL."""
    conn = get_pg_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT machine_id, machine_name, location FROM machines ORDER BY machine_id;"
            )
            return [
                {"machine_id": row[0], "machine_name": row[1], "location": row[2]}
                for row in cur.fetchall()
            ]
    finally:
        conn.close()


def ensure_pg_machines(machines: list[tuple[str, str, str]]) -> None:
    """Asegurar que las máquinas del catálogo existan en PostgreSQL."""
    if not machines:
        return
    conn = get_pg_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                execute_values(
                    cur,
                    """
                    INSERT INTO machines (machine_id, machine_name, location)
                    VALUES %s
                    ON CONFLICT (machine_id) DO NOTHING;
                    """,
                    machines,
                )
    finally:
        conn.close()


def get_mongo_client():
    """Crear un cliente autenticado de MongoDB con la configuración local."""
    return MongoClient(
        host=os.getenv("MONGO_HOST", "localhost"),
        port=int(os.getenv("MONGO_PORT", "27017")),
        username=os.getenv("MONGO_INITDB_ROOT_USERNAME", "admin_mongo"),
        password=required_env("MONGO_INITDB_ROOT_PASSWORD"),
        authSource="admin",
        serverSelectionTimeoutMS=5000,
    )


def get_cassandra_session():
    """Abrir una sesión autenticada con SSL hacia Azure Cosmos DB (Cassandra API)."""
    contact_points = [required_env("CASSANDRA_HOST")]
    port = int(os.getenv("CASSANDRA_PORT", "10350"))
    user = required_env("CASSANDRA_USER")
    password = required_env("CASSANDRA_PASSWORD")
    keyspace = os.getenv("CASSANDRA_KEYSPACE", "bd3_mantenimiento")

    ssl_context = ssl.create_default_context()
    ssl_context.check_hostname = False
    ssl_context.verify_mode = ssl.CERT_NONE

    cluster = Cluster(
        contact_points=contact_points,
        port=port,
        auth_provider=PlainTextAuthProvider(username=user, password=password),
        ssl_context=ssl_context,
    )
    session = cluster.connect(keyspace)
    return cluster, session


def upload_to_azure_blob(local_path: Path, blob_prefix: str = "processed"):
    """Subir un archivo o carpeta Parquet/CSV/JSONL a Azure Blob Storage."""
    conn_str = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    if not conn_str or "TU_CUENTA" in conn_str:
        return

    from azure.storage.blob import BlobServiceClient

    container_name = os.getenv("AZURE_STORAGE_CONTAINER", "datalake")
    blob_service_client = BlobServiceClient.from_connection_string(conn_str)
    container_client = blob_service_client.get_container_client(container_name)

    if not container_client.exists():
        container_client.create_container()

    files_to_upload = (
        [p for p in local_path.rglob("*") if p.is_file() and not p.name.startswith(".")]
        if local_path.is_dir()
        else [local_path]
    )

    uploaded = 0
    for file_item in files_to_upload:
        rel_name = (
            file_item.relative_to(local_path).as_posix()
            if local_path.is_dir()
            else file_item.name
        )
        blob_name = f"{blob_prefix}/{rel_name}"
        with open(file_item, "rb") as data:
            container_client.upload_blob(name=blob_name, data=data, overwrite=True)
            uploaded += 1

    print(f"[Azure Blob Storage] Subidos {uploaded} archivos desde '{local_path.name}' a '{container_name}/{blob_prefix}/'.")


def load_dataframe(file_path: Path) -> pd.DataFrame:
    """Lee tanto Parquet (salida de PySpark) como CSV (fixtures de prueba)."""
    if file_path.suffix == ".parquet" or file_path.is_dir():
        return pd.read_parquet(file_path)
    return pd.read_csv(file_path, dtype={"machine_id": str})


def utc_datetime(value):
    """Normalizar un timestamp de consumo a un datetime UTC para PostgreSQL."""
    timestamp = pd.to_datetime(value, utc=True)
    if pd.isna(timestamp):
        raise ValueError("Los timestamps de consumo no pueden estar vacíos.")
    return timestamp.to_pydatetime()


def upsert_postgres(metrics_path: Path, risk_path: Path, *, batch_id: str | None = None):
    """Cargar las salidas juntas y reservar sus ventanas si se identifica el lote."""
    df_metrics = load_dataframe(metrics_path)
    df_risk = load_dataframe(risk_path)

    # Extraer todas las maquinas unicas para asegurar que existan en el catalogo
    machine_ids = set(df_metrics["machine_id"]).union(df_risk["machine_id"])
    if any(not isinstance(machine_id, str) or not machine_id.strip() or len(machine_id) > 50
           for machine_id in machine_ids):
        raise ValueError("machine_id debe ser texto de 1 a 50 caracteres.")
    machines = sorted(machine_ids)

    metrics_rows = []
    for row in df_metrics.itertuples(index=False):
        measurements = (float(row.avg_temp), float(row.max_vibration), float(row.avg_pressure))
        if not all(math.isfinite(value) for value in measurements):
            raise ValueError("Las métricas agregadas deben ser números finitos.")
        if not isinstance(row.n_readings, Integral) or not 0 <= row.n_readings <= 2_147_483_647:
            raise ValueError("n_readings debe ser un entero no negativo compatible con PostgreSQL.")
        metrics_rows.append((row.machine_id, utc_datetime(row.window_start), *measurements,
                             int(row.n_readings), batch_id))
    metrics_rows.sort(key=lambda row: row[:2])
    risk_rows = []
    for row in df_risk.itertuples(index=False):
        if not isinstance(row.risk_reason, str) or not row.risk_reason.strip():
            raise ValueError("risk_reason debe contener el motivo de la clasificación.")
        risk_rows.append((row.machine_id, utc_datetime(row.calculated_at), str(row.risk_level).upper(),
                          row.risk_reason, utc_datetime(row.window_start) if batch_id is not None else None))
    risk_rows.sort(key=lambda row: row[0])

    conn = get_pg_connection()
    try:
        with conn:
            with conn.cursor() as cur:
                # 1. UPSERT catalogo de maquinas
                machine_rows = [(m_id, f"Equipo {m_id}", "Planta Principal") for m_id in machines]
                execute_values(
                    cur,
                    """
                    INSERT INTO machines (machine_id, machine_name, location)
                    VALUES %s
                    ON CONFLICT (machine_id) DO NOTHING;
                    """,
                    machine_rows,
                )

                accepted = execute_values(
                    cur,
                    """
                    INSERT INTO sensor_metrics_5m AS current
                        (machine_id, window_start, avg_temp, max_vibration, avg_pressure, n_readings, batch_id)
                    VALUES %s
                    ON CONFLICT (machine_id, window_start) DO UPDATE SET
                        avg_temp = EXCLUDED.avg_temp,
                        max_vibration = EXCLUDED.max_vibration,
                        avg_pressure = EXCLUDED.avg_pressure,
                        n_readings = EXCLUDED.n_readings
                    WHERE current.batch_id IS NOT DISTINCT FROM EXCLUDED.batch_id
                    RETURNING machine_id, window_start;
                    """,
                    metrics_rows,
                    fetch=True,
                )
                if len(accepted) != len(metrics_rows):
                    conflicts = {row[:2] for row in metrics_rows} - set(accepted)
                    machine, window = min(conflicts)
                    raise ValueError(f"Ventana de otro lote o sin propietario: {machine} / {window.isoformat()}.")

                execute_values(
                    cur,
                    """
                    INSERT INTO machine_risk_latest AS current
                        (machine_id, calculated_at, risk_level, risk_reason, window_start)
                    VALUES %s
                    ON CONFLICT (machine_id) DO UPDATE SET
                        calculated_at = EXCLUDED.calculated_at,
                        risk_level = EXCLUDED.risk_level,
                        risk_reason = EXCLUDED.risk_reason,
                        window_start = EXCLUDED.window_start
                    WHERE EXCLUDED.window_start > current.window_start
                        OR (EXCLUDED.window_start IS NULL AND current.window_start IS NULL);
                    """,
                    risk_rows,
                )
                if batch_id is not None and risk_rows:
                    cur.execute(
                        "SELECT machine_id FROM machine_risk_latest "
                        "WHERE window_start IS NULL AND machine_id = ANY(%s) LIMIT 1",
                        ([row[0] for row in risk_rows],),
                    )
                    legacy = cur.fetchone()
                    if legacy:
                        raise ValueError(f"El riesgo previo de {legacy[0]} no tiene ventana de origen verificable.")
        print(f"[Azure PostgreSQL] Carga exitosa: {len(metrics_rows)} metricas y {len(risk_rows)} estados de riesgo.")
    finally:
        conn.close()


def upsert_mongodb(alerts_path: Path):
    """Cargar alertas JSONL con una clave única por máquina e instante."""
    client = get_mongo_client()
    try:
        db = client[os.getenv("MONGO_DB", "mantenimiento_predictivo")]
        alerts_col = db["alerts"]
        # Indice unico para garantizar idempotencia en MongoDB
        alerts_col.create_index([("machine_id", 1), ("timestamp", 1)], unique=True)

        operations = []
        with open(alerts_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    doc = json.loads(line)
                    operations.append(
                        UpdateOne(
                            {"machine_id": doc["machine_id"], "timestamp": doc["timestamp"]},
                            {"$set": doc},
                            upsert=True,
                        )
                    )

        if operations:
            result = alerts_col.bulk_write(operations)
            print(
                f"[MongoDB] Alertas procesadas: {len(operations)} "
                f"(Nuevas: {result.upserted_count}, Actualizadas: {result.modified_count})."
            )
    finally:
        client.close()


def init_cassandra_alerts_table(session):
    """Asegurar la tabla de alertas y devolver la consulta preparada de inserción."""
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS alerts (
            machine_id text,
            timestamp timestamp,
            level text,
            description text,
            PRIMARY KEY ((machine_id), timestamp)
        ) WITH CLUSTERING ORDER BY (timestamp DESC);
        """
    )
    return session.prepare(
        """
        INSERT INTO alerts (machine_id, timestamp, level, description)
        VALUES (?, ?, ?, ?);
        """
    )


def insert_cassandra_alert_live(session, prepared_insert, machine_id: str, ts_datetime, level: str, description: str):
    """Insertar una alerta individual directamente en Azure Cosmos DB Cassandra."""
    session.execute(prepared_insert, (machine_id, ts_datetime, level, description))


def init_cassandra_telemetry_table(session):
    """Asegurar la tabla de telemetría rápida en vivo y devolver la consulta preparada."""
    session.execute(
        """
        CREATE TABLE IF NOT EXISTS telemetry_live (
            machine_id text,
            timestamp timestamp,
            temperature double,
            vibration double,
            pressure double,
            rpm int,
            power_kw double,
            machine_state text,
            risk_level text,
            PRIMARY KEY ((machine_id), timestamp)
        ) WITH CLUSTERING ORDER BY (timestamp DESC);
        """
    )
    return session.prepare(
        """
        INSERT INTO telemetry_live (
            machine_id, timestamp, temperature, vibration, pressure,
            rpm, power_kw, machine_state, risk_level
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?) USING TTL 604800;
        """
    )


def insert_cassandra_telemetry_batch(session, prepared_insert, rows: list[tuple]) -> None:
    """Insertar un lote de lecturas en vivo en paralelo sobre Azure Cosmos DB Cassandra."""
    if not rows:
        return
    execute_concurrent_with_args(session, prepared_insert, rows, concurrency=10)


def upsert_cassandra(alerts_path: Path):
    """Cargar alertas JSONL en Azure Cosmos DB (Cassandra API) de forma idempotente."""
    cluster, session = get_cassandra_session()
    try:
        prepared_insert = init_cassandra_alerts_table(session)
        loaded_count = 0
        with open(alerts_path, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    doc = json.loads(line)
                    ts = pd.to_datetime(doc["timestamp"], utc=True).to_pydatetime()
                    insert_cassandra_alert_live(
                        session,
                        prepared_insert,
                        doc["machine_id"],
                        ts,
                        doc.get("level", "AMARILLO"),
                        doc.get("description", ""),
                    )
                    loaded_count += 1

        print(f"[Azure Cassandra] Alertas insertadas en la nube: {loaded_count}")
    finally:
        cluster.shutdown()


if __name__ == "__main__":
    base_sample = Path("data/sample")
    upsert_postgres(
        base_sample / "sensor_metrics_5m_sample.csv",
        base_sample / "machine_risk_latest_sample.csv",
    )
    if os.getenv("CASSANDRA_HOST") and os.getenv("CASSANDRA_PASSWORD"):
        upsert_cassandra(base_sample / "alerts_sample.jsonl")
    if os.getenv("AZURE_STORAGE_CONNECTION_STRING"):
        upload_to_azure_blob(base_sample, blob_prefix="sample")