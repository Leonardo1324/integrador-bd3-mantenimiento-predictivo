"""Dashboard interactivo en tiempo real e histórico Cloud para Monitoreo Predictivo Industrial 4.0 (Fase 5).

Integra toda la arquitectura en Azure:
1. Estado y telemetría por máquina (en vivo o desde Azure Database for PostgreSQL).
2. Curvas históricas de ventanas de 5 minutos procesadas por PySpark (Azure PostgreSQL).
3. Tabla de alertas en tiempo real consumida desde Azure Cosmos DB (Cassandra API).
4. Estado del Data Lake en Azure Blob Storage.

Ejecución:
    .venv\\Scripts\\streamlit run app_live_dashboard.py
"""

import json
import os
from pathlib import Path
import time
import altair as alt

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from src.storage.load_results import get_cassandra_session, get_pg_connection

load_dotenv()

DATA_LIVE_DIR = Path("data/live")
LATEST_METRICS_PATH = DATA_LIVE_DIR / "latest_metrics.json"
LIVE_ALERTS_PATH = DATA_LIVE_DIR / "live_alerts.jsonl"
LIVE_TELEMETRY_PATH = DATA_LIVE_DIR / "telemetry_stream.jsonl"

st.set_page_config(
    page_title="Monitoreo Predictivo Industrial 4.0 - Azure Cloud",
    page_icon="🏭",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom CSS para estética industrial moderna
st.markdown(
    """
    <style>
    .metric-card-green {
        background-color: #0e2a1b;
        border-left: 6px solid #10b981;
        padding: 16px;
        border-radius: 8px;
        margin-bottom: 12px;
    }
    .metric-card-yellow {
        background-color: #2d2605;
        border-left: 6px solid #f59e0b;
        padding: 16px;
        border-radius: 8px;
        margin-bottom: 12px;
    }
    .metric-card-red {
        background-color: #311111;
        border-left: 6px solid #ef4444;
        padding: 16px;
        border-radius: 8px;
        margin-bottom: 12px;
    }
    .status-pill-green {
        background-color: #10b981;
        color: white;
        padding: 3px 8px;
        border-radius: 12px;
        font-weight: bold;
        font-size: 0.85rem;
    }
    .status-pill-yellow {
        background-color: #f59e0b;
        color: black;
        padding: 3px 8px;
        border-radius: 12px;
        font-weight: bold;
        font-size: 0.85rem;
    }
    .status-pill-red {
        background-color: #ef4444;
        color: white;
        padding: 3px 8px;
        border-radius: 12px;
        font-weight: bold;
        font-size: 0.85rem;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_resource
def get_cached_cassandra_connection():
    """Mantener una conexión persistente a Azure Cosmos DB Cassandra sin reconectar en cada rerun."""
    host = os.getenv("CASSANDRA_HOST")
    password = os.getenv("CASSANDRA_PASSWORD")
    if not host or not password:
        return None, None
    try:
        cluster, session = get_cassandra_session()
        return cluster, session
    except Exception:
        return None, None

def plot_metric(df, value_col, y_label, yellow, red):
    data = df[["window_start", "machine_id", value_col]].dropna()

    lines = (
        alt.Chart(data)
        .mark_line(point=True)
        .encode(
            x=alt.X("window_start:T", title="Hora (ventanas de 5 min)"),
            y=alt.Y(f"{value_col}:Q", title=y_label, scale=alt.Scale(zero=False)),
            color=alt.Color("machine_id:N", title="Máquina"),
            tooltip=[
                alt.Tooltip("machine_id:N", title="Máquina"),
                alt.Tooltip("window_start:T", title="Hora"),
                alt.Tooltip(f"{value_col}:Q", title=y_label, format=".2f"),
            ],
        )
    )

    thresholds = pd.DataFrame(
        {"nivel": ["Advertencia", "Crítico"], "valor": [yellow, red]}
    )
    rules = (
        alt.Chart(thresholds)
        .mark_rule(strokeDash=[6, 4], strokeWidth=2)
        .encode(
            y="valor:Q",
            color=alt.Color(
                "nivel:N",
                title="Umbral",
                scale=alt.Scale(
                    domain=["Advertencia", "Crítico"],
                    range=["#f59e0b", "#ef4444"],
                ),
            ),
        )
    )

    st.altair_chart((lines + rules).interactive(), use_container_width=True)

def query_postgres_data() -> tuple[bool, pd.DataFrame, pd.DataFrame]:
    """Consultar estados de riesgo actuales y métricas de 5 minutos desde Azure PostgreSQL."""
    try:
        conn = get_pg_connection()
        with conn.cursor() as cur:
            # 1. Estado actual por máquina + última ventana de 5m
            cur.execute(
                """
                SELECT
                    r.machine_id,
                    COALESCE(m.machine_name, r.machine_id) AS machine_name,
                    COALESCE(m.location, 'Planta Principal') AS location,
                    r.risk_level,
                    r.risk_reason,
                    r.calculated_at,
                    r.window_start,
                    s.avg_temp,
                    s.max_vibration,
                    s.avg_pressure,
                    s.n_readings
                FROM machine_risk_latest r
                LEFT JOIN machines m ON m.machine_id = r.machine_id
                LEFT JOIN LATERAL (
                    SELECT avg_temp, max_vibration, avg_pressure, n_readings
                    FROM sensor_metrics_5m sm
                    WHERE sm.machine_id = r.machine_id
                    ORDER BY sm.window_start DESC
                    LIMIT 1
                ) s ON TRUE
                ORDER BY r.machine_id;
                """
            )
            risk_cols = [desc[0] for desc in cur.description]
            risk_df = pd.DataFrame(cur.fetchall(), columns=risk_cols)

            # 2. Serie histórica de ventanas de 5 minutos (PySpark)
            cur.execute(
                """
                SELECT
                    machine_id,
                    window_start,
                    avg_temp,
                    max_vibration,
                    avg_pressure,
                    n_readings
                FROM sensor_metrics_5m
                ORDER BY window_start ASC, machine_id ASC;
                """
            )
            metrics_cols = [desc[0] for desc in cur.description]
            metrics_df = pd.DataFrame(cur.fetchall(), columns=metrics_cols)
            if not metrics_df.empty:
                metrics_df["window_start"] = pd.to_datetime(metrics_df["window_start"])
                for col in ["avg_temp", "max_vibration", "avg_pressure"]:
                    metrics_df[col] = pd.to_numeric(metrics_df[col], errors="coerce")

        conn.close()
        return True, risk_df, metrics_df
    except Exception:
        return False, pd.DataFrame(), pd.DataFrame()


def load_live_metrics(pg_risk_df: pd.DataFrame) -> tuple[list[dict], str]:
    """Cargar métricas instantáneas del generador en vivo o usar el estado de Azure PostgreSQL."""
    if LATEST_METRICS_PATH.exists():
        try:
            content = LATEST_METRICS_PATH.read_text(encoding="utf-8")
            if content.strip():
                return json.loads(content), "Streaming Local en Vivo"
        except Exception:
            pass

    if not pg_risk_df.empty:
        cards = []
        for row in pg_risk_df.to_dict(orient="records"):
            cards.append(
                {
                    "machine_id": row["machine_id"],
                    "risk_level": row["risk_level"],
                    "temperature": float(row["avg_temp"] or 0.0),
                    "vibration": float(row["max_vibration"] or 0.0),
                    "pressure": float(row["avg_pressure"] or 0.0),
                    "machine_state": f"{row['location']} (Ventana 5m)",
                    "risk_reason": row["risk_reason"],
                    "timestamp": str(row["window_start"] or row["calculated_at"] or ""),
                }
            )
        return cards, "Azure Database for PostgreSQL"

    return [], "Sin datos"


def load_telemetry_history() -> pd.DataFrame:
    """Cargar la ventana deslizante de telemetría en vivo para gráficos."""
    if LIVE_TELEMETRY_PATH.exists():
        try:
            lines = LIVE_TELEMETRY_PATH.read_text(encoding="utf-8").splitlines()
            records = [json.loads(line) for line in lines if line.strip()]
            if records:
                df = pd.DataFrame(records)
                df["timestamp"] = pd.to_datetime(df["timestamp"])
                return df
        except Exception:
            pass
    return pd.DataFrame()


def query_cloud_alerts(session, limit: int = 20) -> pd.DataFrame:
    """Consultar las alertas más recientes directamente desde Azure Cosmos DB (Cassandra API)."""
    if session is not None:
        try:
            query = f"SELECT machine_id, timestamp, level, description FROM alerts LIMIT {limit};"
            rows = session.execute(query)
            data = [
                {
                    "Máquina": row.machine_id,
                    "Fecha y Hora (UTC)": row.timestamp.strftime("%Y-%m-%d %H:%M:%S") if row.timestamp else "",
                    "Nivel": row.level,
                    "Diagnóstico / Motivo": row.description,
                }
                for row in rows
            ]
            if data:
                df = pd.DataFrame(data)
                return df.sort_values(by="Fecha y Hora (UTC)", ascending=False)
        except Exception:
            pass

    if LIVE_ALERTS_PATH.exists():
        try:
            lines = LIVE_ALERTS_PATH.read_text(encoding="utf-8").splitlines()
            records = [json.loads(line) for line in lines if line.strip()]
            if records:
                records = records[-limit:]
                data = [
                    {
                        "Máquina": r.get("machine_id", ""),
                        "Fecha y Hora (UTC)": r.get("timestamp", "").replace("T", " ").replace("Z", ""),
                        "Nivel": r.get("level", ""),
                        "Diagnóstico / Motivo": r.get("description", ""),
                    }
                    for r in reversed(records)
                ]
                return pd.DataFrame(data)
        except Exception:
            pass

    return pd.DataFrame()


# ==============================================================================
# CONSULTA DE DATOS A LA NUBE
# ==============================================================================
pg_connected, pg_risk_df, pg_metrics_df = query_postgres_data()
cluster, session = get_cached_cassandra_connection()
latest_machines, cards_source = load_live_metrics(pg_risk_df)
telemetry_df = load_telemetry_history()
alerts_df = query_cloud_alerts(session, limit=25)
blob_configured = bool(
    os.getenv("AZURE_STORAGE_CONNECTION_STRING")
    and "TU_CUENTA" not in os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
)


# ==============================================================================
# SIDEBAR
# ==============================================================================
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/industrial-scales.png", width=64)
    st.title("Control de Planta")
    st.caption("Arquitectura Híbrida Multi-Base en Azure")
    st.markdown("---")

    # 1. Estado Azure PostgreSQL
    if pg_connected:
        st.success("🐘 **Azure PostgreSQL**: Conectado")
        st.caption(f"Host: `{os.getenv('PG_HOST') or os.getenv('POSTGRES_HOST')}`")
        st.caption(f"Ventanas 5m almacenadas: **{len(pg_metrics_df)}**")
    else:
        st.error("⚠️ **Azure PostgreSQL**: Desconectado")

    # 2. Estado Azure Cosmos DB Cassandra
    if session is not None:
        st.success("☁️ **Azure Cassandra**: Conectado")
        st.caption(f"Host: `{os.getenv('CASSANDRA_HOST')}`")
        st.caption(f"Keyspace: `{os.getenv('CASSANDRA_KEYSPACE', 'bd3_mantenimiento')}.alerts`")
    else:
        st.warning("⚠️ **Azure Cassandra**: Modo local / Desconectado")

    # 3. Estado Azure Blob Storage
    if blob_configured:
        st.success("📦 **Azure Blob Data Lake**: Configurado")
        st.caption(f"Contenedor: `{os.getenv('AZURE_STORAGE_CONTAINER', 'datalake')}`")
    else:
        st.info("📦 **Azure Blob Data Lake**: No configurado")

    st.markdown("---")
    st.subheader("⚙️ Configuración de Vista")
    auto_refresh = st.checkbox("🔄 Actualización automática", value=False)
    refresh_rate = st.slider("Intervalo de refresco (segundos)", min_value=2, max_value=15, value=5)

    if st.button("⚡ Forzar Actualización Manual", use_container_width=True):
        st.rerun()

    st.markdown("---")
    st.info(
        "💡 **Comandos del Proyecto:**\n"
        "- Lote PySpark hacia Azure:\n"
        "`python scripts/run_demo.py --load`\n"
        "- Simulador Streaming en vivo:\n"
        "`python scripts/run_live_demo.py`"
    )


# ==============================================================================
# CONTENIDO PRINCIPAL
# ==============================================================================
st.title("🏭 Monitoreo Predictivo Industrial 4.0 - Cloud Dashboard")
st.markdown(
    "Arquitectura unificada sobre **Azure Database for PostgreSQL** (históricos 5m y riesgo), "
    "**Azure Cosmos DB Cassandra** (alertas en tiempo real) y **Azure Blob Storage** (Data Lake Parquet)."
)

# ------------------------------------------------------------------------------
# 1. KPIs SUPERIORES
# ------------------------------------------------------------------------------
total_machines = len(latest_machines)
green_count = sum(1 for m in latest_machines if m.get("risk_level") == "VERDE")
yellow_count = sum(1 for m in latest_machines if m.get("risk_level") == "AMARILLO")
red_count = sum(1 for m in latest_machines if m.get("risk_level") == "ROJO")
total_alerts = len(alerts_df)

kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
with kpi1:
    st.metric("Total Máquinas", total_machines)
with kpi2:
    st.metric("🟢 Normales", green_count)
with kpi3:
    st.metric("🟡 Advertencia", yellow_count)
with kpi4:
    st.metric("🔴 Críticas", red_count)
with kpi5:
    st.metric("☁️ Alertas en Cassandra", total_alerts)

st.markdown("---")

# ------------------------------------------------------------------------------
# 2. TARJETAS DE ESTADO POR MÁQUINA
# ------------------------------------------------------------------------------
st.subheader(f"🖥️ Estado Operativo por Máquina (Fuente: {cards_source})")

if latest_machines:
    cols = st.columns(min(len(latest_machines), 5))
    for idx, m in enumerate(latest_machines):
        col = cols[idx % len(cols)]
        m_id = m.get("machine_id", f"MAQ-{idx+1:03d}")
        risk = m.get("risk_level", "VERDE")
        temp = m.get("temperature", 0.0)
        vib = m.get("vibration", 0.0)
        pres = m.get("pressure", 0.0)
        state = m.get("machine_state", "operating")
        reason = m.get("risk_reason", "Operación nominal")

        card_class = (
            "metric-card-red"
            if risk == "ROJO"
            else "metric-card-yellow"
            if risk == "AMARILLO"
            else "metric-card-green"
        )
        pill_class = (
            "status-pill-red"
            if risk == "ROJO"
            else "status-pill-yellow"
            if risk == "AMARILLO"
            else "status-pill-green"
        )

        with col:
            st.markdown(
                f"""
                <div class="{card_class}">
                    <div style="display: flex; justify-content: space-between; align-items: center;">
                        <h4 style="margin: 0;">{m_id}</h4>
                        <span class="{pill_class}">{risk}</span>
                    </div>
                    <p style="font-size: 0.8rem; color: #a1a1aa; margin: 4px 0 10px 0;">Origen: <b>{state}</b></p>
                    <div style="font-size: 0.95rem; line-height: 1.5;">
                        🌡️ <b>Temp:</b> {temp:.1f} °C<br/>
                        〰️ <b>Vib:</b> {vib:.2f} mm/s<br/>
                        💨 <b>Pres:</b> {pres:.2f} bar
                    </div>
                    <div style="font-size: 0.75rem; color: #cbd5e1; margin-top: 8px;">
                        <i>{reason}</i>
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
else:
    st.info("Ejecutá `python scripts/run_demo.py --load` o `python scripts/run_live_demo.py` para visualizar las máquinas.")

st.markdown("---")

# ------------------------------------------------------------------------------
# 3. MÉTRICAS HISTÓRICAS DE 5 MINUTOS (AZURE POSTGRESQL + PYSPARK)
# ------------------------------------------------------------------------------
st.subheader("🐘 Agregaciones Históricas en Ventanas de 5 Minutos (Azure PostgreSQL)")
st.caption("Datos procesados por PySpark y consultados en vivo desde las tablas `sensor_metrics_5m` y `machine_risk_latest`.")

if not pg_metrics_df.empty:
    pg_tab1, pg_tab2, pg_tab3, pg_tab4 = st.tabs([
        "🌡️ Temp. Promedio 5m (°C)",
        "〰️ Vibración Máx. 5m (mm/s)",
        "💨 Presión Promedio 5m (bar)",
        "📋 Catálogo y Estado Consolidado (SQL)",
    ])

    with pg_tab1:
        plot_metric(pg_metrics_df, "avg_temp", "Temperatura promedio (°C)", 89.5, 94.0)

    with pg_tab2:
        plot_metric(pg_metrics_df, "max_vibration", "Vibración máxima (mm/s)", 6.0, 6.8)

    with pg_tab3:
        plot_metric(pg_metrics_df, "avg_pressure", "Presión promedio (bar)", 8.0, 8.6)

    with pg_tab4:
        st.dataframe(pg_risk_df, use_container_width=True, hide_index=True)
else:
    st.warning("No se encontraron registros en `sensor_metrics_5m` de Azure PostgreSQL.")

# Si además está corriendo el simulador en vivo, mostrar sus curvas instantáneas
if not telemetry_df.empty:
    st.markdown("---")
    st.subheader("📈 Stream de Sensores Segundo a Segundo (Buffer en Vivo)")
    live_tab1, live_tab2, live_tab3 = st.tabs(["🌡️ Temperatura Vivo", "〰️ Vibración Vivo", "💨 Presión Vivo"])
    with live_tab1:
        st.line_chart(telemetry_df.pivot(index="timestamp", columns="machine_id", values="temperature"))
    with live_tab2:
        st.line_chart(telemetry_df.pivot(index="timestamp", columns="machine_id", values="vibration"))
    with live_tab3:
        st.line_chart(telemetry_df.pivot(index="timestamp", columns="machine_id", values="pressure"))

st.markdown("---")

# ------------------------------------------------------------------------------
# 4. HISTORIAL DE ALERTAS DIRECTO DE AZURE COSMOS DB CASSANDRA
# ------------------------------------------------------------------------------
st.subheader("☁️ Registro de Alertas en Tiempo Real (Azure Cosmos DB for Apache Cassandra)")
st.caption("Consultado directamente desde la tabla `alerts` en el keyspace `bd3_mantenimiento`.")

if not alerts_df.empty:
    def highlight_level(val):
        if val == "ROJO":
            return "background-color: #ef4444; color: white; font-weight: bold;"
        elif val == "AMARILLO":
            return "background-color: #f59e0b; color: black; font-weight: bold;"
        return ""

    styled_df = alerts_df.style.map(highlight_level, subset=["Nivel"])
    st.dataframe(styled_df, use_container_width=True, hide_index=True)
else:
    st.write("No se han registrado alertas aún.")

# ==============================================================================
# BUCLE DE AUTO-REFRESCO
# ==============================================================================
if auto_refresh:
    time.sleep(refresh_rate)
    st.rerun()