"""Dashboard interactivo en tiempo real para Monitoreo Predictivo Industrial 4.0 (Fase 4).

Permite visualizar en vivo durante la defensa del proyecto:
1. Estado y telemetría instantánea por máquina (Tarjetas Verde / Amarillo / Rojo).
2. Gráficos de tendencias en tiempo real de temperatura, vibración y presión.
3. Tabla de alertas consumida directamente desde Azure Cosmos DB (Cassandra API).
4. Auto-refresco continuo sincronizado con el generador en vivo.

Ejecución:
    streamlit run app_live_dashboard.py
"""

from datetime import datetime, timezone
import json
import os
from pathlib import Path
import time

import pandas as pd
import streamlit as st
from dotenv import load_dotenv

from src.storage.load_results import get_cassandra_session

load_dotenv()

DATA_LIVE_DIR = Path("data/live")
LATEST_METRICS_PATH = DATA_LIVE_DIR / "latest_metrics.json"
LIVE_ALERTS_PATH = DATA_LIVE_DIR / "live_alerts.jsonl"
LIVE_TELEMETRY_PATH = DATA_LIVE_DIR / "telemetry_stream.jsonl"

st.set_page_config(
    page_title="Monitoreo Predictivo Industrial 4.0",
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
    except Exception as exc:
        return None, None


def load_live_metrics() -> list[dict]:
    """Cargar las métricas instantáneas más recientes del búfer."""
    if LATEST_METRICS_PATH.exists():
        try:
            content = LATEST_METRICS_PATH.read_text(encoding="utf-8")
            if content.strip():
                return json.loads(content)
        except Exception:
            pass
    return []


def load_telemetry_history() -> pd.DataFrame:
    """Cargar la ventana deslizante de telemetría para gráficos."""
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


def query_cloud_alerts(session, limit: int = 15) -> pd.DataFrame:
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
                return pd.DataFrame(data)
        except Exception:
            pass

    # Fallback a búfer local si Azure no responde
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
# SIDEBAR
# ==============================================================================
with st.sidebar:
    st.image("https://img.icons8.com/fluency/96/industrial-scales.png", width=64)
    st.title("Control de Planta")
    st.caption("Arquitectura Big Data & Cassandra en Azure")
    st.markdown("---")

    # Estado de la conexión a Azure
    cluster, session = get_cached_cassandra_connection()
    if session is not None:
        st.success("☁️ **Azure Cosmos DB**: Conectado")
        st.caption(f"Host: `{os.getenv('CASSANDRA_HOST', 'cosmos.azure.com')}`")
        st.caption(f"Keyspace: `{os.getenv('CASSANDRA_KEYSPACE', 'bd3_mantenimiento')}.alerts`")
    else:
        st.warning("⚠️ **Azure Cosmos DB**: Modo local / Desconectado")

    st.markdown("---")
    st.subheader("⚙️ Configuración de Vista")
    auto_refresh = st.checkbox("🔄 Actualización automática", value=True)
    refresh_rate = st.slider("Intervalo de refresco (segundos)", min_value=1, max_value=10, value=2)

    if st.button("⚡ Forzar Actualización Manual", use_container_width=True):
        st.rerun()

    st.markdown("---")
    st.info(
        "💡 **Instrucciones para la Demo:**\n"
        "1. Dejá corriendo en una terminal:\n"
        "`python scripts/run_live_demo.py`\n"
        "2. Este panel se actualizará automáticamente mostrando cómo llegan los datos y se disparan las alertas en Azure."
    )


# ==============================================================================
# CONTENIDO PRINCIPAL
# ==============================================================================
st.title("🏭 Monitoreo Predictivo Industrial 4.0 - Live Dashboard")
st.markdown(
    "Monitoreo continuo de telemetría, detección analítica de anomalías y persistencia en **Azure Cosmos DB (Cassandra API)**."
)

latest_machines = load_live_metrics()
telemetry_df = load_telemetry_history()
alerts_df = query_cloud_alerts(session, limit=20)

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
    st.metric("Total Máquinas", total_machines if total_machines > 0 else "5")
with kpi2:
    st.metric("🟢 Normales", green_count)
with kpi3:
    st.metric("🟡 Advertencia", yellow_count)
with kpi4:
    st.metric("🔴 Críticas", red_count)
with kpi5:
    st.metric("☁️ Alertas en Azure", total_alerts)

st.markdown("---")

# ------------------------------------------------------------------------------
# 2. TARJETAS DE ESTADO POR MÁQUINA EN TIEMPO REAL
# ------------------------------------------------------------------------------
st.subheader("🖥️ Estado Operativo y Sensores por Máquina")

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
        ts = m.get("timestamp", "").replace("T", " ").replace("Z", "")

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
                    <p style="font-size: 0.8rem; color: #a1a1aa; margin: 4px 0 10px 0;">Estado: <b>{state.upper()}</b></p>
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
    st.info("Esperando datos del generador en vivo... Ejecutá `python scripts/run_live_demo.py` para comenzar a recibir telemetría.")

st.markdown("---")

# ------------------------------------------------------------------------------
# 3. GRÁFICOS DE TENDENCIAS EN TIEMPO REAL
# ------------------------------------------------------------------------------
st.subheader("📈 Tendencias de Sensores en Vivo")

if not telemetry_df.empty:
    tab1, tab2, tab3 = st.tabs(["🌡️ Temperatura (°C)", "〰️ Vibración (mm/s)", "💨 Presión (bar)"])

    # Pivotear para graficar series por máquina
    with tab1:
        st.caption(r"Umbrales de Alerta: Amarillo $\ge 89.5^\circ\text{C}$ | Rojo $\ge 94.0^\circ\text{C}$")
        pivot_temp = telemetry_df.pivot(index="timestamp", columns="machine_id", values="temperature")
        st.line_chart(pivot_temp)

    with tab2:
        st.caption(r"Umbrales de Alerta: Amarillo $\ge 6.0\text{ mm/s}$ | Rojo $\ge 6.8\text{ mm/s}$")
        pivot_vib = telemetry_df.pivot(index="timestamp", columns="machine_id", values="vibration")
        st.line_chart(pivot_vib)

    with tab3:
        st.caption(r"Umbrales de Alerta: Amarillo $\ge 8.0\text{ bar}$ | Rojo $\ge 8.6\text{ bar}$")
        pivot_pres = telemetry_df.pivot(index="timestamp", columns="machine_id", values="pressure")
        st.line_chart(pivot_pres)
else:
    st.info("Aún no hay historial suficiente para graficar tendencias en vivo.")

st.markdown("---")

# ------------------------------------------------------------------------------
# 4. HISTORIAL DE ALERTAS DIRECTO DE AZURE COSMOS DB CASSANDRA
# ------------------------------------------------------------------------------
st.subheader("☁️ Registro de Alertas en Azure Cosmos DB (Cassandra API)")
st.caption("Consultado en tiempo real desde la tabla `alerts` en el keyspace `bd3_mantenimiento`.")

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
