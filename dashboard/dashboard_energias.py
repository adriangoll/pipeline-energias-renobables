"""
Dashboard Streamlit - Análisis de Potencial Energético Renovable
Proyecto: Pipeline ETLT Energías Renovables
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import boto3
from io import BytesIO

# ============================================================
# CONFIGURACIÓN
# ============================================================

st.set_page_config(
    page_title="Dashboard Energías Renovables",
    page_icon="🌞",
    layout="wide"
)

# ============================================================
# LECTOR S3 COMPATIBLE CON PARTITIONS SPARK
# ============================================================

@st.cache_data(ttl=3600)
def load_data_from_s3(bucket, prefix):

    s3 = boto3.client("s3")

    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    if "Contents" not in response:
        return pd.DataFrame()

    dfs = []

    for obj in response["Contents"]:

        key = obj["Key"]

        if not key.endswith(".parquet"):
            continue

        obj_data = s3.get_object(Bucket=bucket, Key=key)

        df = pd.read_parquet(BytesIO(obj_data["Body"].read()))

        # leer particiones del path
        parts = key.split("/")

        for p in parts:

            if "=" in p:

                col, val = p.split("=", 1)

                df[col] = val

        dfs.append(df)

    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)

    df.columns = df.columns.str.strip()

    for c in ["year", "month", "day", "hour"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df


# ============================================================
# DATOS FICTICIOS — esquemas alineados con el ETL Silver→Gold
# ============================================================
#
# Esquemas Gold (según etl_silver_to_gold.py):
#
# potencial_solar      → partitionBy(city, year, month)
#   cols: day, hour, potencial_solar_promedio, potencial_solar_maximo,
#         uv_index_promedio, nubosidad_promedio, temperatura_promedio
#
# potencial_eolico     → partitionBy(city, year, month)
#   cols: day, potencial_eolico_promedio_w, potencial_eolico_maximo_w,
#         velocidad_viento_promedio_ms, velocidad_viento_maxima_ms,
#         direccion_viento_promedio, mediciones
#
# condiciones_criticas → partitionBy(city, year)
#   cols: month, reduccion_solar, reduccion_eolica, condicion_adversa,
#         ocurrencias, temperatura_promedio, humedad_promedio,
#         nubosidad_promedio, viento_promedio
#
# analisis_comparativo → partitionBy(year, month)
#   cols: city, potencial_solar_promedio, uv_index_maximo, nubosidad_promedio,
#         potencial_eolico_promedio, velocidad_viento_promedio,   ← sin "_ms" (nombre real en S3)
#         velocidad_viento_maxima, temperatura_promedio, humedad_promedio,
#         presion_promedio, total_mediciones
#
# ranking_dias         → partitionBy(city, year)
#   cols: date, month, day, score_total, score_solar, score_eolico,
#         uv_promedio, viento_promedio, nubosidad_promedio,
#         temperatura_promedio, tipo_dia, rank_mejor, rank_peor
#
# ============================================================

def generate_sample_data():

    import numpy as np
    from datetime import datetime, timedelta

    np.random.seed(42)

    start   = datetime(2024, 1, 1)
    cities  = ["Riohacha", "Patagonia"]

    # ----------------------------------------------------------
    # 1. potencial_solar  (granularidad: city/year/month/day/hour)
    # ----------------------------------------------------------
    solar_rows = []

    for i in range(365):

        date = start + timedelta(days=i)

        for h in range(24):

            for city in cities:

                uv     = np.random.uniform(2, 10)
                clouds = np.random.uniform(0, 90)
                temp   = np.random.uniform(5, 35)

                factor = max(0.0, 1.0 - abs(h - 12) / 12.0) if 6 <= h <= 18 else 0.0
                solar  = round(uv * (1 - clouds / 100) * factor, 2)

                solar_rows.append({
                    "city"                    : city,
                    "year"                    : date.year,
                    "month"                   : date.month,
                    "day"                     : date.day,
                    "hour"                    : h,
                    "potencial_solar_promedio": solar,
                    "potencial_solar_maximo"  : round(solar * 1.1, 2),
                    "uv_index_promedio"       : round(uv, 2),
                    "nubosidad_promedio"      : round(clouds, 2),
                    "temperatura_promedio"    : round(temp, 2),
                })

    df_solar = pd.DataFrame(solar_rows)

    # ----------------------------------------------------------
    # 2. potencial_eolico  (granularidad: city/year/month/day)
    # ----------------------------------------------------------
    eolico_rows = []

    for i in range(365):

        date = start + timedelta(days=i)

        for city in cities:

            wind     = np.random.uniform(2, 20)
            temp     = np.random.uniform(5, 35)
            pressure = np.random.uniform(990, 1020)

            temp_k      = temp + 273.15
            densidad    = (pressure * 100) / (287.05 * temp_k)
            pot_w       = round(0.5 * densidad * wind ** 3, 2)

            eolico_rows.append({
                "city"                       : city,
                "year"                       : date.year,
                "month"                      : date.month,
                "day"                        : date.day,
                "potencial_eolico_promedio_w": pot_w,
                "potencial_eolico_maximo_w"  : round(pot_w * 1.2, 2),
                "velocidad_viento_promedio_ms": round(wind, 2),
                "velocidad_viento_maxima_ms" : round(wind * 1.3, 2),
                "direccion_viento_promedio"  : round(np.random.uniform(0, 360), 1),
                "mediciones"                 : 24,
            })

    df_eolico = pd.DataFrame(eolico_rows)

    # ----------------------------------------------------------
    # 3. condiciones_criticas  (granularidad: city/year/month + categorías)
    # ----------------------------------------------------------
    criticas_rows = []

    for month_num in range(1, 13):

        for city in cities:

            for reduccion_solar in ["Alta nubosidad", "UV bajo"]:

                for reduccion_eolica in ["Viento insuficiente", "Viento excesivo (peligro)"]:

                    criticas_rows.append({
                        "city"               : city,
                        "year"               : 2024,
                        "month"              : month_num,
                        "reduccion_solar"    : reduccion_solar,
                        "reduccion_eolica"   : reduccion_eolica,
                        "condicion_adversa"  : np.random.choice(
                            ["Lluvia probable", "Calor extremo", "Congelamiento"]
                        ),
                        "ocurrencias"        : int(np.random.randint(1, 50)),
                        "temperatura_promedio": round(np.random.uniform(5, 35), 2),
                        "humedad_promedio"   : round(np.random.uniform(40, 95), 2),
                        "nubosidad_promedio" : round(np.random.uniform(50, 100), 2),
                        "viento_promedio"    : round(np.random.uniform(0.5, 28), 2),
                    })

    df_condiciones = pd.DataFrame(criticas_rows)

    # ----------------------------------------------------------
    # 4. analisis_comparativo  (granularidad: city/year/month)
    # ----------------------------------------------------------
    comp_rows = []

    for month_num in range(1, 13):

        for city in cities:

            sol_m  = df_solar[(df_solar.city == city) & (df_solar.month == month_num)]
            eol_m  = df_eolico[(df_eolico.city == city) & (df_eolico.month == month_num)]

            comp_rows.append({
                "city"                    : city,
                "year"                    : 2024,
                "month"                   : month_num,
                "potencial_solar_promedio": round(sol_m["potencial_solar_promedio"].mean(), 4),
                "uv_index_maximo"         : round(sol_m["uv_index_promedio"].max(), 2),
                "nubosidad_promedio"      : round(sol_m["nubosidad_promedio"].mean(), 2),
                "potencial_eolico_promedio": round(eol_m["potencial_eolico_promedio_w"].mean(), 2),
                "velocidad_viento_promedio": round(eol_m["velocidad_viento_promedio_ms"].mean(), 2),
                "velocidad_viento_maxima"  : round(eol_m["velocidad_viento_maxima_ms"].max(), 2),
                "temperatura_promedio"     : round(sol_m["temperatura_promedio"].mean(), 2),
                "humedad_promedio"         : round(np.random.uniform(40, 80), 2),
                "presion_promedio"         : round(np.random.uniform(990, 1020), 2),
                "total_mediciones"         : len(sol_m),
            })

    df_comparativo = pd.DataFrame(comp_rows)

    # ----------------------------------------------------------
    # 5. ranking_dias  (top 10 mejores + top 10 peores por ciudad)
    # ----------------------------------------------------------
    dias_rows = []

    for i in range(365):

        date = start + timedelta(days=i)

        for city in cities:

            uv     = np.random.uniform(2, 10)
            wind   = np.random.uniform(2, 20)
            clouds = np.random.uniform(0, 90)
            temp   = np.random.uniform(5, 35)

            score_solar  = round(uv * (1 - clouds / 100), 4)
            score_eolico = round(wind ** 2 / 10, 4)
            score_total  = round(score_solar + score_eolico, 2)

            dias_rows.append({
                "city"                : city,
                "date"                : date.strftime("%Y-%m-%d"),
                "year"                : date.year,
                "month"               : date.month,
                "day"                 : date.day,
                "score_total"         : score_total,
                "score_solar"         : score_solar,
                "score_eolico"        : score_eolico,
                "uv_promedio"         : round(uv, 2),
                "viento_promedio"     : round(wind, 2),
                "nubosidad_promedio"  : round(clouds, 2),
                "temperatura_promedio": round(temp, 2),
            })

    df_dias = pd.DataFrame(dias_rows)

    # Aplicar lógica de ranking idéntica al ETL
    ranking_parts = []

    for city in cities:

        df_c = df_dias[df_dias.city == city].copy()
        df_c = df_c.sort_values("score_total", ascending=False).reset_index(drop=True)
        df_c["rank_mejor"] = df_c.index + 1

        df_c = df_c.sort_values("score_total", ascending=True).reset_index(drop=True)
        df_c["rank_peor"] = df_c.index + 1

        df_c = df_c[(df_c["rank_mejor"] <= 10) | (df_c["rank_peor"] <= 10)]

        df_c["tipo_dia"] = df_c.apply(
            lambda r: "Top 10 Mejores" if r["rank_mejor"] <= 10 else "Top 10 Peores",
            axis=1
        )

        ranking_parts.append(df_c)

    df_ranking = pd.concat(ranking_parts, ignore_index=True)

    return df_solar, df_eolico, df_condiciones, df_comparativo, df_ranking


# ============================================================
# CARGA DE DATOS
# ============================================================

st.title("🌞 Dashboard Energías Renovables")

st.sidebar.header("Configuración")

use_real_data = st.sidebar.checkbox("Usar datos reales S3", False)

if use_real_data:

    bucket = "datalake-energias-renovables-dev"

    with st.spinner("Cargando datos..."):

        df_solar       = load_data_from_s3(bucket, "gold/potencial_solar/")
        df_eolico      = load_data_from_s3(bucket, "gold/potencial_eolico/")
        df_condiciones = load_data_from_s3(bucket, "gold/condiciones_criticas/")
        df_ranking     = load_data_from_s3(bucket, "gold/ranking_dias/")
        df_comparativo = load_data_from_s3(bucket, "gold/analisis_comparativo/")

else:

    st.info("Usando datos simulados")

    df_solar, df_eolico, df_condiciones, df_comparativo, df_ranking = generate_sample_data()


# ============================================================
# MÉTRICAS PRINCIPALES
# ============================================================

st.header("Métricas Principales")

c1, c2, c3, c4 = st.columns(4)

with c1:
    # ranking_dias contiene columna "date" (string "yyyy-MM-dd") con top/peor 10 por ciudad
    total_dias = df_ranking["date"].nunique()
    st.metric("Días en Ranking", total_dias)

with c2:
    # potencial_solar → columna "potencial_solar_promedio"
    solar = df_solar["potencial_solar_promedio"].mean()
    st.metric("Solar Promedio (kWh/m²)", round(solar, 4))

with c3:
    # potencial_eolico → columna real en S3: "velocidad_viento_promedio_ms"
    wind = df_eolico["velocidad_viento_promedio_ms"].mean()
    st.metric("Viento Promedio (m/s)", round(wind, 2))

with c4:
    st.metric("Ubicaciones", df_solar["city"].nunique())


# ============================================================
# PREGUNTA 1 — Variación del Potencial Solar a lo largo del día
# Tabla: potencial_solar
# Cols relevantes: city, month, hour, potencial_solar_promedio
# ============================================================

st.header("Pregunta 1 — Variación Potencial Solar por Hora")

city_solar = st.selectbox("Ciudad (solar)", df_solar["city"].unique(), key="sel_city_solar")

month_solar = st.selectbox("Mes (solar)", sorted(df_solar["month"].unique()), key="sel_month_solar")

df1 = df_solar[
    (df_solar["city"]  == city_solar) &
    (df_solar["month"] == month_solar)
]

df_hour = (
    df1
    .groupby("hour")["potencial_solar_promedio"]
    .mean()
    .reset_index()
)

fig1 = px.line(
    df_hour,
    x="hour",
    y="potencial_solar_promedio",
    title=f"Potencial Solar por Hora — {city_solar} / Mes {month_solar}",
    labels={"hour": "Hora del día", "potencial_solar_promedio": "Potencial Solar (kWh/m²)"}
)

st.plotly_chart(fig1, use_container_width=True)


# ============================================================
# PREGUNTA 2 — Patrones Históricos del Potencial Eólico
# Tabla: potencial_eolico
# Cols relevantes: city, month, velocidad_viento_promedio_ms
# ============================================================

st.header("Pregunta 2 — Patrones Históricos del Potencial Eólico")

df2_line = (
    df_eolico
    .groupby(["city", "month"])["velocidad_viento_promedio_ms"]
    .mean()
    .reset_index()
)

fig2_line = px.line(
    df2_line,
    x="month",
    y="velocidad_viento_promedio_ms",
    color="city",
    markers=True,
    title="Velocidad de Viento Promedio por Mes",
    labels={"month": "Mes", "velocidad_viento_promedio_ms": "Velocidad Viento (m/s)"}
)

st.plotly_chart(fig2_line, use_container_width=True)

fig2_box = px.box(
    df_eolico,
    x="city",
    y="velocidad_viento_promedio_ms",
    color="city",
    title="Distribución de Velocidad de Viento por Ciudad",
    labels={"city": "Ciudad", "velocidad_viento_promedio_ms": "Velocidad Viento (m/s)"}
)

st.plotly_chart(fig2_box, use_container_width=True)


# ============================================================
# PREGUNTA 3 — Condiciones Climáticas que Reducen el Potencial
# Tabla: condiciones_criticas
# Cols relevantes: city, month, reduccion_solar, reduccion_eolica,
#                  condicion_adversa, ocurrencias, nubosidad_promedio,
#                  viento_promedio
# ============================================================

st.header("Pregunta 3 — Condiciones Climáticas Críticas")

# Impacto de nubosidad sobre el potencial solar
# (usando potencial_solar para el scatter, tabla más rica en esos valores)
fig3_scatter = px.scatter(
    df_solar.sample(min(5000, len(df_solar)), random_state=1),
    x="nubosidad_promedio",
    y="potencial_solar_promedio",
    color="city",
    opacity=0.5,
    title="Impacto de la Nubosidad en el Potencial Solar",
    labels={
        "nubosidad_promedio"      : "Nubosidad (%)",
        "potencial_solar_promedio": "Potencial Solar (kWh/m²)"
    }
)

st.plotly_chart(fig3_scatter, use_container_width=True)

# Ocurrencias de condiciones adversas por ciudad y mes
fig3_bar = px.bar(
    df_condiciones,
    x="month",
    y="ocurrencias",
    color="reduccion_solar",
    facet_col="city",
    barmode="stack",
    title="Ocurrencias de Reducción Solar por Mes",
    labels={"month": "Mes", "ocurrencias": "Ocurrencias", "reduccion_solar": "Tipo reducción"}
)

st.plotly_chart(fig3_bar, use_container_width=True)


# ============================================================
# PREGUNTA 4 — Comparación entre Ciudades
# Tabla: analisis_comparativo
# Cols relevantes: city, month, potencial_solar_promedio,
#                  velocidad_viento_promedio, potencial_eolico_promedio,
#                  temperatura_promedio
# ============================================================

st.header("Pregunta 4 — Comparación entre Ciudades")

fig4_solar = px.bar(
    df_comparativo,
    x="month",
    y="potencial_solar_promedio",
    color="city",
    barmode="group",
    title="Potencial Solar Mensual por Ciudad",
    labels={"month": "Mes", "potencial_solar_promedio": "Potencial Solar (kWh/m²)"}
)

st.plotly_chart(fig4_solar, use_container_width=True)

fig4_eolico = px.line(
    df_comparativo,
    x="month",
    y="velocidad_viento_promedio",
    color="city",
    markers=True,
    title="Velocidad de Viento Mensual por Ciudad",
    labels={"month": "Mes", "velocidad_viento_promedio": "Velocidad Viento (m/s)"}
)

st.plotly_chart(fig4_eolico, use_container_width=True)

fig4_eolico_pot = px.bar(
    df_comparativo,
    x="month",
    y="potencial_eolico_promedio",
    color="city",
    barmode="group",
    title="Potencial Eólico Mensual por Ciudad (W/m²)",
    labels={"month": "Mes", "potencial_eolico_promedio": "Potencial Eólico (W/m²)"}
)

st.plotly_chart(fig4_eolico_pot, use_container_width=True)


# ============================================================
# PREGUNTA 5 — Ranking de Días
# Tabla: ranking_dias
# Cols relevantes: city, date, score_total, score_solar,
#                  score_eolico, tipo_dia
# ============================================================

st.header("Pregunta 5 — Ranking de Días (Top 10 Mejores / Peores)")

city_rank = st.selectbox(
    "Ciudad (ranking)",
    df_ranking["city"].unique(),
    key="sel_city_rank"
)

df_city_rank = df_ranking[df_ranking["city"] == city_rank].copy()

# La tabla ya viene filtrada con top 10 mejores + top 10 peores desde el ETL
fig5_bar = px.bar(
    df_city_rank.sort_values("score_total", ascending=False),
    x="date",
    y="score_total",
    color="tipo_dia",
    title=f"Top 10 Mejores y Peores Días — {city_rank}",
    labels={"date": "Fecha", "score_total": "Score Total", "tipo_dia": "Tipo"}
)

st.plotly_chart(fig5_bar, use_container_width=True)

# Tabla detallada
cols_tabla = ["date", "tipo_dia", "score_total", "score_solar", "score_eolico",
              "uv_promedio", "viento_promedio", "nubosidad_promedio"]

st.dataframe(
    df_city_rank[cols_tabla]
    .sort_values("score_total", ascending=False)
    .reset_index(drop=True)
)


# ============================================================
# FOOTER
# ============================================================

st.markdown("---")
st.markdown(
    """
**Proyecto:** Pipeline ETLT Energías Renovables  
**Autor:** Marcelo Adrián Sosa  
**Fuente:** AWS S3 Gold Layer
"""
)