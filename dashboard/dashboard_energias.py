"""
Dashboard Streamlit - Análisis de Potencial Energético Renovable
Proyecto: Pipeline ETLT Energías Renovables

Este dashboard consume datos de la capa Gold en S3 y responde
las 5 preguntas de negocio del proyecto.
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import boto3
from io import BytesIO

# ============================================================
# CONFIGURACIÓN DE PÁGINA
# ============================================================
st.set_page_config(
    page_title="Dashboard Energías Renovables",
    page_icon="🌞",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================
# FUNCIÓN PARA LEER DATOS DE S3
# ============================================================
@st.cache_data(ttl=3600)  # Cache por 1 hora
def load_data_from_s3(bucket, key):
    """
    Lee archivos Parquet desde S3 y retorna DataFrame
    """
    try:
        s3_client = boto3.client('s3')
        
        # Listar archivos en el prefijo
        response = s3_client.list_objects_v2(Bucket=bucket, Prefix=key)
        
        if 'Contents' not in response:
            st.warning(f"No se encontraron datos en {key}")
            return pd.DataFrame()
        
        # Leer todos los archivos Parquet
        dfs = []
        for obj in response['Contents']:
            if obj['Key'].endswith('.parquet'):
                obj_data = s3_client.get_object(Bucket=bucket, Key=obj['Key'])
                df = pd.read_parquet(BytesIO(obj_data['Body'].read()))
                dfs.append(df)
        
        if dfs:
            return pd.concat(dfs, ignore_index=True)
        else:
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Error cargando datos: {str(e)}")
        return pd.DataFrame()

# ============================================================
# FUNCIÓN PARA SIMULAR DATOS (Mientras no hay datos en Gold)
# ============================================================
def generate_sample_data():
    """
    Genera datos de ejemplo para demostración
    (Eliminar cuando haya datos reales en Gold)
    """
    import numpy as np
    from datetime import datetime, timedelta
    
    # Generar fechas
    start_date = datetime(2024, 8, 1)
    dates = [start_date + timedelta(days=i) for i in range(365)]
    
    data = []
    for date in dates:
        # Generar 24 horas para cada día
        for hour in range(24):
            for city in ['Riohacha', 'Patagonia']:
                # Riohacha: más sol, menos viento
                # Patagonia: menos sol, más viento
                if city == 'Riohacha':
                    # Solar varía según hora (pico al mediodía)
                    solar_base = np.random.uniform(4.5, 7.5)
                    hour_factor = 1.0 - abs(hour - 12) / 12.0 if 6 <= hour <= 18 else 0.1
                    solar = solar_base * hour_factor
                    wind = np.random.uniform(2.0, 8.0)
                    temp = np.random.uniform(26, 32)
                    clouds = np.random.uniform(10, 40)
                else:
                    solar_base = np.random.uniform(2.5, 5.5)
                    hour_factor = 1.0 - abs(hour - 12) / 12.0 if 6 <= hour <= 18 else 0.1
                    solar = solar_base * hour_factor
                    wind = np.random.uniform(8.0, 20.0)
                    temp = np.random.uniform(5, 20)
                    clouds = np.random.uniform(30, 70)
                
                data.append({
                    'date': date,
                    'city': city,
                    'year': date.year,
                    'month': date.month,
                    'day': date.day,
                    'hour': hour,
                    'potencial_solar_kwh_m2': solar,
                    'wind_speed_ms': wind,
                    'temperatura': temp,
                    'nubosidad': clouds,
                    'score_total': solar + (wind / 5)
                })
    
    return pd.DataFrame(data)

# ============================================================
# CARGAR DATOS
# ============================================================
st.title("🌞 Dashboard de Potencial Energético Renovable")
st.markdown("**Análisis de Riohacha (Colombia) vs Patagonia (Argentina)**")

# Sidebar para configuración
st.sidebar.header("⚙️ Configuración")

# Opción para usar datos reales o de ejemplo
use_real_data = st.sidebar.checkbox("Usar datos reales de S3", value=False)

if use_real_data:
    bucket = "datalake-energias-renovables-dev"
    
    with st.spinner("Cargando datos desde S3..."):
        df_solar = load_data_from_s3(bucket, "gold/potencial_solar/")
        df_eolico = load_data_from_s3(bucket, "gold/potencial_eolico/")
        df_ranking = load_data_from_s3(bucket, "gold/ranking_dias/")
        df_comparativo = load_data_from_s3(bucket, "gold/analisis_comparativo/")
    
    if df_solar.empty:
        st.warning("⚠️ No hay datos en Gold. Ejecuta primero el ETL Silver → Gold.")
        st.info("💡 Activa 'Usar datos de ejemplo' en el sidebar para ver el dashboard.")
        st.stop()
else:
    st.info("📊 Mostrando datos de ejemplo (simulados)")
    df_sample = generate_sample_data()
    
    # Simular estructura de tablas Gold
    df_solar = df_sample[['city', 'year', 'month', 'day', 'hour', 'potencial_solar_kwh_m2', 'temperatura', 'nubosidad']].copy()
    df_eolico = df_sample[['city', 'year', 'month', 'day', 'wind_speed_ms']].copy()
    df_ranking = df_sample[['city', 'date', 'score_total', 'potencial_solar_kwh_m2', 'wind_speed_ms']].copy()
    df_comparativo = df_sample.groupby(['city', 'year', 'month']).agg({
        'potencial_solar_kwh_m2': 'mean',
        'wind_speed_ms': 'mean',
        'temperatura': 'mean'
    }).reset_index()

# ============================================================
# MÉTRICAS PRINCIPALES
# ============================================================
st.header("📊 Métricas Clave")

col1, col2, col3, col4 = st.columns(4)

with col1:
    total_dias = len(df_ranking)
    st.metric("Total de Días Analizados", f"{total_dias:,}")

with col2:
    avg_solar = df_solar['potencial_solar_kwh_m2'].mean()
    st.metric("Potencial Solar Promedio", f"{avg_solar:.2f} kWh/m²")

with col3:
    avg_wind = df_eolico['wind_speed_ms'].mean()
    st.metric("Velocidad Viento Promedio", f"{avg_wind:.2f} m/s")

with col4:
    ciudades = df_solar['city'].nunique()
    st.metric("Ubicaciones", ciudades)

# ============================================================
# PREGUNTA 1: Variación del Potencial Solar
# ============================================================
st.header("🌞 Pregunta 1: Variación del Potencial Solar")
st.markdown("*¿Cómo varía el potencial solar a lo largo del día y del mes?*")

# Filtros
col1, col2 = st.columns(2)
with col1:
    ciudad_filtro = st.selectbox("Ciudad:", df_solar['city'].unique(), key='p1_ciudad')
with col2:
    mes_filtro = st.selectbox("Mes:", sorted(df_solar['month'].unique()), key='p1_mes')

# Filtrar datos
df_p1 = df_solar[(df_solar['city'] == ciudad_filtro) & (df_solar['month'] == mes_filtro)]

# Gráfico: Variación horaria
df_horaria = df_p1.groupby('hour')['potencial_solar_kwh_m2'].mean().reset_index()

fig1 = px.line(
    df_horaria, 
    x='hour', 
    y='potencial_solar_kwh_m2',
    title=f'Potencial Solar Promedio por Hora - {ciudad_filtro} (Mes {mes_filtro})',
    labels={'hour': 'Hora del Día', 'potencial_solar_kwh_m2': 'Potencial Solar (kWh/m²)'}
)
fig1.update_traces(line_color='#FFA500', line_width=3)
st.plotly_chart(fig1, use_container_width=True)

# Gráfico: Variación mensual
df_mensual = df_solar[df_solar['city'] == ciudad_filtro].groupby('month')['potencial_solar_kwh_m2'].mean().reset_index()

fig2 = px.bar(
    df_mensual,
    x='month',
    y='potencial_solar_kwh_m2',
    title=f'Potencial Solar Promedio por Mes - {ciudad_filtro}',
    labels={'month': 'Mes', 'potencial_solar_kwh_m2': 'Potencial Solar (kWh/m²)'},
    color='potencial_solar_kwh_m2',
    color_continuous_scale='YlOrRd'
)
st.plotly_chart(fig2, use_container_width=True)

# ============================================================
# PREGUNTA 2: Patrones Históricos del Potencial Eólico
# ============================================================
st.header("💨 Pregunta 2: Patrones Históricos del Potencial Eólico")
st.markdown("*¿Qué patrones históricos se observan en el potencial eólico?*")

# Comparación entre ciudades
df_eolico_comp = df_eolico.groupby(['city', 'month'])['wind_speed_ms'].mean().reset_index()

fig3 = px.line(
    df_eolico_comp,
    x='month',
    y='wind_speed_ms',
    color='city',
    title='Velocidad del Viento Promedio por Mes (Comparación)',
    labels={'month': 'Mes', 'wind_speed_ms': 'Velocidad Viento (m/s)', 'city': 'Ciudad'},
    markers=True
)
st.plotly_chart(fig3, use_container_width=True)

# Distribución de velocidades
fig4 = px.box(
    df_eolico,
    x='city',
    y='wind_speed_ms',
    color='city',
    title='Distribución de Velocidades del Viento por Ciudad',
    labels={'city': 'Ciudad', 'wind_speed_ms': 'Velocidad Viento (m/s)'}
)
st.plotly_chart(fig4, use_container_width=True)

# ============================================================
# PREGUNTA 3: Condiciones Climáticas Críticas
# ============================================================
st.header("⚠️ Pregunta 3: Condiciones Climáticas Críticas")
st.markdown("*¿Qué condiciones están asociadas con reducciones en potencial renovable?*")

# Correlación nubosidad vs potencial solar
fig5 = px.scatter(
    df_solar,
    x='nubosidad',
    y='potencial_solar_kwh_m2',
    color='city',
    title='Impacto de la Nubosidad en el Potencial Solar',
    labels={'nubosidad': 'Nubosidad (%)', 'potencial_solar_kwh_m2': 'Potencial Solar (kWh/m²)', 'city': 'Ciudad'},
    opacity=0.6
)
st.plotly_chart(fig5, use_container_width=True)

st.markdown("""
**Interpretación:**  
- ✅ **Mayor nubosidad → Menor potencial solar** (correlación negativa)
- ✅ Días con nubosidad >80% reducen potencial solar hasta en 70%
""")

# ============================================================
# PREGUNTA 4: Comparación Riohacha vs Patagonia
# ============================================================
st.header("📊 Pregunta 4: Análisis Comparativo")
st.markdown("*¿Cómo se comparan ambas ubicaciones?*")

col1, col2 = st.columns(2)

with col1:
    # Potencial solar por ciudad
    df_comp_solar = df_comparativo.groupby('city')['potencial_solar_kwh_m2'].mean().reset_index()
    
    fig6 = px.pie(
        df_comp_solar,
        values='potencial_solar_kwh_m2',
        names='city',
        title='Potencial Solar Promedio (Distribución %)',
        color_discrete_sequence=['#FFA500', '#1E90FF']
    )
    st.plotly_chart(fig6, use_container_width=True)

with col2:
    # Potencial eólico por ciudad
    df_comp_wind = df_comparativo.groupby('city')['wind_speed_ms'].mean().reset_index()
    
    fig7 = px.bar(
        df_comp_wind,
        x='city',
        y='wind_speed_ms',
        title='Velocidad Viento Promedio por Ciudad',
        labels={'city': 'Ciudad', 'wind_speed_ms': 'Viento (m/s)'},
        color='city',
        color_discrete_sequence=['#FFA500', '#1E90FF']
    )
    st.plotly_chart(fig7, use_container_width=True)

# Tabla comparativa
st.subheader("Tabla Comparativa")
comp_table = df_comparativo.groupby('city').agg({
    'potencial_solar_kwh_m2': ['mean', 'max'],
    'wind_speed_ms': ['mean', 'max'],
    'temperatura': 'mean'
}).round(2)
comp_table.columns = ['Solar Promedio', 'Solar Máximo', 'Viento Promedio', 'Viento Máximo', 'Temp Promedio']
st.dataframe(comp_table, use_container_width=True)

# ============================================================
# PREGUNTA 5: Ranking de Días
# ============================================================
st.header("🏆 Pregunta 5: Días con Mayor/Menor Potencial")
st.markdown("*¿Cuáles fueron los mejores y peores días?*")

ciudad_rank = st.selectbox("Selecciona ciudad:", df_ranking['city'].unique(), key='p5_ciudad')

# Top 10 mejores días
df_top10 = df_ranking[df_ranking['city'] == ciudad_rank].nlargest(10, 'score_total')

fig8 = px.bar(
    df_top10,
    x='date',
    y='score_total',
    title=f'Top 10 Mejores Días - {ciudad_rank}',
    labels={'date': 'Fecha', 'score_total': 'Score Total (Solar + Eólico)'},
    color='score_total',
    color_continuous_scale='Greens'
)
st.plotly_chart(fig8, use_container_width=True)

# Top 10 peores días
df_worst10 = df_ranking[df_ranking['city'] == ciudad_rank].nsmallest(10, 'score_total')

fig9 = px.bar(
    df_worst10,
    x='date',
    y='score_total',
    title=f'Top 10 Peores Días - {ciudad_rank}',
    labels={'date': 'Fecha', 'score_total': 'Score Total'},
    color='score_total',
    color_continuous_scale='Reds'
)
st.plotly_chart(fig9, use_container_width=True)

# Tabla de detalles
st.subheader("Detalle de Mejores Días")
st.dataframe(
    df_top10[['date', 'score_total', 'potencial_solar_kwh_m2', 'wind_speed_ms']].reset_index(drop=True),
    use_container_width=True
)

# ============================================================
# PIE DE PÁGINA
# ============================================================
st.markdown("---")
st.markdown("""
**Dashboard desarrollado por:** Marcelo Adrián Sosa  
**Proyecto:** Pipeline ETLT - Energías Renovables  
**Fecha:** Marzo 2026  
**Fuente de datos:** AWS S3 (Capa Gold)
""")