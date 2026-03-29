# ⚡ Pipeline ETLT - Energías Renovables

[![CI/CD](https://github.com/adriangoll/pipeline-energias-renobables/actions/workflows/spark_kafka.yml/badge.svg)](https://github.com/adriangoll/pipeline-energias-renobables/actions)

**Proyecto de Data Engineering end-to-end** para análisis de potencial energético solar y eólico.

📍 Regiones: Riohacha (Colombia) y Patagonia (Argentina)

---

## 🚀 Overview

Pipeline ETLT con arquitectura **Medallion (Bronze → Silver → Gold)** que integra:

- 📥 Ingesta batch + streaming (API + Kafka)
- ⚙️ Procesamiento distribuido con PySpark
- 🔄 Orquestación con Airflow
- ☁️ Data Lake en AWS S3
- 📊 Dashboard interactivo (Streamlit)
- 🔁 CI/CD con GitHub Actions

---

## 🏗️ Arquitectura

API → Airbyte / Kafka → S3 Bronze → Spark → Silver → Gold → Dashboard
- **Bronze:** datos crudos (JSON / AVRO)
- **Silver:** datos limpios (Parquet)
- **Gold:** tablas analíticas

---

## 🛠️ Stack Tecnológico

- PySpark 3.5
- Apache Airflow
- Apache Kafka
- AWS S3 + Glue
- Airbyte
- Streamlit
- GitHub Actions

---

## 📊 Casos de Uso

- ☀️ Análisis de potencial solar por hora/día
- 🌬️ Patrones históricos de viento
- ⚠️ Detección de condiciones críticas (nubosidad, viento)
- 📈 Comparación entre regiones
- 🏆 Ranking de mejores/peores días

---

## ⚡ Features Clave

- Streaming near real-time (<1 min latencia)
- Reducción de almacenamiento: **97% (JSON → Parquet)**
- Pipeline automatizado con Airflow
- ETL ejecutado en CI/CD (optimización de costos)
- Datos batch + streaming unificados

---

## 📂 Estructura

├── scripts_spark/ # ETL PySpark
├── scripts_airflow/ # DAGs
├── scripts_kafka/ # Streaming
├── dashboard/ # Streamlit
├── docs/ # Documentación y capturas
---

## ▶️ Cómo ejecutar

```bash
pip install -r requirements.txt
streamlit run dashboard/dashboard_energias.py
