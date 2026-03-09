# Pipeline ETLT - Análisis de Potencial Energético Renovable

[![CI/CD Status](https://github.com/adriangoll/pipeline-energias-renobables/actions/workflows/spark_kafka.yml/badge.svg)](https://github.com/adriangoll/pipeline-energias-renobables/actions/workflows/spark_kafka.yml)

**Proyecto Integrador - Data Engineering**  
**Autor:** Marcelo Adrián Sosa  
**Institución:** Henry  
**Fecha:** Marzo 2026

---

## 📋 Descripción del Proyecto

Pipeline ETLT end-to-end para análisis de potencial energético renovable (solar y eólico) en **Riohacha (Colombia)** y **Patagonia (Argentina)**.

### Preguntas de Negocio

1. ¿Cómo varía el potencial solar a lo largo del día y del mes?
2. ¿Qué patrones históricos se observan en el potencial eólico?
3. ¿Qué condiciones climáticas están asociadas con reducciones en potencial renovable?
4. ¿Cómo se comportan las predicciones vs condiciones observadas?
5. ¿Cuáles fueron los días con mayor/menor potencial energético?

---

## 🏗️ Arquitectura

**Arquitectura Medallion** implementada en AWS S3:

- **Bronze:** Datos crudos (JSON/Parquet) de Airbyte y Kafka
- **Silver:** Datos transformados y optimizados (Parquet Snappy)
- **Gold:** Tablas analíticas listas para consumo

**Stack Tecnológico:**
- **Ingesta Batch:** Carga manual de históricos
- **Ingesta Streaming:** Airbyte Cloud (AVRO) + Apache Kafka (tiempo real)
- **Procesamiento:** PySpark 3.5
- **Orquestación:** Apache Airflow 2.8
- **Storage:** AWS S3
- **Catalogación:** AWS Glue Data Catalog
- **CI/CD:** GitHub Actions
- **Visualización:** Streamlit

---

## 📂 Estructura del Proyecto

```
pipeline-energias-renobables/
├── .github/workflows/
│   ├── spark_kafka.yml          # CI/CD principal
│   └── etl_gold.yml             # ETL Silver → Gold
├── docs/
│   ├── Avance1_Arquitectura_DataLake.pdf
│   ├── Avance2_Ingesta_Airbyte.md
│   ├── Avance3_Spark_Airflow.md
│   ├── Avance6_Kafka_Streaming.md
│   ├── GUIA_KAFKA_GOLD_COMPLETA.md
│   └── imagenes/
│       ├── avance3/              # Capturas Spark/Airflow
│       ├── glue/                 # Capturas Glue Catalog
│       ├── kafka/                # Capturas Kafka
│       └── dashboard/            # Capturas Dashboard
├── scripts_spark/
│   ├── etl_weather_s3.py         # Bronze → Silver
│   ├── etl_silver_to_gold.py     # Silver → Gold
│   └── consolidar_streaming_batch.py
├── scripts_airflow/dags/
│   └── dags_completos.py         # 2 DAGs orquestados
├── scripts_kafka/
│   ├── api_to_kafka.py           # Producer
│   └── streaming_kafka_to_s3.py  # Consumer Spark Streaming
├── dashboard/
│   └── dashboard_energias.py     # Dashboard Streamlit
├── infraestructura/
│   └── s3-lifecycle-policy.json
├── requirements.txt
└── README.md
```

---

## 🚀 Pipeline Implementado

### 1. Ingesta de Datos

**Batch (Manual):**
- Source: Archivos JSON históricos descargados de OpenWeather
- Destination: S3 `bronze/batch/historicos/`
- Formato: JSON (3.6-3.7 MB por ciudad)
- Registros: 17,568 total (8,784 por ciudad)
- Período: 2024-2025

**Streaming (Airbyte Cloud):**
- Source: OpenWeather API (tiempo real)
- Destination: S3 `bronze/streaming/openweather/`
- Formato: AVRO (comprimido)
- Frecuencia: Cada 1 hora
- Schedule: 48 calls/día (4.8% del límite Free Tier)

### 2. Procesamiento (PySpark)

**ETL Bronze → Silver:**
- Input: JSON históricos + Parquet streaming
- Transformaciones: Limpieza, conversión unidades, particionado
- Output: Parquet Snappy (97% reducción: 7.5MB → 250KB)
- Partición: Por ciudad

**ETL Silver → Gold:**
- Ejecutado en **GitHub Actions** (optimización de recursos)
- 5 tablas analíticas:
  1. `potencial_solar` - Cálculo kWh/m² por hora/día
  2. `potencial_eolico` - Potencia eólica en Watts
  3. `condiciones_criticas` - Nubosidad >80%, viento <3m/s
  4. `analisis_comparativo` - Riohacha vs Patagonia mensual
  5. `ranking_dias` - Top 10 mejores/peores días

### 3. Orquestación (Airflow)

**DAG 1: `pipeline_etl_completo`**
- Frecuencia: Diario 00:00 UTC
- Tareas: consolidar → bronze → silver → gold
- Control de errores: 3 reintentos

**DAG 2: `consolidacion_streaming_12h`**
- Frecuencia: Cada 12 horas
- Tarea: Consolidación incremental streaming → batch

### 4. Streaming en Tiempo Real (Kafka)

**Implementado con:**
- **Producer Python:** Consulta OpenWeather API cada 60s
- **Kafka Broker:** Confluent Platform 7.6.0 (Docker)
- **Topics:** `weather_riohacha`, `weather_patagonia`
- **Consumer:** Spark Structured Streaming
- **Output:** S3 `bronze/streaming/kafka/` (Parquet particionado)
- **Latencia:** <1 minuto

**Arquitectura:**
```
OpenWeather API → Producer Python → Kafka Topics → Spark Streaming → S3 Parquet
```

**Consolidación:** DAG Airflow cada 12h mueve datos de Kafka a batch para procesamiento unificado.

### 5. Gobernanza (AWS Glue)

- **Databases:** `silver_db`, `gold_db`
- **Crawler:** `crawler-silver-weather` (automatizado)
- **Catalogación:** Automática post-ETL
- **Lake Formation:** Documentado para permisos granulares (no implementado por costos)

### 6. CI/CD (GitHub Actions)

**Workflow Principal:**
- Validación: Pylint, Black, Tests
- Deploy: Scripts a S3
- Status: ✅ 5 ejecuciones exitosas

**Workflow ETL Gold:**
- Descarga JARs Hadoop AWS
- Ejecuta PySpark con credenciales IAM
- Escribe tablas a S3 Gold

---

## 📊 Dashboard Interactivo

**Tecnología:** Streamlit + Plotly

**Funcionalidades:**
- Toggle: Datos reales S3 / Datos de ejemplo
- KPIs: 4 métricas principales
- Gráficos interactivos:
  - Variación solar horaria/mensual (line + bar)
  - Patrones eólicos históricos (line + box plot)
  - Correlación nubosidad vs solar (scatter)
  - Comparativo ciudades (pie + bar + tabla)
  - Ranking Top 10 días (bar charts)

**Ejecución:**
```bash
pip install -r requirements.txt
streamlit run dashboard/dashboard_energias.py
```

---

## 🔐 Seguridad y Costos

**Seguridad:**
- IAM Role: `SparkS3Profile` con permisos S3
- Security Groups: Puertos específicos (22, 7077, 8080, 9092)
- Secrets: GitHub Actions Secrets para credenciales

**Optimización de Costos:**
- Formato: Parquet Snappy (97% compresión)
- S3 Lifecycle Policies:
  - Bronze/batch → Glacier (1 año)
  - Bronze/streaming → Borrar (30 días)
  - Silver → Deep Archive (2 años)
  - Gold → Borrar (1 año, recalculable)
- Instancias: t3.medium terminadas post-ejecución
- Free Tier: S3, Glue Catalog, GitHub Actions

---

## 📸 Capturas Documentadas

### **Arquitectura y Diseño**
1. **diagrama_arquitectura.png** - Diagrama completo del pipeline
2. **security_groups.png** - Puertos 22, 7077, 8080, 9092 configurados

### **Ingesta (Airbyte + Batch Manual)**
3. **airbyte_sources.png** - Conectores OpenWeather para streaming
4. **airbyte_destination.png** - S3 Bronze streaming (AVRO)
5. **s3_bronze_batch.png** - Archivos JSON históricos cargados manualmente
6. **s3_bronze_streaming.png** - Archivos AVRO de Airbyte

### **Procesamiento (Spark)**
7. **spark_containers.png** - `docker ps` mostrando master + worker
8. **etl_bronze_silver_logs.png** - Output exitoso con reducción 97%
9. **s3_silver_parquet.png** - Archivos Parquet en silver/weather_unified/

### **Orquestación (Airflow)**
10. **airflow_dags.png** - 2 DAGs visibles en UI
11. **dag_graph_view.png** - Vista de tareas con dependencias

### **Streaming (Kafka)**
12. **kafka_containers.png** - Zookeeper + Kafka corriendo
13. **producer_logs.png** - Mensajes enviados cada 60s
14. **spark_streaming_logs.png** - Consumer escribiendo a S3
15. **s3_kafka_data.png** - Parquet particionado en bronze/streaming/kafka/

### **Catalogación (Glue)**
16. **glue_databases.png** - silver_db y gold_db creadas
17. **glue_crawler.png** - Crawler ejecutado exitosamente
18. **glue_tables.png** - Tablas catalogadas automáticamente

### **CI/CD (GitHub Actions)**
19. **workflow_cicd_success.png** - ✅ Check verde en workflow principal
20. **workflow_etl_gold_success.png** - ✅ ETL Gold completado
21. **workflow_logs.png** - Logs mostrando instalación JARs y ejecución

### **Capa Gold**
22. **s3_gold_tables.png** - 5 carpetas con tablas analíticas
23. **gold_table_details.png** - `aws s3 ls` mostrando archivos Parquet

### **Dashboard**
24. **dashboard_home.png** - Vista general con KPIs
25. **dashboard_pregunta1.png** - Gráficos variación solar
26. **dashboard_pregunta2.png** - Patrones eólicos
27. **dashboard_pregunta5.png** - Ranking días

### **Infraestructura**
28. **ec2_instances.png** - KafkaNode y SparkStreamNode corriendo
29. **ec2_terminated.png** - Instancias terminadas (optimización costos)

---

## 🎯 Resultados y Conclusiones

### Métricas del Pipeline
- **Reducción storage:** 97% (JSON → Parquet Snappy)
- **Registros procesados:** 17,568 históricos + streaming continuo
- **Latencia streaming:** <1 minuto (trigger cada 60s)
- **Uptime CI/CD:** 100% (5/5 ejecuciones exitosas)

### Respuestas a Preguntas de Negocio
1. **Potencial solar:** Riohacha supera Patagonia en 40% promedio, pico a las 12:00
2. **Potencial eólico:** Patagonia 3x mayor velocidad viento (patrón estable)
3. **Condiciones críticas:** Nubosidad >80% reduce solar en 60-70%
4. **Predicciones:** Datos en tiempo real para validación continua
5. **Días extremos:** Identificados Top 10 para planificación operativa

### Lecciones Aprendidas
- **GitHub Actions > EC2** para ETL esporádicos (costos y simplicidad)
- **Parquet Snappy** esencial para proyectos de datos masivos
- **Kafka** ideal para validación en tiempo real de modelos
- **Lake Formation** overkill para proyectos pequeños (documentar es suficiente)

---

## 🚧 Trabajo Futuro

- [ ] Implementar Kafka Connect para ingesta directa
- [ ] Agregar dbt para transformaciones SQL en Gold
- [ ] Dashboard en producción (Streamlit Cloud)
- [ ] Integración con Amazon QuickSight
- [ ] ML para predicción de potencial energético

---

## 📚 Referencias

- [OpenWeather API](https://openweathermap.org/api)
- [Airbyte Documentation](https://docs.airbyte.com/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [AWS Glue Best Practices](https://docs.aws.amazon.com/glue/)
- [Medallion Architecture](https://www.databricks.com/glossary/medallion-architecture)

---

## 📞 Contacto

**Marcelo Adrián Sosa**  
GitHub: [@adriangoll](https://github.com/adriangoll)  
Proyecto: [pipeline-energias-renobables](https://github.com/adriangoll/pipeline-energias-renobables)

---

**Licencia:** MIT  
**Última actualización:** Marzo 2026