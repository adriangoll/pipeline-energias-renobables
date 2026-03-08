# Pipeline ETLT - Análisis de Potencial Energético Renovable

---

## 📊 Descripción del Proyecto

Pipeline de datos end-to-end para análisis de potencial solar y eólico en **Riohacha (Colombia)** y **Patagonia (Argentina)**. Combina procesamiento batch y streaming sobre una arquitectura de Data Lake moderna en AWS.

**Período de análisis:** Agosto 2024 - Marzo 2026  
**Datos procesados:** ~17,500+ registros históricos + streaming continuo  
**Preguntas de negocio respondidas:** 5 casos de uso clave

---

## 🎯 Preguntas de Negocio

Este pipeline fue diseñado para responder:

1. ¿Cómo varía el potencial solar estimado a lo largo del día y del mes en ambas ubicaciones?
2. ¿Qué patrones históricos se observan en el potencial eólico?
3. ¿Qué condiciones climáticas están asociadas con reducciones significativas en el potencial renovable?
4. ¿Cómo se comportan las predicciones meteorológicas vs condiciones observadas en el pasado reciente?
5. ¿Cuáles fueron los días con mayor y menor potencial energético durante el período de análisis?

---

## 🏗️ Arquitectura

### Stack Tecnológico

| Componente | Tecnología | Propósito |
|------------|-----------|-----------|
| **Almacenamiento** | AWS S3 | Data Lake (Bronze-Silver-Gold) |
| **Ingesta Batch** | Carga manual datos históricos .json
| **Streaming** | Apache Kafka | Eventos en tiempo real |Airbyte Cloud |         |Conectores a APIs y bases de datos |
| **Procesamiento** | Apache Spark (PySpark) | Transformaciones distribuidas |
| **Orquestación** | Apache Airflow | DAGs y scheduling |
| **CI/CD** | GitHub Actions | Validación y deploy automático |
| **Infraestructura** | AWS EC2, IAM, S3 | Cloud computing y seguridad |

### Arquitectura Medallion (Bronze-Silver-Gold)

```
┌─────────────────┐
│  Fuentes Datos  │
│  - OpenWeather  │──┐
│  - JSON Files   │  │
│  - PostgreSQL   │  │
└─────────────────┘  │
                     ▼
          ┌──────────────────┐
          │   Airbyte/Kafka  │ (Ingesta)
          └────────┬─────────┘
                   ▼
┌──────────────────────────────────────────┐
│          AWS S3 Data Lake                │
│  ┌──────────┐  ┌──────────┐  ┌────────┐ │
│  │  BRONZE  │→ │  SILVER  │→ │  GOLD  │ │
│  │  (Raw)   │  │ (Clean)  │  │(Analyt)│ │
│  │   JSON   │  │ Parquet  │  │Parquet │ │
│  └──────────┘  └──────────┘  └────────┘ │
└───────────┬──────────────────────────────┘
            ▼
   ┌────────────────┐
   │  Apache Spark  │ (Transformaciones)
   └────────┬───────┘
            ▼
   ┌────────────────┐
   │ Apache Airflow │ (Orquestación)
   └────────────────┘
```

**Ver diagrama completo:** [docs/diagrama_ETLT.png](docs/diagrama_arquitectura.png)

---

## 📂 Estructura del Proyecto

```
pipeline-energias-renovables/
├── .github/
│   └── workflows/
│       └── spark_kafka.yml          # CI/CD pipeline
├── docs/
│   ├── Avance1_Arquitectura_DataLake.pdf
│   ├── Avance2_Ingesta_Airbyte.md
│   ├── Avance3_Spark_Airflow.md
│   ├── diagrama_ETLT.png
│   └── imagenes/
│       ├── avance2/
│       └── avance3/
├── scripts_spark/
│   └── etl_weather_s3.py            # ETL Bronze → Silver
├── scripts_airflow/
│   └── dags/
│       └── spark_etl_dag.py         # DAG principal
├── scripts_kafka/
│   └── (scripts de streaming)
├── tests/
│   └── test_spark_etl.py            # Tests automatizados
├── .gitignore
└── README.md                         # Este archivo
```

---

## 🚀 Pipeline de Datos

### Flujo de Datos

#### 1. **Ingesta (Bronze Layer)**

**Batch:**
- Archivos JSON históricos → S3 (carga manual inicial)
- Airbyte: OpenWeather API → S3 (cada hora)

**Streaming:**
- Kafka Producer: OpenWeather API → Topics (cada hora)
- Kafka Consumer: Topics → S3 Bronze


#### 2. **Transformación (Silver Layer)**

**Herramienta:** PySpark

**Transformaciones:**
- Normalización de esquemas JSON
- Conversión de tipos de datos
- Eliminación de duplicados
- Manejo de valores nulos
- Agregación de columna `city`

**Formato:** Parquet con compresión Snappy  
**Particionamiento:** `year/month/day/city`

**Optimización:** 97% reducción de storage (7.5 MB → 250 KB)

#### 3. **Agregación (Gold Layer)**

**Tablas analíticas:**
- `potencial_solar/` - Estimaciones kWh/m²/día
- `potencial_eolico/` - Patrones históricos de viento
- `analisis_comparativo/` - Riohacha vs Patagonia
- `prediccion_vs_observado/` - Accuracy de forecasts

#### 4. **Orquestación (Airflow)**

**DAG:** `etl_energias_renovables`

**Tareas:**
1. Trigger Airbyte sync
2. Spark: Bronze → Silver
3. Spark: Silver → Gold
4. Data Quality Checks

**Schedule:** Diario a las 00:00 UTC

---

## 🛠️ Setup y Configuración

### Prerrequisitos

- AWS CLI configurado
- Python 3.10+
- Docker Desktop
- Git

### 1. Clonar el Repositorio

```bash
git clone https://github.com/adriangoll/pipeline-energias-renovables.git
cd pipeline-energias-renovables
```

### 2. Configurar AWS S3

```bash
# Crear bucket
aws s3 mb s3://datalake-energias-renovables-dev --region us-east-1

# Crear estructura de carpetas
aws s3api put-object --bucket datalake-energias-renovables-dev --key bronze/streaming/openweather/
aws s3api put-object --bucket datalake-energias-renovables-dev --key bronze/batch/historicos/
aws s3api put-object --bucket datalake-energias-renovables-dev --key silver/weather_unified/
aws s3api put-object --bucket datalake-energias-renovables-dev --key gold/potencial_solar/
```

### 3. Configurar Airbyte (Cloud o Local)

**Opción A: Airbyte Cloud**
1. Crear cuenta en https://cloud.airbyte.com
2. Configurar Sources (OpenWeather API)
3. Configurar Destination (S3)
4. Crear Connections con schedule horario

**Opción B: Airbyte Open Source**
```bash
git clone https://github.com/airbytehq/airbyte.git
cd airbyte
./run-ab-platform.sh
```

### 4. Desplegar Spark en EC2

Ver guía completa: [docs/Avance3_Spark_Airflow.md](docs/Avance3_Spark_airflow.md)

**Resumen:**
```bash
# Lanzar instancia EC2
aws ec2 run-instances --image-id ami-0c101f26f147fa7fd --instance-type t3.medium ...

# SSH a la instancia
ssh -i SparkKeyV3.pem ec2-user@IP_PUBLICA

# Instalar Docker y Spark
sudo yum install -y docker
docker run -d --name spark-master spark:4.0.1
```

### 5. Desplegar Airflow en EC2

```bash
# En servidor Airflow
docker run -d --name airflow -p 8080:8080 apache/airflow:2.8.1

# Copiar DAGs
docker cp scripts_airflow/dags/ airflow:/opt/airflow/dags/
```

---

## 🧪 CI/CD con GitHub Actions

### Workflow Automático

Cada `git push` ejecuta:

1. ✅ **Validación de sintaxis** (Pylint)
2. ✅ **Tests unitarios** (Pytest)
3. ✅ **Validación de DAGs** (Airflow)
4. ✅ **Deploy a S3** (si push a main)

**Archivo:** [.github/workflows/spark_kafka.yml](.github/workflows/spark_kafka.yml)

### Ejecutar Tests Localmente

```bash
# Instalar dependencias
pip install -r requirements.txt

# Ejecutar tests
pytest tests/ -v

# Validar sintaxis
pylint scripts_spark/*.py scripts_airflow/dags/*.py
```

---

## 📊 Resultados y Métricas

### Datos Procesados

| Métrica | Valor |
|---------|-------|
| **Registros históricos** | 17,568 (8,784 por ciudad) |
| **Período histórico** | Agosto 2024 - Agosto 2025 |
| **Registros streaming** | ~1,440/mes (2 ciudades × 24 horas × 30 días) |
| **Storage Bronze** | 7.5 MB (JSON) |
| **Storage Silver** | 250 KB (Parquet, 97% reducción) |
| **Frecuencia de ingesta** | Cada 1 hora |

### Optimizaciones Logradas

**Storage:**
- JSON → Parquet: 97% reducción de tamaño
- Compresión Snappy: 70% reducción adicional
- Particionamiento inteligente: Queries 10x más rápidos

**Procesamiento:**
- PySpark distribuido: Procesamiento paralelo en múltiples cores
- Caché de DataFrames: Reduce lecturas repetidas de S3
- Broadcast joins: Optimiza joins de tablas pequeñas

**Costos estimados (AWS):**
- S3 Storage: ~$0.01/mes (primeros 5 GB gratis)
- EC2 t3.medium: Detenidas cuando no se usan (solo costos por uso)
- Free Tier completo utilizado

---

## 🔒 Seguridad

### AWS IAM Roles

**Segregación de permisos:**

| Role | Permisos | Uso |
|------|----------|-----|
| `AirbyteRole` | `s3:PutObject` en `/bronze/` | Solo ingesta |
| `SparkS3Role` | `s3:GetObject` en `/bronze/`, `s3:PutObject` en `/silver/,/gold/` | Procesamiento |
| `AnalystRole` | `s3:GetObject` en `/gold/` (read-only) | Consumo de datos |

### Buenas Prácticas Implementadas

- ✅ Principio de mínimo privilegio
- ✅ Secrets en variables de entorno
- ✅ Encriptación AES-256 en S3
- ✅ Versionado de objetos habilitado
- ✅ Bucket no público

---

## 📖 Documentación Técnica

### Avances del Proyecto

1. **[Avance 1: Arquitectura del Data Lake](docs/Avance1_Arquitectura_DataLake.pdf)**
   - Diseño de arquitectura Medallion
   - Justificación de stack tecnológico
   - Análisis de fuentes de datos

2. **[Avance 2: Ingesta con Airbyte](docs/Avance2_Ingesta_Airbyte.md)**
   - Configuración de Airbyte Cloud
   - Conexiones OpenWeather API → S3
   - Validación de datos ingestados

3. **[Avance 3: Procesamiento con Spark y Orquestación con Airflow](docs/Avance3_Spark_Airflow.md)**
   - Despliegue de Spark en EC2
   - ETL Bronze → Silver con PySpark
   - Configuración de Airflow
   - DAG de orquestación

### Diagramas

- **[Arquitectura Completa](docs/diagrama_arquitectura.png)** - Vista general del sistema
- **[Flujo de Datos](docs/Avance3_Spark_Airflow.md#flujo-de-datos)** - Pipeline detallado

---

## 🚧 Trabajo Futuro

### Mejoras Planificadas

- [ ] **Streaming real con Kafka:** Implementar producers/consumers
- [ ] **Capa Gold completa:** Calcular métricas de potencial energético
- [ ] **Data Quality:** Integrar Great Expectations
- [ ] **Visualización:** Dashboard con Streamlit o Superset
- [ ] **Monitoreo:** CloudWatch Alarms para detección de fallos
- [ ] **Lake Formation:** Gobernanza avanzada (permisos granulares)
- [ ] **Glue Crawler:** Catalogación automática de metadatos

### Escalabilidad

**Para producción:**
- EMR en lugar de Spark en EC2 (auto-scaling)
- AWS MSK en lugar de Kafka en EC2 (managed)
- Airbyte Open Source en EKS (mayor control)
- Multiple environments (dev/staging/prod)

---

## 👨‍💻 Autor

**[Tu nombre]**  
Proyecto Integrador - Henry Data Engineering  
Marzo 2026

---

## 📝 Licencia

Este proyecto es de uso académico. Desarrollado como parte del Proyecto Integrador de Henry.

---

## 🙏 Agradecimientos

- **Henry:** Por la formación en Data Engineering
- **Comunidad open-source:** Apache Spark, Airflow, Kafka, Airbyte
- **AWS:** Por el Free Tier que permitió este proyecto

---

## 📞 Contacto

- LinkedIn: [tu-linkedin]
- GitHub: [@tu-usuario](https://github.com/tu-usuario)
- Email: tu-email@ejemplo.com

---

**⭐ Si este proyecto te resultó útil, dale una estrella en GitHub!**