# Pipeline ETLT - Análisis de Potencial Energético Renovable

![CI/CD Status](https://github.com/TU_USUARIO/pipeline-energias-renovables/actions/workflows/spark_kafka.yml/badge.svg)

## 📊 Descripción

Pipeline de datos para análisis de potencial solar y eólico en Riohacha (Colombia) y Patagonia (Argentina).

**Stack tecnológico:**
- AWS S3 (Data Lake)
- Apache Spark (Procesamiento)
- Apache Airflow (Orquestación)
- Apache Kafka (Streaming)
- GitHub Actions (CI/CD)

## 📂 Estructura del Proyecto
```
├── docs/              # Documentación técnica
├── scripts_spark/     # ETL con PySpark
├── scripts_airflow/   # DAGs de orquestación
├── scripts_kafka/     # Scripts de streaming
└── tests/            # Tests automatizados
```

## 📖 Documentación

- [Arquitectura del Data Lake (Avance 1)](docs/Avance1_Arquitectura_DataLake.md)
- [Diagrama de Arquitectura](docs/arquitectura_datalake.mermaid)

## 🚀 CI/CD

Este proyecto usa GitHub Actions para:
- ✅ Validar sintaxis Python
- ✅ Ejecutar tests automáticos
- ✅ Validar DAGs de Airflow
- ✅ Deploy automático a S3

## 👨‍💻 Autor

[Tu nombre] - Proyecto Integrador Henry