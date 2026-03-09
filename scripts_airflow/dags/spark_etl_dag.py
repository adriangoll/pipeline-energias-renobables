from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# ============================================================
# CONFIGURACIÓN DEL DAG
# ============================================================

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False
}

# ============================================================
# DAG 1: Pipeline ETL Completo (Diario a las 00:00 UTC)
# ============================================================

with DAG(
    'pipeline_etl_completo',
    default_args=default_args,
    description='Pipeline ETL completo Bronze → Silver → Gold (diario)',
    schedule_interval='0 0 * * *',  # Diario a las 00:00 UTC
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=['spark', 'etl', 'batch', 'production']
) as dag_etl:
    
    # Tarea 1: Consolidar streaming → batch
    consolidar_streaming = SSHOperator(
        task_id='consolidar_streaming_a_batch',
        ssh_conn_id='spark_ec2_ssh',
        cmd_timeout=600,
        command="""
        echo "Consolidando datos de streaming..."
        docker exec spark-master /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --conf spark.jars.ivy=/tmp/.ivy2 \
            --packages org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.367 \
            --executor-memory 1g \
            --executor-cores 2 \
            /opt/spark/consolidar_streaming_batch.py
        """
    )
    
    # Tarea 2: ETL Bronze → Silver
    etl_bronze_to_silver = SSHOperator(
        task_id='etl_bronze_to_silver',
        ssh_conn_id='spark_ec2_ssh',
        cmd_timeout=600,
        command="""
        echo "Ejecutando ETL Bronze → Silver..."
        docker exec spark-master /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --conf spark.jars.ivy=/tmp/.ivy2 \
            --packages org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.367 \
            --executor-memory 1g \
            --executor-cores 2 \
            /opt/spark/etl_weather_s3.py
        """
    )
    
    # Tarea 3: ETL Silver → Gold
    etl_silver_to_gold = SSHOperator(
        task_id='etl_silver_to_gold',
        ssh_conn_id='spark_ec2_ssh',
        cmd_timeout=900,
        command="""
        echo "Ejecutando ETL Silver → Gold..."
        docker exec spark-master /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --conf spark.jars.ivy=/tmp/.ivy2 \
            --packages org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.367 \
            --executor-memory 1g \
            --executor-cores 2 \
            /opt/spark/etl_silver_to_gold.py
        """
    )
    
    # Dependencias: streaming → bronze → silver → gold
    consolidar_streaming >> etl_bronze_to_silver >> etl_silver_to_gold


# ============================================================
# DAG 2: Consolidación Incremental (Cada 12 horas)
# ============================================================

with DAG(
    'consolidacion_streaming_12h',
    default_args=default_args,
    description='Consolidación incremental streaming → batch (cada 12h)',
    schedule_interval='0 */12 * * *',  # Cada 12 horas (00:00 y 12:00 UTC)
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=['streaming', 'consolidacion', 'incremental']
) as dag_consolidacion:
    
    consolidar_incremental = SSHOperator(
        task_id='consolidar_streaming_incremental',
        ssh_conn_id='spark_ec2_ssh',
        cmd_timeout=600,
        command="""
        echo "Consolidación incremental streaming → batch..."
        docker exec spark-master /opt/spark/bin/spark-submit \
            --master spark://spark-master:7077 \
            --deploy-mode client \
            --conf spark.jars.ivy=/tmp/.ivy2 \
            --packages org.apache.hadoop:hadoop-aws:3.4.0,com.amazonaws:aws-java-sdk-bundle:1.12.367 \
            --executor-memory 1g \
            --executor-cores 2 \
            /opt/spark/consolidar_streaming_batch.py
        """
    )