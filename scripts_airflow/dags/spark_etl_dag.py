from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False
}

with DAG(
    'etl_energias_renovables',
    default_args=default_args,
    description='Pipeline ETL completo Bronze -> Silver -> Gold para datos meteorologicos',
    schedule_interval='0 0 * * *',  # Diario a las 00:00 UTC
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=['spark', 'etl', 'energias-renovables', 'production']
) as dag:
    
    # ============================================================
    # TAREA 1: ETL Bronze → Silver
    # ============================================================
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
        
        echo "ETL Bronze → Silver completado"
        """
    )
    
    # ============================================================
    # TAREA 2: ETL Silver → Gold
    # ============================================================
    etl_silver_to_gold = SSHOperator(
        task_id='etl_silver_to_gold',
        ssh_conn_id='spark_ec2_ssh',
        cmd_timeout=900,  # 15 minutos (más tiempo para agregaciones)
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
        
        echo "ETL Silver → Gold completado"
        """
    )
    
    # ============================================================
    # DEPENDENCIAS: Silver depende de Bronze
    # ============================================================
    etl_bronze_to_silver >> etl_silver_to_gold