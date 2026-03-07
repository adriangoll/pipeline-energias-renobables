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
    description='Pipeline ETL Bronze -> Silver -> Gold para datos meteorologicos',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2026, 3, 1),
    catchup=False,
    tags=['spark', 'etl', 'energias-renovables', 'production']
) as dag:
    
    # Tarea 1: ETL Bronze -> Silver
    bronze_to_silver = SSHOperator(
        task_id='etl_bronze_to_silver',
        ssh_conn_id='spark_ec2_ssh',
        cmd_timeout=600,
        command="""
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
    
    # Tarea 2: ETL Silver -> Gold (placeholder)
    silver_to_gold = SSHOperator(
        task_id='etl_silver_to_gold',
        ssh_conn_id='spark_ec2_ssh',
        cmd_timeout=600,
        command="""
        echo "ETL Silver -> Gold ejecutado exitosamente"
        """
    )
    
    # Definir dependencias
    bronze_to_silver >> silver_to_gold