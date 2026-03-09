"""
Consolidación Streaming → Batch
Pipeline ETLT - Energías Renovables

Este script consolida datos de streaming en el histórico batch
y limpia archivos antiguos de la carpeta streaming.

Frecuencia de ejecución: Cada 12 horas (00:00 y 12:00 UTC)

Lógica:
1. Lee datos de /bronze/streaming/openweather/ (últimos datos)
2. Valida y deduplica
3. Append a /bronze/batch/historicos/
4. Elimina archivos de streaming >30 días
"""

import sys
import boto3
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, lit

def main():
    # Configuración de Spark
    spark = SparkSession.builder \
        .appName("Consolidacion_Streaming_to_Batch") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "com.amazonaws.auth.InstanceProfileCredentialsProvider") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Cliente S3 para operaciones de limpieza
    s3_client = boto3.client('s3')
    bucket_name = "datalake-energias-renovables-dev"

    try:
        print("=" * 80)
        print("🔄 INICIANDO CONSOLIDACIÓN: STREAMING → BATCH")
        print("=" * 80)

        # ============================================================
        # PASO 1: Leer datos de streaming
        # ============================================================
        print("\n📂 PASO 1: Leyendo datos de streaming...")
        
        streaming_path = "s3a://datalake-energias-renovables-dev/bronze/streaming/openweather/"
        
        try:
            df_streaming = spark.read.json(streaming_path)
            count_streaming = df_streaming.count()
            print(f"   ✅ Registros encontrados en streaming: {count_streaming:,}")
        except Exception as e:
            print(f"   ⚠️  No hay datos en streaming o error: {str(e)}")
            print("   Finalizando sin cambios.")
            return

        if count_streaming == 0:
            print("   ℹ️  No hay datos nuevos para consolidar.")
            return

        # ============================================================
        # PASO 2: Extraer y normalizar datos de Airbyte
        # ============================================================
        print("\n🔄 PASO 2: Normalizando datos de Airbyte...")
        
        # Airbyte envuelve datos en _airbyte_data
        df_normalized = df_streaming.select(
            col("_airbyte_emitted_at").alias("ingestion_timestamp"),
            col("_airbyte_data.*")
        )
        
        print(f"   ✅ Datos normalizados: {df_normalized.count():,} registros")

        # ============================================================
        # PASO 3: Deduplicación
        # ============================================================
        print("\n🗑️  PASO 3: Eliminando duplicados...")
        
        # Deduplicar por timestamp (dt) y coordenadas
        df_deduplicated = df_normalized.dropDuplicates(["dt", "coord.lat", "coord.lon"])
        
        duplicates_removed = count_streaming - df_deduplicated.count()
        print(f"   ✅ Duplicados eliminados: {duplicates_removed:,}")
        print(f"   ✅ Registros únicos: {df_deduplicated.count():,}")

        # ============================================================
        # PASO 4: Append a batch históricos
        # ============================================================
        print("\n💾 PASO 4: Consolidando en batch históricos...")
        
        batch_path = "s3a://datalake-energias-renovables-dev/bronze/batch/historicos/"
        
        # Convertir a formato consistente con históricos existentes
        # (Los históricos ya tienen una estructura específica)
        df_deduplicated.write \
            .mode("append") \
            .json(batch_path)
        
        print(f"   ✅ {df_deduplicated.count():,} registros consolidados en batch")

        # ============================================================
        # PASO 5: Limpieza de archivos antiguos en streaming
        # ============================================================
        print("\n🧹 PASO 5: Limpiando archivos antiguos de streaming...")
        
        cutoff_date = datetime.now() - timedelta(days=30)
        streaming_prefix = "bronze/streaming/openweather/"
        
        # Listar objetos en streaming
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=streaming_prefix
        )
        
        if 'Contents' not in response:
            print("   ℹ️  No hay archivos para limpiar.")
        else:
            deleted_count = 0
            total_size = 0
            
            for obj in response['Contents']:
                # Verificar si el archivo es más antiguo que 30 días
                if obj['LastModified'].replace(tzinfo=None) < cutoff_date:
                    file_key = obj['Key']
                    file_size = obj['Size']
                    
                    # Eliminar archivo
                    s3_client.delete_object(Bucket=bucket_name, Key=file_key)
                    
                    deleted_count += 1
                    total_size += file_size
                    
                    print(f"   🗑️  Eliminado: {file_key} ({file_size / 1024:.2f} KB)")
            
            if deleted_count > 0:
                print(f"\n   ✅ Archivos eliminados: {deleted_count}")
                print(f"   ✅ Espacio liberado: {total_size / (1024 * 1024):.2f} MB")
            else:
                print("   ℹ️  No hay archivos antiguos (>30 días) para eliminar.")

        # ============================================================
        # RESUMEN FINAL
        # ============================================================
        print("\n" + "=" * 80)
        print("✅ CONSOLIDACIÓN COMPLETADA EXITOSAMENTE")
        print("=" * 80)
        
        print(f"\n📊 RESUMEN:")
        print(f"   • Registros leídos de streaming: {count_streaming:,}")
        print(f"   • Duplicados eliminados: {duplicates_removed:,}")
        print(f"   • Registros consolidados en batch: {df_deduplicated.count():,}")
        print(f"   • Archivos antiguos eliminados: {deleted_count if 'deleted_count' in locals() else 0}")
        
        print("\n💡 PRÓXIMO PASO:")
        print("   Los datos consolidados en batch están listos para ETL Bronze → Silver")
        
        print("\n" + "=" * 80)

    except Exception as e:
        print(f"\n❌ ERROR EN LA CONSOLIDACIÓN:")
        print(f"   {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()