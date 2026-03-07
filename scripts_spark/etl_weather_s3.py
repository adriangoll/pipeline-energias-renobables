import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

def main():
    spark = SparkSession.builder \
        .appName("ETL_Energias_Renovables") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "com.amazonaws.auth.InstanceProfileCredentialsProvider") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        print("=" * 70)
        print("INICIANDO ETL: BRONZE a SILVER")
        print("=" * 70)

        print("\nPASO 1: Extrayendo datos de S3 Bronze...")
        
        df_riohacha = spark.read.option("multiline", "true") \
            .json("s3a://datalake-energias-renovables-dev/bronze/batch/historicos/riohacha_2024_2025.json")
        
        df_patagonia = spark.read.option("multiline", "true") \
            .json("s3a://datalake-energias-renovables-dev/bronze/batch/historicos/patagonia_2024_2025.json")
        
        print(f"   Riohacha: {df_riohacha.count():,} registros")
        print(f"   Patagonia: {df_patagonia.count():,} registros")

        print("\nPASO 2: Normalizando esquemas...")
        
        df_riohacha_norm = df_riohacha.select(
            col("dt").cast("long"),
            col("dt_iso").cast("string"),
            lit("Riohacha").alias("city"),
            col("lat").cast("double"),
            col("lon").cast("double"),
            col("main.temp").cast("double").alias("temp"),
            col("main.temp_min").cast("double").alias("temp_min"),
            col("main.temp_max").cast("double").alias("temp_max"),
            col("main.feels_like").cast("double").alias("feels_like"),
            col("main.pressure").cast("integer").alias("pressure"),
            col("main.humidity").cast("integer").alias("humidity"),
            col("clouds.all").cast("integer").alias("clouds"),
            col("wind.speed").cast("double").alias("wind_speed"),
            col("wind.deg").cast("integer").alias("wind_deg"),
            col("uv_index").cast("double").alias("uv_index")
        )
        
        df_patagonia_norm = df_patagonia.select(
            col("dt").cast("long"),
            col("dt_iso").cast("string"),
            lit("Patagonia").alias("city"),
            col("lat").cast("double"),
            col("lon").cast("double"),
            col("main.temp").cast("double").alias("temp"),
            col("main.temp_min").cast("double").alias("temp_min"),
            col("main.temp_max").cast("double").alias("temp_max"),
            col("main.feels_like").cast("double").alias("feels_like"),
            col("main.pressure").cast("integer").alias("pressure"),
            col("main.humidity").cast("integer").alias("humidity"),
            col("clouds.all").cast("integer").alias("clouds"),
            col("wind.speed").cast("double").alias("wind_speed"),
            col("wind.deg").cast("integer").alias("wind_deg"),
            col("uv_index").cast("double").alias("uv_index")
        )

        print("   Esquemas normalizados")
        
        print("\nPASO 3: Uniendo datasets...")
        df_final = df_riohacha_norm.union(df_patagonia_norm)
        
        total_rows = df_final.count()
        print(f"   Total de registros unificados: {total_rows:,}")
        
        assert total_rows > 0, "ERROR: DataFrame vacio"
        
        print("\nMUESTRA DE DATOS (5 registros):")
        df_final.show(5, truncate=False)

        print("\nPASO 4: Escribiendo a S3 Silver (formato Parquet)...")
        
        output_path = "s3a://datalake-energias-renovables-dev/silver/weather_unified/"
        
        df_final.write \
            .mode("overwrite") \
            .partitionBy("city") \
            .parquet(output_path)
        
        print(f"   Datos escritos en: {output_path}")
        
        print("\n" + "=" * 70)
        print("ETL COMPLETADO EXITOSAMENTE")
        print("=" * 70)

    except Exception as e:
        print(f"\nERROR CRITICO EN EL PIPELINE:")
        print(f"   {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()