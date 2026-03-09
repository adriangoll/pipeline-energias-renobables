"""
ETL Silver → Gold: Generación de Tablas Analíticas
Pipeline ETLT - Energías Renovables

Este script lee datos limpios de la capa Silver y genera tablas agregadas
optimizadas para responder las preguntas de negocio del proyecto.

Preguntas de Negocio:
1. ¿Cómo varía el potencial solar a lo largo del día y del mes?
2. ¿Qué patrones históricos se observan en el potencial eólico?
3. ¿Condiciones climáticas asociadas con reducciones en potencial renovable?
4. ¿Cómo se comportan las predicciones vs observaciones?
5. ¿Días con mayor y menor potencial energético?
"""

import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, hour, dayofmonth, month, year, date_format,
    avg, max, min, sum, count, round as spark_round,
    when, lit, row_number
)
from pyspark.sql.window import Window

def main():
    # Configuración de Spark
    spark = SparkSession.builder \
        .appName("ETL_Silver_to_Gold") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "com.amazonaws.auth.InstanceProfileCredentialsProvider") \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    try:
        print("=" * 80)
        print("🚀 INICIANDO ETL: SILVER → GOLD")
        print("=" * 80)

        # ============================================================
        # EXTRACCIÓN: Leer datos de Silver
        # ============================================================
        print("\n📂 PASO 1: Leyendo datos de Silver...")
        
        silver_path = "s3a://datalake-energias-renovables-dev/silver/weather_unified/"
        df_silver = spark.read.parquet(silver_path)
        
        print(f"   ✅ Registros leídos: {df_silver.count():,}")
        
        # Agregar columnas de tiempo derivadas
        df_silver = df_silver \
            .withColumn("timestamp", col("dt").cast("timestamp")) \
            .withColumn("year", year(col("timestamp"))) \
            .withColumn("month", month(col("timestamp"))) \
            .withColumn("day", dayofmonth(col("timestamp"))) \
            .withColumn("hour", hour(col("timestamp"))) \
            .withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd"))

        # ============================================================
        # TABLA 1: Potencial Solar (Pregunta 1)
        # ============================================================
        print("\n🌞 PASO 2: Generando tabla 'potencial_solar'...")
        
        """
        Estima el potencial solar basado en:
        - UV Index (indicador directo de radiación)
        - Nubosidad (reduce radiación disponible)
        - Hora del día (radiación mayor al mediodía)
        
        Fórmula simplificada:
        Potencial Solar = UV Index * (1 - nubosidad/100) * factor_hora
        """
        
        df_potencial_solar = df_silver \
            .withColumn("factor_hora", 
                when((col("hour") >= 6) & (col("hour") <= 18), 
                     1.0 - abs(col("hour") - 12) / 12.0)
                .otherwise(0.0)
            ) \
            .withColumn("potencial_solar_kwh_m2", 
                spark_round(
                    col("uv_index") * (1.0 - col("clouds") / 100.0) * col("factor_hora"),
                    2
                )
            ) \
            .groupBy("city", "year", "month", "day", "hour") \
            .agg(
                avg("potencial_solar_kwh_m2").alias("potencial_solar_promedio"),
                max("potencial_solar_kwh_m2").alias("potencial_solar_maximo"),
                avg("uv_index").alias("uv_index_promedio"),
                avg("clouds").alias("nubosidad_promedio"),
                avg("temp").alias("temperatura_promedio")
            ) \
            .orderBy("city", "year", "month", "day", "hour")
        
        # Escribir a Gold
        gold_solar_path = "s3a://datalake-energias-renovables-dev/gold/potencial_solar/"
        df_potencial_solar.write \
            .mode("overwrite") \
            .partitionBy("city", "year", "month") \
            .parquet(gold_solar_path)
        
        print(f"   ✅ Tabla 'potencial_solar' creada: {df_potencial_solar.count():,} registros")

        # ============================================================
        # TABLA 2: Potencial Eólico (Pregunta 2)
        # ============================================================
        print("\n💨 PASO 3: Generando tabla 'potencial_eolico'...")
        
        """
        Estima el potencial eólico basado en:
        - Velocidad del viento (potencia ∝ velocidad³)
        - Densidad del aire (función de presión y temperatura)
        
        Fórmula: P = 0.5 × ρ × A × v³
        Donde:
        - ρ = densidad del aire ≈ P / (R × T)
        - P = presión atmosférica
        - T = temperatura en Kelvin
        - v = velocidad del viento
        - A = área del rotor (asumimos 1 m² para comparación)
        """
        
        df_potencial_eolico = df_silver \
            .withColumn("temp_kelvin", col("temp") + 273.15) \
            .withColumn("densidad_aire", 
                col("pressure") * 100.0 / (287.05 * col("temp_kelvin"))
            ) \
            .withColumn("potencial_eolico_w", 
                spark_round(
                    0.5 * col("densidad_aire") * col("wind_speed") * col("wind_speed") * col("wind_speed"),
                    2
                )
            ) \
            .groupBy("city", "year", "month", "day") \
            .agg(
                avg("potencial_eolico_w").alias("potencial_eolico_promedio_w"),
                max("potencial_eolico_w").alias("potencial_eolico_maximo_w"),
                avg("wind_speed").alias("velocidad_viento_promedio_ms"),
                max("wind_speed").alias("velocidad_viento_maxima_ms"),
                avg("wind_deg").alias("direccion_viento_promedio"),
                count("*").alias("mediciones")
            ) \
            .orderBy("city", "year", "month", "day")
        
        # Escribir a Gold
        gold_eolico_path = "s3a://datalake-energias-renovables-dev/gold/potencial_eolico/"
        df_potencial_eolico.write \
            .mode("overwrite") \
            .partitionBy("city", "year", "month") \
            .parquet(gold_eolico_path)
        
        print(f"   ✅ Tabla 'potencial_eolico' creada: {df_potencial_eolico.count():,} registros")

        # ============================================================
        # TABLA 3: Condiciones Climáticas Críticas (Pregunta 3)
        # ============================================================
        print("\n⚠️  PASO 4: Generando tabla 'condiciones_criticas'...")
        
        """
        Identifica condiciones que reducen significativamente el potencial renovable:
        - Alta nubosidad (>80%) → reduce solar
        - Baja velocidad viento (<3 m/s) → reduce eólico
        - Alta humedad + lluvia → condiciones adversas
        """
        
        df_condiciones_criticas = df_silver \
            .withColumn("reduccion_solar", 
                when(col("clouds") > 80, lit("Alta nubosidad"))
                .when(col("uv_index") < 2, lit("UV bajo"))
                .otherwise(lit("Normal"))
            ) \
            .withColumn("reduccion_eolica", 
                when(col("wind_speed") < 3.0, lit("Viento insuficiente"))
                .when(col("wind_speed") > 25.0, lit("Viento excesivo (peligro)"))
                .otherwise(lit("Normal"))
            ) \
            .withColumn("condicion_adversa",
                when((col("humidity") > 85) & (col("clouds") > 80), lit("Lluvia probable"))
                .when(col("temp") > 40, lit("Calor extremo"))
                .when(col("temp") < 0, lit("Congelamiento"))
                .otherwise(lit("Normal"))
            ) \
            .groupBy("city", "year", "month", "reduccion_solar", "reduccion_eolica", "condicion_adversa") \
            .agg(
                count("*").alias("ocurrencias"),
                avg("temp").alias("temperatura_promedio"),
                avg("humidity").alias("humedad_promedio"),
                avg("clouds").alias("nubosidad_promedio"),
                avg("wind_speed").alias("viento_promedio")
            ) \
            .filter(
                (col("reduccion_solar") != "Normal") | 
                (col("reduccion_eolica") != "Normal") | 
                (col("condicion_adversa") != "Normal")
            ) \
            .orderBy("city", "year", "month", col("ocurrencias").desc())
        
        # Escribir a Gold
        gold_criticas_path = "s3a://datalake-energias-renovables-dev/gold/condiciones_criticas/"
        df_condiciones_criticas.write \
            .mode("overwrite") \
            .partitionBy("city", "year") \
            .parquet(gold_criticas_path)
        
        print(f"   ✅ Tabla 'condiciones_criticas' creada: {df_condiciones_criticas.count():,} registros")

        # ============================================================
        # TABLA 4: Análisis Comparativo Riohacha vs Patagonia (Pregunta 5)
        # ============================================================
        print("\n📊 PASO 5: Generando tabla 'analisis_comparativo'...")
        
        """
        Compara métricas clave entre ambas ubicaciones
        """
        
        # Calcular potencial solar y eólico para cada ciudad
        df_comparativo = df_silver \
            .withColumn("potencial_solar_estimado",
                col("uv_index") * (1.0 - col("clouds") / 100.0)
            ) \
            .withColumn("potencial_eolico_estimado",
                col("wind_speed") * col("wind_speed") * col("wind_speed") / 100.0
            ) \
            .groupBy("city", "year", "month") \
            .agg(
                # Métricas solares
                avg("potencial_solar_estimado").alias("potencial_solar_promedio"),
                max("uv_index").alias("uv_index_maximo"),
                avg("clouds").alias("nubosidad_promedio"),
                
                # Métricas eólicas
                avg("potencial_eolico_estimado").alias("potencial_eolico_promedio"),
                avg("wind_speed").alias("velocidad_viento_promedio"),
                max("wind_speed").alias("velocidad_viento_maxima"),
                
                # Métricas climáticas
                avg("temp").alias("temperatura_promedio"),
                avg("humidity").alias("humedad_promedio"),
                avg("pressure").alias("presion_promedio"),
                
                # Contadores
                count("*").alias("total_mediciones")
            ) \
            .orderBy("year", "month", "city")
        
        # Escribir a Gold
        gold_comparativo_path = "s3a://datalake-energias-renovables-dev/gold/analisis_comparativo/"
        df_comparativo.write \
            .mode("overwrite") \
            .partitionBy("year", "month") \
            .parquet(gold_comparativo_path)
        
        print(f"   ✅ Tabla 'analisis_comparativo' creada: {df_comparativo.count():,} registros")

        # ============================================================
        # TABLA 5: Ranking de Días (Pregunta 5)
        # ============================================================
        print("\n🏆 PASO 6: Generando tabla 'ranking_dias'...")
        
        """
        Identifica los mejores y peores días para generación de energía
        """
        
        # Calcular score combinado (solar + eólico) por día
        df_dias = df_silver \
            .groupBy("city", "date", "year", "month", "day") \
            .agg(
                avg("uv_index").alias("uv_promedio"),
                avg("wind_speed").alias("viento_promedio"),
                avg("clouds").alias("nubosidad_promedio"),
                avg("temp").alias("temperatura_promedio")
            ) \
            .withColumn("score_solar", 
                col("uv_promedio") * (1.0 - col("nubosidad_promedio") / 100.0)
            ) \
            .withColumn("score_eolico",
                col("viento_promedio") * col("viento_promedio") / 10.0
            ) \
            .withColumn("score_total",
                spark_round(col("score_solar") + col("score_eolico"), 2)
            )
        
        # Ranking de mejores días por ciudad
        window_top = Window.partitionBy("city").orderBy(col("score_total").desc())
        window_worst = Window.partitionBy("city").orderBy(col("score_total").asc())
        
        df_ranking = df_dias \
            .withColumn("rank_mejor", row_number().over(window_top)) \
            .withColumn("rank_peor", row_number().over(window_worst)) \
            .filter((col("rank_mejor") <= 10) | (col("rank_peor") <= 10)) \
            .withColumn("tipo_dia",
                when(col("rank_mejor") <= 10, lit("Top 10 Mejores"))
                .when(col("rank_peor") <= 10, lit("Top 10 Peores"))
            ) \
            .select(
                "city", "date", "year", "month", "day",
                "score_total", "score_solar", "score_eolico",
                "uv_promedio", "viento_promedio", "nubosidad_promedio",
                "temperatura_promedio", "tipo_dia", "rank_mejor", "rank_peor"
            ) \
            .orderBy("city", col("score_total").desc())
        
        # Escribir a Gold
        gold_ranking_path = "s3a://datalake-energias-renovables-dev/gold/ranking_dias/"
        df_ranking.write \
            .mode("overwrite") \
            .partitionBy("city", "year") \
            .parquet(gold_ranking_path)
        
        print(f"   ✅ Tabla 'ranking_dias' creada: {df_ranking.count():,} registros")

        # ============================================================
        # RESUMEN FINAL
        # ============================================================
        print("\n" + "=" * 80)
        print("✅ ETL SILVER → GOLD COMPLETADO EXITOSAMENTE")
        print("=" * 80)
        
        print("\n📊 TABLAS GENERADAS EN GOLD:")
        print(f"   1. potencial_solar     → Variación horaria/mensual del potencial solar")
        print(f"   2. potencial_eolico    → Patrones históricos de potencial eólico")
        print(f"   3. condiciones_criticas → Condiciones que reducen potencial renovable")
        print(f"   4. analisis_comparativo → Comparación Riohacha vs Patagonia")
        print(f"   5. ranking_dias        → Top 10 mejores/peores días por ciudad")
        
        print("\n💡 CONSUMO DE DATOS:")
        print("   Las tablas están optimizadas para herramientas de BI:")
        print("   - AWS QuickSight: Conectar a S3 con Athena")
        print("   - Streamlit: Leer Parquet directo con pandas/pyarrow")
        print("   - Superset: Conectar con Presto/Trino sobre S3")
        print("   - Power BI: Usar conector AWS S3 + Parquet")
        
        print("\n" + "=" * 80)

    except Exception as e:
        print(f"\n❌ ERROR CRÍTICO EN EL PIPELINE:")
        print(f"   {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
    
    finally:
        spark.stop()

if __name__ == "__main__":
    main()