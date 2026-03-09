import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, year, month, dayofmonth
from pyspark.sql.types import StructType, StructField, DoubleType, LongType, IntegerType, StringType, ArrayType

def main():
    spark = SparkSession.builder \
        .appName("KafkaToS3_WeatherStream") \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", 
                "com.amazonaws.auth.InstanceProfileCredentialsProvider") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    
    print("=" * 80)
    print("🚀 SPARK STRUCTURED STREAMING INICIADO")
    print("=" * 80)
    
    esquema_weather = StructType([
        StructField("coord", StructType([
            StructField("lon", DoubleType()),
            StructField("lat", DoubleType())
        ])),
        StructField("main", StructType([
            StructField("temp", DoubleType()),
            StructField("humidity", IntegerType()),
            StructField("pressure", IntegerType())
        ])),
        StructField("wind", StructType([
            StructField("speed", DoubleType()),
            StructField("deg", IntegerType())
        ])),
        StructField("clouds", StructType([
            StructField("all", IntegerType())
        ])),
        StructField("dt", LongType()),
        StructField("name", StringType())
    ])

    print(">>> Conectando a Kafka...")
    
    KAFKA_BROKER = "172.31.65.239:9092"  #  IP privada de Kafka
    TOPICS = "weather_riohacha,weather_patagonia"
    
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", TOPICS) \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "false") \
        .load()

    print(f">>> Conexión a Kafka exitosa: {KAFKA_BROKER}")
    
    df_json = df_kafka.selectExpr("CAST(value AS STRING) as json_string", "topic")
    
    df_parsed = df_json.select(
        from_json(col("json_string"), esquema_weather).alias("data"),
        col("topic")
    )

    df_final = df_parsed.select(
        col("data.dt").alias("dt"),
        col("data.coord.lat").alias("lat"),
        col("data.coord.lon").alias("lon"),
        col("data.main.temp").alias("temp"),
        col("data.main.humidity").alias("humidity"),
        col("data.wind.speed").alias("wind_speed"),
        col("data.clouds.all").alias("clouds"),
        col("data.name").alias("city_name"),
        col("topic"),
        current_timestamp().alias("ingestion_timestamp")
    )
    
    df_with_partitions = df_final \
        .withColumn("ingestion_date", col("ingestion_timestamp").cast("date")) \
        .withColumn("year", year(col("ingestion_date"))) \
        .withColumn("month", month(col("ingestion_date"))) \
        .withColumn("day", dayofmonth(col("ingestion_date")))

    print(">>> Configurando escritura a S3...")
    
    S3_PATH = "s3a://datalake-energias-renovables-dev/bronze/streaming/openweather/"
    CHECKPOINT_PATH = "s3a://datalake-energias-renovables-dev/checkpoints/weather_stream/"
    
    query = df_with_partitions.writeStream \
        .outputMode("append") \
        .format("parquet") \
        .option("path", S3_PATH) \
        .option("checkpointLocation", CHECKPOINT_PATH) \
        .partitionBy("topic", "year", "month", "day") \
        .trigger(processingTime="1 minute") \
        .start()

    print(">>> Streaming activo ✅")
    print("=" * 80)
    
    query.awaitTermination()

if __name__ == "__main__":
    main()