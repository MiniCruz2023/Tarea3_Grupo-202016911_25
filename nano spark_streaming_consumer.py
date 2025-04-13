from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, IntegerType, DoubleType, TimestampType

# Esquema de los datos del sensor
schema = StructType() \
    .add("sensor_id", IntegerType()) \
    .add("timestamp", DoubleType()) \
    .add("value", DoubleType())

# Crear sesión de Spark
spark = SparkSession.builder \
    .appName("KafkaSensorConsumer") \
    .master("local[*]") \
    .getOrCreate()

# Nivel de log
spark.sparkContext.setLogLevel("WARN")
# Leer datos desde Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor_data") \
    .option("startingOffsets", "latest") \
    .load()

# Convertir el valor (JSON) a columnas reales
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Mostrar en consola los datos
query = json_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()