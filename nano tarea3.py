# Importamos las librerías necesarias
from pyspark.sql import SparkSession, functions as F

# Inicializa la sesión de Spark
spark = SparkSession.builder.appName("Tarea3").getOrCreate()

# Define la ruta del archivo CSV en HDFS
file_path = 'hdfs://localhost:9000/Tarea3/depymun.csv'

# Lee el archivo CSV
df = spark.read.format('csv').option('header', 'true').option('inferSchema', 't>

# Imprime el esquema del DataFrame
print("Esquema del DataFrame:")
df.printSchema()

# Muestra las primeras 10 filas del DataFrame
print("\nPrimeras 10 filas:")
df.show(10, truncate=False)

# Estadísticas básicas
print("\nResumen estadístico:")
df.summary().show(truncate=False)

# Filtra las filas donde el estrato es igual a 1 y selecciona las columnas requ>
print("\nDatos donde el estrato es igual a 1:")
estrato_1 = df.filter(F.col("estrato") == 1).select('empresa', 'municipios', 'e>
estrato_1.show(10, truncate=False)

# Ordenar las filas por los valores en la columna "empresa" en orden ascendente
print("\nDatos ordenados por EMPRESA en orden ascendente:")
sorted_df = estrato_1.sort(F.col('empresa').asc())
sorted_df.show(10, truncate=False)
