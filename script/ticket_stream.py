from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import StructType, StringType, IntegerType

# --- Spark Session ---
spark = (
    SparkSession.builder
    .appName("ClientTicketsStreaming")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8")
    .getOrCreate()
)

# --- Schéma des tickets ---
schema = StructType() \
    .add("ticket_id", IntegerType()) \
    .add("client_id", IntegerType()) \
    .add("datetime_creation", StringType()) \
    .add("demande", StringType()) \
    .add("type_demande", StringType()) \
    .add("priorite", StringType())

# --- Lecture du topic Kafka ---
df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "172.17.224.1:9092")
    .option("subscribe", "client_tickets")
    .load()
)

# --- Parsing JSON ---
json_df = (
    df.selectExpr("CAST(value AS STRING)")
      .select(from_json(col("value"), schema).alias("data"))
      .select("data.*")
)

# --- Ajout automatique de l'équipe de support ---
json_df = json_df.withColumn(
    "equipe_support",
    when(col("type_demande") == "Incident", "Support N1")
    .when(col("type_demande") == "Problème technique", "Support Technique")
    .when(col("type_demande") == "Maintenance", "Equipe Maintenance")
    .when(col("type_demande") == "Demande d'information", "Support Client")
    .otherwise("Support Général")
)

# --- Fonction d'écriture batch vers SQLite ---
def write_to_sqlite(batch_df, batch_id):
    batch_df.write \
        .format("jdbc") \
        .option("url", "jdbc:sqlite:/mnt/C:\Users\noela\Desktop\Mes projets\Data\Modélisez une infrastructure dans le cloud\database/data.db") \
        .option("dbtable", "tickets") \
        .option("driver", "org.sqlite.JDBC") \
        .mode("append") \
        .save()

# --- Écriture en streaming via foreachBatch ---
query = (
    json_df.writeStream
    .foreachBatch(write_to_sqlite)
    .option("checkpointLocation", "../checkpoint/")
    .start()
)

query.awaitTermination()
