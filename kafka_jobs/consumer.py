import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import from_json, col, when, lit, round

from configparser import ConfigParser

# Load configuration from config.ini
config = ConfigParser()
config.read('config.ini')

# Kafka configuration
TOPIC = config.get('kafka', 'topic')
BOOTSTRAP_SERVERS = config.get('kafka', 'bootstrap_servers')

# PostgreSQL configuration
URL = config.get('postgresql', 'postgresql_url')
TABLE = config.get('postgresql', 'postgresql_table')
DRIVER = config.get('postgresql', 'postgresql_driver')
USER = config.get('postgresql', 'postgresql_user')
PWD = config.get('postgresql', 'postgresql_pwd')


# Function to write data to PostgreSQL
def write_to_postgresql(df, epoch_id):
    df.write \
        .format('jdbc') \
        .option('url', URL) \
        .option('driver', DRIVER) \
        .option('dbtable', TABLE) \
        .option('user', USER) \
        .option('password', PWD) \
        .mode('append') \
        .save()


if __name__ == "__main__":
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Real Estate Kafka Consumer") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Define Kafka streaming source
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("subscribe", TOPIC) \
        .option("startingOffsets", "latest") \
        .load()

    # Define schema for real estate sales data
    real_estate_schema = StructType([
        StructField("Serial Number", StringType(), True),
        StructField("List Year", StringType(), True),
        StructField("Date Recorded", StringType(), True),
        StructField("Town", StringType(), True),
        StructField("Address", StringType(), True),
        StructField("Assessed Value", DoubleType(), True),
        StructField("Sale Amount", DoubleType(), True),
        StructField("Sales Ratio", DoubleType(), True),
        StructField("Property Type", StringType(), True),
        StructField("Residential Type", StringType(), True)
    ])

    # Parse Kafka messages
    parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), real_estate_schema).alias("data")) \
        .select("data.*")

    # Clean and process the data
    processed_stream = parsed_stream \
        .withColumn("Date Recorded", col("Date Recorded").cast(TimestampType())) \
        .withColumn("Assessed Value", when(col("Assessed Value").isNull(), lit(0)).otherwise(col("Assessed Value"))) \
        .withColumn("Sale Amount", when(col("Sale Amount").isNull(), lit(0)).otherwise(col("Sale Amount"))) \
        .withColumn("Sales Ratio", when(col("Sales Ratio").isNull(), lit(0)).otherwise(col("Sales Ratio"))) \
        .withColumn("Total Sale Value", round(col("Sale Amount") * col("Sales Ratio"), 2))

    # Define streaming sink for PostgreSQL
    postgresql_stream = processed_stream.writeStream \
        .trigger(processingTime="10 seconds") \
        .outputMode("append") \
        .foreachBatch(write_to_postgresql) \
        .start()

    # Optional: Write to console for debugging
    console_stream = processed_stream.writeStream \
        .trigger(processingTime="10 seconds") \
        .outputMode("append") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    # Await termination
    spark.streams.awaitAnyTermination()
