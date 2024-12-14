from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import os

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("Generalized Preprocess Data for PostgreSQL") \
    .config("spark.jars", "/path/to/postgresql-42.7.4.jar") \
    .getOrCreate()

# HDFS directory containing the files
hdfs_directory = "hdfs://namenode:9000/user/data/"
jdbc_url = "jdbc:postgresql://postgresDB:5432/postgres"
db_properties = {
    "user": "your_user",
    "password": "your_password",
    "driver": "org.postgresql.Driver"
}

# Step 1: List all files in the HDFS directory
try:
    hdfs_files = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration()) \
        .listStatus(spark._jvm.org.apache.hadoop.fs.Path(hdfs_directory))

    file_paths = [file.getPath().toString() for file in hdfs_files if file.isFile()]

except Exception as e:
    print(f"Error reading files from HDFS: {e}")
    spark.stop()
    exit(1)

# Step 2: Preprocess each file and load it into PostgreSQL
for file_path in file_paths:
    try:
        # Extract table name from file name
        table_name = os.path.basename(file_path).split(".")[0]  # Remove extension to get table name

        print(f"Processing file: {file_path} -> Table: {table_name}")

        # Read file into a DataFrame
        df = spark.read.csv(file_path, header=True, inferSchema=True)

        # Preprocessing steps (modify as needed)
        # Example: Remove columns, drop nulls, filter rows
        columns_to_keep = [col for col in df.columns if col != "unwanted_column"]
        df = df.select(*columns_to_keep)
        df = df.dropna()
        df = df.filter(col("some_column") >= 0)

        # Show DataFrame for debugging (optional)
        df.show(5)
        df.printSchema()

        # Write DataFrame to PostgreSQL
        df.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=db_properties)
        print(f"Successfully processed and loaded {file_path} into {table_name}")

    except Exception as e:
        print(f"Error processing file {file_path}: {e}")

# Stop SparkSession
spark.stop()
