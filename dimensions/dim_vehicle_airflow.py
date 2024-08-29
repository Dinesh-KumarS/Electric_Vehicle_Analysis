import pyspark
import sys
import logging
import yaml
from pyspark.sql import SparkSession

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)

# Load configuration from YAML file
logging.info("Reading Configuration File.!!")
with open('/home/dinesh/EV_Analysis/config/config.yaml', 'r') as f:
    CONFIG = yaml.safe_load(f)

spark = SparkSession.builder \
    .appName("dim_vehicle") \
    .config("spark.jars.packages", 
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "org.apache.hadoop:hadoop-common:3.3.4,"
            "org.postgresql:postgresql:42.7.3")\
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", CONFIG['s3_access_key']) \
    .config("spark.hadoop.fs.s3a.secret.key", CONFIG['s3_secret_key']) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.postgresql.jdbc.socket.timeout", "600") \
    .config("spark.postgresql.jdbc.connection.timeout", "600") \
    .config("spark.datasource.postgres.connection.pool.enabled", "true") \
    .config("spark.datasource.postgres.connection.pool.maxSize", "10") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

def dim_vehicle(source_path):
    
    logging.info("Reading Source File.!!")
    raw_data = spark.read.format("csv").option("header", "True").load(source_path)

    logging.info("Applying Transformation Logic")
    raw_vehicle = raw_data \
              .select("VIN (1-10)", "Make", "Model", "Model Year", "Electric Vehicle Type", "Clean Alternative Fuel Vehicle (CAFV) Eligibility") \
              .withColumnRenamed("VIN (1-10)", "VIN") \
              .withColumnRenamed("Clean Alternative Fuel Vehicle (CAFV) Eligibility", "CAFV") \
              .withColumnRenamed("Model Year", "Model_Year") \
              .withColumnRenamed("Electric Vehicle Type", "Electric_Vehicle_Type") \
              .distinct()
    raw_vehicle.createOrReplaceTempView("raw_vehicle_data")

    cleaned_vehicle = spark.sql("""
    SELECT
        CAST(VIN AS String) AS VIN
        , CAST(Make AS String) AS Make
        , CAST(Model_Year AS Integer) AS Model_Year
        , CAST(Electric_Vehicle_Type AS String) AS Electric_Vehicle_Type
        , CAST(CASE
            WHEN CAFV = 'Clean Alternative Fuel Vehicle Eligible' THEN 1
            ELSE 0
            END AS BOOLEAN) AS CAFV
    FROM raw_vehicle_data
    """)

    logging.info("Saving data to Target File.!!")
    cleaned_vehicle.write \
    .format("jdbc") \
    .option("url", CONFIG['url']) \
    .option("dbtable", "dimension.dim_vehicle") \
    .option("user", CONFIG['user']) \
    .option("password", CONFIG['password']) \
    .option("driver", CONFIG['driver']) \
    .mode("overwrite") \
    .save()
    logging.info("Data written successfully")

def main():
    source_path = sys.argv[1]
    dim_vehicle(source_path)

if __name__ == '__main__':
    main()