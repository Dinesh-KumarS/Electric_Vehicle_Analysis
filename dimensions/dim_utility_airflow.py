import logging
import pyspark
import sys
import yaml
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, row_number

logging.basicConfig(format='%(asctime)s %(message)s', datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)

# Load configuration from YAML file
logging.info("Reading Configuration File.!!")
with open('/home/dinesh/EV_Analysis/config/config.yaml', 'r') as f:
    CONFIG = yaml.safe_load(f)

spark = SparkSession.builder \
    .appName("dim_vehicle") \
    .config("spark.jars.packages", 
            "org.apache.hadoop:hadoop-aws:3.3.4,"  # Hadoop AWS package
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

def dim_electric_utility(source_path):
    
    logging.info("Reading Source File.!!")
    raw_data = spark.read.format("csv").option("header", "True").load(source_path)

    logging.info("Applying Transformation Logic")
    cleaned_data = raw_data \
            .select("Electric Utility") \
            .where("`Electric Utility` IS NOT NULL") \
            .withColumnRenamed("Electric Utility", "Electric_Utility") \
            .distinct()

    w = Window().orderBy(col("Electric_Utility"))
    cleaned_utility = cleaned_data \
                    .withColumn("Utility_Number", row_number().over(w)) \
                    .select("Utility_Number", "Electric_Utility")

    logging.info("Saving data to Target File.!!")
    cleaned_utility.write \
    .format("jdbc") \
    .option("url", CONFIG['url']) \
    .option("dbtable", "dimension.dim_electric_utility") \
    .option("user", CONFIG['user']) \
    .option("password", CONFIG['password']) \
    .option("driver", CONFIG['driver']) \
    .mode("overwrite") \
    .save()
    logging.info("Data written successfully")

def main():
    source_path = sys.argv[1]
    dim_electric_utility(source_path)

if __name__ == '__main__':
    main()