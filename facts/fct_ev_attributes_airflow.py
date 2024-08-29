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

def fct_ev_attributes(raw_path):
    
    logging.info("Reading Source Files.!!")
    raw_data = spark.read.format("csv") \
            .option("header", "True") \
            .load(raw_path) \
            .select("VIN (1-10)", "DOL Vehicle ID", "Electric Range", "Base MSRP", "Electric Utility", "Legislative District", "Vehicle Location", "County", "City", "Postal Code") 

    dim_location = spark.read.format("jdbc") \
                    .option("url", CONFIG['url']) \
                    .option("dbtable", "dimension.dim_location") \
                    .option("user", CONFIG['user']) \
                    .option("password", CONFIG['password']) \
                    .option("driver", CONFIG['driver']) \
                    .load()
    
    dim_utility = spark.read.format("jdbc") \
                    .option("url", CONFIG['url']) \
                    .option("dbtable", "dimension.dim_electric_utility") \
                    .option("user", CONFIG['user']) \
                    .option("password", CONFIG['password']) \
                    .option("driver", CONFIG['driver']) \
                    .load()
    
    raw_data.createOrReplaceTempView("RAW_EV_POPULATION")
    dim_location.createOrReplaceTempView("DIM_LOCATION")
    dim_utility.createOrReplaceTempView("DIM_UTILITY")

    logging.info("Applying Transformation Logic")
    fact_ev_attributes = spark.sql("""
    SELECT 
        DISTINCT
        CAST(REVP.`DOL Vehicle ID` AS INT) AS DOL_Vehicle_ID
        , CAST(REVP.`VIN (1-10)` AS STRING) AS VIN
        , CAST(DL.Geography_ID AS INT) AS Geography_ID
        , CAST(DU.Utility_Number AS INT) AS Utility_Number
        , CAST(REVP.`Electric Range` AS INT) AS Electric_Range
        , CAST(REVP.`Base MSRP` AS INT) AS Base_MSRP
    FROM RAW_EV_POPULATION REVP
    LEFT JOIN DIM_LOCATION DL
        ON REVP.`Legislative District` = DL.Legislative_District
        AND REVP.`Vehicle Location` = DL.Vehicle_Location
        AND REVP.`Postal Code` = DL.Postal_Code
        AND REVP.City = DL.City
        AND REVP.County = DL.County
    LEFT JOIN DIM_UTILITY DU
        ON REVP.`Electric Utility` = DU.Electric_Utility
    """)

    logging.info("Saving data to Target File.!!")
    fact_ev_attributes.write \
    .format("jdbc") \
    .option("url", CONFIG['url']) \
    .option("dbtable", "fact.fct_ev_attributes") \
    .option("user", CONFIG['user']) \
    .option("password", CONFIG['password']) \
    .option("driver", CONFIG['driver']) \
    .mode("overwrite") \
    .save()
    logging.info("Data written successfully")

def main():
    raw_path = sys.argv[1]
    fct_ev_attributes(raw_path)

if __name__ == '__main__':
    main()