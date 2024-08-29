from datetime import datetime
from airflow import DAG
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator

with DAG(
    "ev_population_pipeline",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=["ev_population"],
) as dag:

    dim_utility_job = SparkSubmitOperator(
        task_id="dim_utility_job",
        application="dimensions/dim_utility_airflow.py",
        conn_id="spark_default",
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.hadoop:hadoop-common:3.3.4,org.postgresql:postgresql:42.7.3",
        conf={
            "spark.master": "local[3]",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.sql.repl.eagerEval.enabled": True,
            "spark.sql.repl.eagerEval.maxNumRows": '10',
            "spark.sql.debug.maxToStringFields": '20',
            "spark.network.maxFrameSizeBytes": "1000000000",
            "spark.rpc.message.maxSize": "1024",
            "spark.sql.files.maxPartitionBytes": "134217728",
        },
        executor_memory='2g',
        driver_memory='2g',
        application_args=[
            "s3a://ev-raw-bucket/raw_files/Electric_Vehicle_Population_Data.csv"
        ],
        verbose=True,
    )

    dim_vehicle_job = SparkSubmitOperator(
        task_id="dim_vehicle_job",
        application="dimensions/dim_vehicle_airflow.py",
        conn_id="spark_default",
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.hadoop:hadoop-common:3.3.4,org.postgresql:postgresql:42.7.3",
        conf={
            "spark.master": "local[3]",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.sql.repl.eagerEval.enabled": True,
            "spark.sql.repl.eagerEval.maxNumRows": '10',
            "spark.sql.debug.maxToStringFields": '20',
            "spark.network.maxFrameSizeBytes": "1000000000",
            "spark.rpc.message.maxSize": "1024",
            "spark.sql.files.maxPartitionBytes": "134217728",
        },
        executor_memory='2g',
        driver_memory='2g',
        application_args=[
            "s3a://ev-raw-bucket/raw_files/Electric_Vehicle_Population_Data.csv"
        ],
        verbose=True,
    )

    dim_location_job = SparkSubmitOperator(
        task_id="dim_location_job",
        application="dimensions/dim_location_airflow.py",
        conn_id="spark_default",
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.hadoop:hadoop-common:3.3.4,org.postgresql:postgresql:42.7.3",
        conf={
            "spark.master": "local[3]",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.sql.repl.eagerEval.enabled": True,
            "spark.sql.repl.eagerEval.maxNumRows": '10',
            "spark.sql.debug.maxToStringFields": '20',
            "spark.network.maxFrameSizeBytes": "1000000000",
            "spark.rpc.message.maxSize": "1024",
            "spark.sql.files.maxPartitionBytes": "134217728",
        },
        executor_memory='1g',
        driver_memory='1g',
        application_args=[
            "s3a://ev-raw-bucket/raw_files/Electric_Vehicle_Population_Data.csv"
        ],
        verbose=True,
    )

    fct_ev_attributes_job = SparkSubmitOperator(
        task_id="fct_ev_attributes_job",
        application="facts/fct_ev_attributes_airflow.py",
        conn_id="spark_default",
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262,org.apache.hadoop:hadoop-common:3.3.4,org.postgresql:postgresql:42.7.3",
        conf={
            "spark.master": "local[3]",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.sql.repl.eagerEval.enabled": True,
            "spark.sql.repl.eagerEval.maxNumRows": '10',
            "spark.sql.debug.maxToStringFields": '20',
            "spark.network.maxFrameSizeBytes": "1000000000",
            "spark.rpc.message.maxSize": "1024",
            "spark.sql.files.maxPartitionBytes": "134217728",
        },
        executor_memory='2g',
        driver_memory='2g',
        application_args=[
            "s3a://ev-raw-bucket/raw_files/Electric_Vehicle_Population_Data.csv"
        ],
        verbose=True,
    )

    [dim_utility_job, dim_vehicle_job, dim_location_job] >> fct_ev_attributes_job
