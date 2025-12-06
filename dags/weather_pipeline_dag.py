from __future__ import annotations
import pendulum
import pandas as pd
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

S3_BUCKET_NAME = "data-pipeline-weather-project-2025-12"
RAW_S3_KEY = "raw/weather_data_20251202.csv"
STAGING_S3_PATH = f"s3://{S3_BUCKET_NAME}/staging/processed/"
TRANSFORM_SCRIPT_PATH = "/opt/airflow/scripts/weather_transform.py"
SNOWFLAKE_CONN_ID = "snowflake_default"
AWS_CONN_ID = "aws_default"
SPARK_CONN_ID = "spark_default"
SNOWFLAKE_TABLE = "WEATHER_DATA_PROCESSED"

CREATE_TABLE_SQL = f"""
    CREATE TABLE IF NOT EXISTS {SNOWFLAKE_TABLE} (
        city VARCHAR,
        temperature FLOAT,
        timestamp TIMESTAMP_NTZ,
        processing_time TIMESTAMP_NTZ
    );
"""

def _extract_and_load_to_s3(s3_bucket, s3_key, aws_conn_id):
    print(f"S3 버킷: {s3_bucket}, 키: {s3_key}로 데이터 업로드 시도...")
    data = {
        'city': ['Seoul', 'Tokyo', 'New York'],
        'temperature': [10.5, 15.2, 5.0],
        'timestamp': [pendulum.now().to_datetime_string()] * 3
    }
    df = pd.DataFrame(data)
    temp_csv_path = "/tmp/temp_weather.csv"
    df.to_csv(temp_csv_path, index=False)
    
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    s3_hook.load_file(
        filename=temp_csv_path,
        key=s3_key,
        bucket_name=s3_bucket,
        replace=True
    )
    print("S3 업로드 완료!")
    return f"s3://{s3_bucket}/{s3_key}"

with DAG(
    dag_id="spark_weather_pipeline_v2",
    start_date=pendulum.datetime(2025, 12, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["spark", "emr", "snowflake", "etl"],
) as dag:
    
    create_snowflake_table = SQLExecuteQueryOperator(
        task_id="create_snowflake_table_if_not_exists",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=CREATE_TABLE_SQL,
    )
    
    extract_and_load_to_s3_task = PythonOperator(
        task_id="extract_and_load_to_s3",
        python_callable=_extract_and_load_to_s3,
        op_kwargs={
            "s3_bucket": S3_BUCKET_NAME,
            "s3_key": RAW_S3_KEY,
            "aws_conn_id": AWS_CONN_ID,
        }
    )
    
    spark_transform_data = SparkSubmitOperator(
        task_id="spark_transform_data",
        application=TRANSFORM_SCRIPT_PATH,
        conn_id=SPARK_CONN_ID,
        name="weather-spark-transform",
        conf={
            "spark.driver.memory": "2g",
            "spark.executor.memory": "4g",
            "spark.hadoop.fs.s3a.connection.timeout": "60000",
            "spark.hadoop.fs.s3a.socket.timeout": "60000",
            "spark.hadoop.fs.s3a.access.key": "{{ conn.aws_default.login }}",
            "spark.hadoop.fs.s3a.secret.key": "{{ conn.aws_default.password }}",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
        },
        verbose=True,
    )
    
    copy_to_snowflake = SQLExecuteQueryOperator(
        task_id="copy_s3_to_snowflake",
        conn_id=SNOWFLAKE_CONN_ID,
        sql="""
            COPY INTO WEATHER_DATA_PROCESSED
            FROM '{{ params.staging_path }}'
            CREDENTIALS = (
                AWS_KEY_ID = '{{ conn.aws_default.login }}'
                AWS_SECRET_KEY = '{{ conn.aws_default.password }}'
            )
            FILE_FORMAT = (TYPE = PARQUET)
	    MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
	    ON_ERROR = CONTINUE;
        """,
	params={
            "staging_path": STAGING_S3_PATH
        }
    )
    
    create_snowflake_table >> extract_and_load_to_s3_task >> spark_transform_data >> copy_to_snowflake


