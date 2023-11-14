
# Tasks performed by this code
# import sys
# sys.path.append("C:/Users/SALA443/Desktop/Projetos/Dados_B3/CVM/airflow_env/Lib/")
# x = sys.path
# print(x)
# Import libraries
from tasks.extract import create_path_folder, storing_files,UnzipLandingToRaw,s3_upload_file,s3_upload_file_iterate_source
from airflow.decorators import dag, task
from datetime import datetime
from airflow.operators.empty import EmptyOperator
from airflow.models.baseoperator import chain
from astro import sql as aql
from astro.files import File
from astro.constants import FileType
from astro.sql.table import Table, Metadata

# Connections and variables
SNOWFLAKE_CONN_ID = "snowflake_default"
SOURCE_AWS_CONN_ID = "aws_default"
# Default args
default_args = {    
    "owner":"Okonomikus admin",
    "retries": 1,
    "retry_delay": 0    
}

# Init dag
@dag(
    dag_id="load_CVM_to_snowflake",
    start_date=datetime(2023,11,12),
    schedule="0,7 13 * * *",
    max_active_runs=1,
    catchup=False,
    max_active_tasks=1,
    default_args=default_args,
    owner_links={"linkedin":"https://www.linkedin.com/in/jose-carlos-da-silva-a9bb21182/" },
    tags=['astro_sdk','snowflake','load','from_consume_zone','okkus_pipelines_dfp']
)


# tasks
def load_files_snowflake():
    '''
    
    '''
    # init and finish
    init    = EmptyOperator(task_id="init")
    finish  = EmptyOperator(task_id="finish")
    
    # Load files into snowflake
    load_DFP_files = aql.load_file(
        task_id="load_DFP_files",
        input_file=File(path=f"s3://de-okkus-landing-dev-727477891012/2023/11/14/",filetype=FileType.CSV, conn_id=SOURCE_AWS_CONN_ID),
        output_table=Table(name="DFP_CVM",conn_id=SNOWFLAKE_CONN_ID,metadata=Metadata(schema="ASTRO_SDK_SCHEMA")),
        if_exists="replace",
        # is_incremental=False,
        use_native_support=True,
        columns_names_capitalization="original"
    )
    # s3.console.aws.amazon.com/s3/buckets/

    # Sequence
    init >> load_DFP_files >> finish 

dag = load_files_snowflake()