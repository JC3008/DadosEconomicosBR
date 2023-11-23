
# Tasks performed by this code
'''This code performs the upload from s3 files into Snowflake platform
by using task flow aql.load_file built in function. This function gets
the file from s3 bucket consume-zone once a day.

It s required to set the aws credendials in the airflow connection.
For detailed information take a look at ReadMe file on https://github.com/JC3008/DadosEconomicosBR.
'''
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
    schedule="0 13 * * *",
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
        input_file=File(path="s3://de-okkus-consume-dev-727477891012/2023/11/22/dfp_cia_aberta_DRE_con_2023.csv",filetype=FileType.CSV, conn_id=SOURCE_AWS_CONN_ID),
        output_table=Table(name="DFP_CIA_ABERTA_DRE_CON_2023",conn_id=SNOWFLAKE_CONN_ID,metadata=Metadata(schema="ASTRO_SDK_SCHEMA")),
        if_exists="replace",
        # is_incremental=False,
        use_native_support=True,
        columns_names_capitalization="original"
    )
    # s3.console.aws.amazon.com/s3/buckets/

    # Sequence
    init >> load_DFP_files >> finish 

dag = load_files_snowflake()