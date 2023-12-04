from pendulum import datetime
from tasks.extract import create_path_folder, storing_files,UnzipLandingToRaw,s3_upload_file_iterate_source
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from airflow.decorators import (
    dag,
    task,
)  
from airflow.models.baseoperator import chain


@dag(
    dag_id="elt_dfp_to_s3",
    schedule="0 13 * * *",
    start_date=datetime(2023, 11, 9),
    catchup=False,
    default_args={
        "retries": 2,  
    },
    tags=["elt","s3","okkus_pipelines_dfp","ingestion"],
    max_active_runs=1,
    max_active_tasks=2,
)  

def elt_to_s3():    
    
    init    = EmptyOperator(task_id="init")
    finish  = EmptyOperator(task_id="finish")    
    
    @task()
    def create_local_dir():
        create_path_folder()
        return None
        
    @task()
    def getting_files_from_opendata():
        storing_files()
        return None
    
    @task()
    def saving_as_csv():
        UnzipLandingToRaw()
        return None
    
    @task()
    def upload_to_s3_landing():
        s3_upload_file_iterate_source()
        return None
        
    t1 = create_local_dir()
    t2 = getting_files_from_opendata()
    t3 = saving_as_csv()
    t4 = upload_to_s3_landing()  
    
    chain(init,t1,t2,t3,t4,finish)    

dag = elt_to_s3()   
