from pendulum import datetime
from tasks.extract import s3_cad_cia_abertaCVM_to_landing,s3_cad_cia_abertaCVM_to_processed,s3_cad_cia_abertaCVM_to_consume
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from airflow.decorators import (
    dag,
    task,
)  
from airflow.models.baseoperator import chain


@dag(
    dag_id="elt_cad_to_s3",
    schedule="0 13 * * *",
    start_date=datetime(2023, 11, 22),
    catchup=False,
    default_args={
        "retries": 2,  
    },
    tags=["elt","s3","okkus_pipelines_cad","ingestion","cad","CVM"],
    max_active_runs=1,
    max_active_tasks=2,
)  

def elt_cad_to_s3():    
    
    init    = EmptyOperator(task_id="init")
    finish  = EmptyOperator(task_id="finish")    
    
    @task()
    def extract_cad_to_landing():
        s3_cad_cia_abertaCVM_to_landing()
        return None
        
    @task()
    def extract_cad_to_processed():
        s3_cad_cia_abertaCVM_to_processed()
        return None
    
    @task()
    def extract_cad_to_consume():
        s3_cad_cia_abertaCVM_to_consume()
        return None
    
    
    t1 = extract_cad_to_landing()
    t2 = extract_cad_to_processed()
    t3 = extract_cad_to_consume()
   

    
    chain(init,t1,t2,t3,finish)    

dag = elt_cad_to_s3()   
