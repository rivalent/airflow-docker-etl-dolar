from __future__ import annotations
import pendulum
from airflow.decorators import dag, task
from scripts.etl_dolar import extract_data, validate_extracted_data, transform_and_load_to_db, read_from_db_and_visualize

@dag(
    dag_id="dolar_etl_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2025, 8, 28, tz="America/Sao_Paulo"),
    catchup=False,
    tags=["etl", "dolar", "postgres"],
    doc_md="""
    ## Pipeline de ETL para Cotação do Dólar
    Pipeline robusto que inclui validação de dados.
    """
)
def etl_dolar_pipeline():
    @task
    def task_extract():
        return extract_data()

    @task
    def task_validate(data: list):
        return validate_extracted_data(data)

    @task
    def task_transform_and_load(data: list):
        transform_and_load_to_db(data)

    @task
    def task_visualize():
        read_from_db_and_visualize()

    raw_data = task_extract()
    validated_data = task_validate(raw_data)
    transform_load_task = task_transform_and_load(validated_data)
    visualize_task = task_visualize()
    
    transform_load_task >> visualize_task

etl_dolar_pipeline()