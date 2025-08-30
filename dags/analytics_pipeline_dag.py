from __future__ import annotations
import pendulum
from airflow.decorators import dag, task
from scripts.generate_analytics import create_monthly_average_chart, create_day_of_week_analysis_chart, create_monthly_volatility_chart

@dag(
    dag_id="dolar_analytics_pipeline",
    schedule="@weekly",  # Roda uma vez por semana (aos Domingos à meia-noite UTC)
    start_date=pendulum.datetime(2025, 8, 29, tz="America/Sao_Paulo"),
    catchup=False,
    tags=['analytics', 'reporting', 'dolar'],
    doc_md="""
    ## DAG de Relatórios Analíticos
    Esta DAG executa o script de análise para gerar gráficos a partir dos dados já processados no banco.
    Pode ser agendada para rodar com uma frequência menor que o ETL principal.
    """
)
def analytics_pipeline():
    """
    ### Pipeline de Análise
    Esta DAG orquestra a geração de relatórios visuais.
    """
    
    @task
    def task_generate_monthly_avg():
        create_monthly_average_chart()

    @task
    def task_generate_day_of_week():
        create_day_of_week_analysis_chart()

    @task
    def task_generate_volatility():
        create_monthly_volatility_chart()

    # Define que as 3 tarefas podem rodar em paralelo, pois não dependem uma da outra
    [task_generate_monthly_avg(), task_generate_day_of_week(), task_generate_volatility()]

analytics_pipeline()