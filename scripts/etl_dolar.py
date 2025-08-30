import pandas as pd
import requests
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path
from datetime import datetime, timedelta
from sqlalchemy import create_engine
import os

# configurações iniciais
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")
DATABASE_URI = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
BASE_API_URL = "https://api.bcb.gov.br/dados/serie/bcdata.sgs.10813/dados"
CHARTS_PATH = Path("/opt/airflow/charts")

# funções do Pipeline de ETL
def extract_data():
    """Extrai os dados da API do Banco Central"""
    print("Iniciando a extração dos dados da API do BCB...")
    end_date = datetime.now() - timedelta(days=1)
    start_date = end_date - timedelta(days=365)
    start_date_str = start_date.strftime('%d/%m/%Y')
    end_date_str = end_date.strftime('%d/%m/%Y')
    api_url_com_periodo = f"{BASE_API_URL}?formato=json&dataInicial={start_date_str}&dataFinal={end_date_str}"
    
    print(f"Consultando a URL: {api_url_com_periodo}")
    try:
        response = requests.get(api_url_com_periodo)
        response.raise_for_status()
        data = response.json()
        print("Dados extraídos com sucesso!")
        return data
    except requests.exceptions.RequestException as e:
        print(f"Erro na extração: {e}")
        raise

def validate_extracted_data(data: list):
    """
    Verifica se a lista de dados extraída não está vazia e se contém as colunas esperadas.
    """
    print("Iniciando a validação dos dados extraídos...")
    if not data:
        raise ValueError("Erro de validação: A API não retornou dados.")
    
    required_keys = ['data', 'valor']
    if not all(key in data[0] for key in required_keys):
        raise ValueError(f"Erro de validação: Os dados não contêm as chaves esperadas: {required_keys}")
    
    print("Validação dos dados extraídos concluída com sucesso!")
    return data

def transform_and_load_to_db(data: list):
    """Carrega os dados em JSON, os transforma e os carrega no banco de dados."""
    print("Iniciando a transformação e carga dos dados no Postgres...")
    try:
        if not data:
            print("A extração não retornou dados. A transformação será pulada.")
            return

        df = pd.DataFrame(data)
        df = df.rename(columns={'data': 'date', 'valor': 'price_brl'})
        df['price_brl'] = pd.to_numeric(df['price_brl'])
        df['date'] = pd.to_datetime(df['date'], format='%d/%m/%Y')

        engine = create_engine(DATABASE_URI)
        table_name = "cotacao_dolar"
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        
        print(f"Dados carregados com sucesso na tabela '{table_name}'!")
    except Exception as e:
        print(f"Erro na transformação e carga: {e}")
        raise

def read_from_db_and_visualize():
    """Lê os dados da tabela no Postgres e gera o gráfico."""
    print(f"Iniciando a leitura do banco de dados para gerar o gráfico principal...")
    try:
        engine = create_engine(DATABASE_URI)
        table_name = "cotacao_dolar"
        df = pd.read_sql(f"SELECT * FROM {table_name} ORDER BY date", engine, parse_dates=['date'])
        
        df['price_rolling_30d'] = df['price_brl'].rolling(window=30).mean()
        
        plt.style.use('dark_background')
        plt.figure(figsize=(14, 7))
        
        sns.lineplot(x='date', y='price_brl', data=df, label='Cotação Diária', color='lightblue', alpha=0.7)
        sns.lineplot(x='date', y='price_rolling_30d', data=df, label='Média Móvel (30 dias)', color='orange', linewidth=2)
        
        plt.title('Cotação do Dólar (BRL) no Último Ano')
        plt.xlabel('Data')
        plt.ylabel('Preço (R$)')
        plt.legend()
        plt.grid(True, linestyle='--', alpha=0.3)
        plt.tight_layout()
        
        CHARTS_PATH.mkdir(parents=True, exist_ok=True)
        today = datetime.now().strftime("%Y-%m-%d")
        chart_filepath = CHARTS_PATH / f"chart_cotacao_dolar_diaria_{today}.png"
        
        plt.savefig(chart_filepath, facecolor='#242424')
        plt.close()
        
        print(f"Gráfico principal (dark mode) salvo com sucesso em: {chart_filepath}")
    except Exception as e:
        print(f"Erro na geração do gráfico: {e}")
        raise

if __name__ == "__main__":
    print("Executando pipeline de ETL completo (com validação e carga no DB)")
    json_data = extract_data()
    if json_data:
        validated_data = validate_extracted_data(json_data)
        transform_and_load_to_db(validated_data)
        read_from_db_and_visualize()
        print("Pipeline de teste concluído com sucesso!")