import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path
from sqlalchemy import create_engine
import os

# Configurações
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST")
DB_PORT = os.getenv("POSTGRES_PORT")
DB_NAME = os.getenv("POSTGRES_DB")
DATABASE_URI = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
REPORTS_PATH = Path("/opt/airflow/charts/reports")

# Funções de Análise e Visualização
def create_monthly_average_chart():
    """Gera um gráfico da cotação média mensal"""
    print("Gerando gráfico: Cotação Média Mensal...")
    engine = create_engine(DATABASE_URI)
    query = """
    SELECT TO_CHAR(date, 'YYYY-MM') AS mes,
    AVG(price_brl) AS cotacao_media 
    FROM cotacao_dolar 
    GROUP BY mes ORDER BY mes;
    """
    df_analysis = pd.read_sql(query, engine, index_col='mes')

    if df_analysis.empty:
        print("Não foram encontrados dados para o gráfico de média mensal.")
        return

    plt.style.use('dark_background')
    plt.figure(figsize=(12, 7))
    plot = df_analysis['cotacao_media'].plot(kind='bar', color=sns.color_palette('viridis', len(df_analysis)))
    plt.title('Cotação Média Mensal do Dólar (BRL)')
    plt.xlabel('Mês')
    plt.ylabel('Cotação Média (R$)')
    plt.xticks(rotation=45, ha='right')
    plt.tight_layout()

    for index, value in enumerate(df_analysis['cotacao_media']):
        plt.text(index, value + 0.01, f"R${value:.2f}", ha='center', color='white')

    chart_filepath = REPORTS_PATH / "01_cotacao_media_mensal.png"
    plt.savefig(chart_filepath, facecolor='#242424')
    plt.close()
    print(f"Gráfico salvo em: {chart_filepath}")

def create_day_of_week_analysis_chart():
    """Gera um gráfico da cotação média por dia da semana"""
    print("Gerando gráfico: Análise por Dia da Semana...")
    engine = create_engine(DATABASE_URI)
    query = """
    SELECT TO_CHAR(date, 'ID') AS dia_semana_num,
    AVG(price_brl) AS cotacao_media 
    FROM cotacao_dolar 
    GROUP BY dia_semana_num ORDER BY dia_semana_num;
    """
    df_analysis = pd.read_sql(query, engine)

    if df_analysis.empty:
        print("Não foram encontrados dados para o gráfico de dia da semana.")
        return

    dias_semana_map = {'1': 'Segunda-feira', '2': 'Terça-feira', '3': 'Quarta-feira', '4': 'Quinta-feira', '5': 'Sexta-feira'}
    df_analysis['dia_semana_nome'] = df_analysis['dia_semana_num'].map(dias_semana_map)
    df_analysis = df_analysis.dropna(subset=['dia_semana_nome'])
    plt.style.use('dark_background')
    plt.figure(figsize=(12, 7))
    plot = sns.barplot(x='dia_semana_nome', y='cotacao_media', data=df_analysis, order=df_analysis['dia_semana_nome'], palette='plasma', hue='dia_semana_nome', legend=False)
    plt.title('Cotação Média por Dia da Semana (Dias Úteis)')
    plt.xlabel('Dia da Semana')
    plt.ylabel('Cotação Média (R$)')
    plt.xticks(rotation=0)
    plt.tight_layout()

    for p in plot.patches:
        plot.annotate(f"R${p.get_height():.2f}", (p.get_x() + p.get_width() / 2., p.get_height()), ha='center', va='center', xytext=(0, 9), textcoords='offset points', color='white')

    chart_filepath = REPORTS_PATH / "02_cotacao_media_dia_semana.png"
    plt.savefig(chart_filepath, facecolor='#242424')
    plt.close()
    print(f"Gráfico salvo em: {chart_filepath}")

def create_monthly_volatility_chart():
    """Gera um gráfico da volatilidade (range max-min) mensal"""
    print("Gerando gráfico: Volatilidade Mensal...")
    engine = create_engine(DATABASE_URI)
    query = """
    SELECT TO_CHAR(date, 'YYYY-MM') AS mes, 
    MAX(price_brl) - MIN(price_brl) AS volatilidade 
    FROM cotacao_dolar 
    GROUP BY mes ORDER BY mes;
    """
    df_analysis = pd.read_sql(query, engine, index_col='mes')

    if df_analysis.empty:
        print("Não foram encontrados dados para o gráfico de volatilidade.")
        return

    plt.style.use('dark_background')
    plt.figure(figsize=(12, 7))
    plot = df_analysis['volatilidade'].plot(kind='line', marker='o', linestyle='--', color='cyan')
    plt.title('Volatilidade Mensal do Dólar (Diferença MAX-MIN)')
    plt.xlabel('Mês')
    plt.ylabel('Variação em R$')
    plt.xticks(rotation=45, ha='right')
    plt.grid(True, linestyle='--', alpha=0.5)
    plt.tight_layout()
    chart_filepath = REPORTS_PATH / "03_volatilidade_mensal.png"
    plt.savefig(chart_filepath, facecolor='#242424')
    plt.close()
    print(f"Gráfico salvo em: {chart_filepath}")

if __name__ == "__main__":
    print("Iniciando geração de todos os relatórios analíticos...")
    try:
        REPORTS_PATH.mkdir(parents=True, exist_ok=True)
        create_monthly_average_chart()
        create_day_of_week_analysis_chart()
        create_monthly_volatility_chart()
        print("\nTodos os relatórios foram gerados com sucesso!")
    except Exception as e:
        print(f"\nOcorreu um erro geral: {e}")