# Pipeline de ETL com Docker, Airflow e PostgreSQL

Este projeto demonstra a construção de um pipeline de dados completo e automatizado para Extração, Transformação e Carga (ETL) da cotação do Dólar, além da geração de relatórios analíticos.

## Objetivo

O ecossistema orquestrado pelo Apache Airflow executa dois pipelines principais:

1.  **Pipeline de ETL (Diário):**
    * Extrai o histórico de cotações do Dólar (BRL) do último ano, utilizando a API pública do Banco Central do Brasil.
    * Valida os dados extraídos para garantir a integridade.
    * Transforma os dados e os carrega em um banco de dados PostgreSQL.
    * Gera um gráfico da evolução diária da cotação.

2.  **Pipeline de Análise (Semanal):**
    * Lê os dados consolidados do PostgreSQL.
    * Gera múltiplos relatórios visuais, como a cotação média por mês e por dia da semana.

## Tecnologias Utilizadas

* **Orquestração:** Apache Airflow
* **Containerização:** Docker & Docker Compose
* **Banco de Dados:** PostgreSQL
* **Linguagem & Bibliotecas:** Python, Pandas, SQLAlchemy, Matplotlib, Seaborn

## Como Executar

1.  Clone este repositório.
2.  Na raiz do projeto, crie um arquivo `.env` baseado no arquivo `.env.example` e preencha suas credenciais de usuário para o Airflow.
3.  Execute o seguinte comando no terminal:
    ```bash
    docker-compose up -d --build
    ```
4.  Acesse a interface do Airflow em `http://localhost:8080` e ative as DAGs.