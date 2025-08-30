# começa a partir da imagem oficial do Airflow
FROM apache/airflow:2.9.2

# Copia o arquivo de requisitos para dentro da imagem
COPY requirements.txt .

# Instala as bibliotecas Python que o projeto precisa
RUN pip install --no-cache-dir -r requirements.txt