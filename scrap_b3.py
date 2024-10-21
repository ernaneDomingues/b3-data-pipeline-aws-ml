import os
import time
import pandas as pd
import boto3
from datetime import datetime, timedelta
from selenium import webdriver
from selenium.webdriver.edge.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from webdriver_manager.microsoft import EdgeChromiumDriverManager  # WebDriverManager para Edge
from bs4 import BeautifulSoup
import holidays
from dotenv import load_dotenv
import logging

# Configuração do logging
logging.basicConfig(level=logging.INFO)

# Carregar variáveis de ambiente do arquivo .env
load_dotenv()

# Adiciona o calendário de feriados do Brasil
br_holidays = holidays.Brazil()


AWS_ACCESS_KEY = os.getenv('AWS_ACCESS_KEY')
AWS_SECRET_KEY = os.getenv('AWS_SECRET_KEY')
AWS_REGION = os.getenv('AWS_REGION')
BUCKET_NAME = 's3-ibov-upload-lab-fiap'

# Função para obter o dia útil anterior
def get_previous_trading_day():
    today = datetime.now()
    delta = 1  # A princípio, subtrai 1 dia
    
    # Se for segunda-feira, retrocede para a sexta-feira anterior
    if today.weekday() == 0:  # Segunda-feira
        delta = 3
    # Se for domingo, retrocede para a sexta-feira
    elif today.weekday() == 6:  # Domingo
        delta = 2
    # Se for sábado, retrocede para a sexta-feira
    elif today.weekday() == 5:  # Sábado
        delta = 1
    
    # Calcula a data anterior
    previous_day = today - timedelta(days=delta)
    
    # Se o dia anterior for feriado, continua retrocedendo até encontrar o último dia útil
    while previous_day in br_holidays or previous_day.weekday() >= 5:  # Verifica se é feriado ou final de semana
        previous_day -= timedelta(days=1)
    
    return previous_day

# Função para fazer o upload para o S3
def upload_to_s3(local_file, bucket_name, s3_file):
    s3 = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )
    
    try:
        s3.upload_file(local_file, bucket_name, s3_file)
        logging.info(f"Arquivo {local_file} enviado para {bucket_name}/{s3_file}")
    except Exception as e:
        logging.error(f"Erro ao enviar para o S3: {e}")

# Função para salvar os dados em Parquet e fazer o upload
def save_and_upload_parquet(data, bucket_name):
    df = pd.DataFrame(data, columns=['Setor', 'Código', 'Ação', 'Tipo', 'Qtde. Teórica', 'Part. (%)', 'Part. (%)Acum.'])
    
    previous_day = get_previous_trading_day().strftime("%Y-%m-%d")

    df['date'] = previous_day 
    
    # Estrutura do caminho S3 ano/mês/dia
    year, month, day = previous_day.split('-')
    parquet_file = f"b3_data_{previous_day}.parquet"
    s3_folder = f"{year}/{month}/{day}"
    
    # Salvar o arquivo localmente como parquet
    df.to_parquet(parquet_file, engine='pyarrow')
    
    # Caminho completo no S3
    s3_file_path = f"upload/{s3_folder}/{parquet_file}"
    
    # Fazer upload para o S3
    upload_to_s3(parquet_file, bucket_name, s3_file_path)
    
    # Remover o arquivo local
    os.remove(parquet_file)

# Função para fazer o web scraping dos dados da B3
def scrape_b3_data():
    previous_day = get_previous_trading_day()
    url = f"https://sistemaswebb3-listados.b3.com.br/indexPage/day/ibov?language=pt-br"
    
    # Configurar o Edge com WebDriverManager
    service = Service(EdgeChromiumDriverManager().install())  # Gerenciar o download do EdgeDriver
    driver = webdriver.Edge(service=service)  # Iniciar o Edge
    
    driver.get(url)
    time.sleep(2)
    select_element = Select(driver.find_element(By.ID, "segment"))
    select_element.select_by_visible_text("Setor de Atuação")

    time.sleep(2)

    select_element = Select(driver.find_element(By.ID, "selectPage"))
    select_element.select_by_visible_text("120")

    time.sleep(2)

    page_source = driver.page_source

    soup = BeautifulSoup(page_source, 'html.parser')

    table = soup.find('table', {'class': 'table table-responsive-sm table-responsive-md'})
    
    if table:
        headers = [th.text.strip() for th in table.find_all('th')]
        print("Headers:", headers)

        rows = table.find('tbody').find_all('tr')
        data = []
        for row in rows:
            columns = row.find_all('td')
            data_row = [column.text.strip() for column in columns]
            data.append(data_row)

        save_and_upload_parquet(data, 's3-ibov-fiap-lab')
    else:
        print("Tabela não encontrada.")
    
    driver.quit()

if __name__ == "__main__":
    scrape_b3_data()
