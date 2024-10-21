import sys
import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pandas as pd
import re
from awsglue.context import GlueContext
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from awsglue.dynamicframe import DynamicFrame
from pyspark.context import SparkContext
from datetime import datetime


# Obtendo os parâmetros do job Glue
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session
s3 = boto3.client('s3')

# Função para transformar os nomes das colunas
def clean_column_names(df):
    df.columns = df.columns.str.lower().str.replace(' ', '_')
    df.columns = [re.sub(r'[^\w\s]', '', col) for col in df.columns]  # Remove caracteres especiais
    return df

# Função para transformar strings para float
def transform_to_float(df):
    float_cols = ['qtde_teorica', 'part_teorica', 'part_%acum']  # Usar part_teorica em vez de part_%
    for col in float_cols:
        df[col] = df[col].str.replace('.', '', regex=False).str.replace(',', '.', regex=False).astype(float)
    return df

# Função para calcular a diferença entre datas
def calculate_date_difference(df):
    df['date_init'] = pd.to_datetime(df['date_init'], format='%Y-%m-%d')  # Usar date_init em vez de date
    df['date_fim'] = datetime.today().strftime('%Y-%m-%d') # Obtenha a data de hoje
    df['diferencas_date'] = (df['date_fim'] - df['date_init']).dt.days  # Diferença de dias entre as datas
    return df

# Definindo os buckets de input e output
bucket_input = 's3://s3-ibov-fiap-lab/upload/'
bucket_output = 's3://s3-ibov-fiap-lab/transform/'

# Listando arquivos no bucket de input
objects = s3.list_objects_v2(Bucket='s3-ibov-fiap-lab', Prefix='upload/')

for obj in objects.get('Contents', []):
    key = obj['Key']
    
    # Verifica se o arquivo tem a extensão .parquet e corresponde à estrutura de ano/mês/dia
    if key.endswith('.parquet') and len(key.split('/')) == 5:
        # Construindo o caminho correto para o arquivo Parquet
        parquet_path = f's3://{obj["Bucket"]}/{key}'
        
        # Lendo o arquivo Parquet
        parquet_file = pq.read_table(parquet_path)
        
        # Convertendo para DataFrame do pandas
        df = parquet_file.to_pandas()
        
        # Limpando os nomes das colunas
        df = clean_column_names(df)

        # Renomeando as colunas 'part_%' para 'part_teorica' e 'date' para 'date_init'
        df.rename(columns={'part_%': 'part_teorica', 'date': 'date_init'}, inplace=True)

        # Transformando colunas de string para float
        df = transform_to_float(df)

        # Calculando a diferença de datas
        df = calculate_date_difference(df)

        # A: Agrupamento e sumarização
        agg_df = df.groupby('setor').agg(
            Total_Qtde_Teórica=('qtde_teorica', 'sum'),
            Media_Part=('part_teorica', 'mean')  # Usar part_teorica aqui também
        ).reset_index()

        print("A: Agrupamento e Sumarização")
        print(agg_df)

        # B: Renomear colunas
        agg_df.rename(columns={'setor': 'setor_agrupado', 'Total_Qtde_Teórica': 'total_qtde_teorica', 'Media_Part': 'media_part'}, inplace=True)

        print("\nB: Renomeação de Colunas")
        print(agg_df)

        # C: Cálculo com campos de data (continua após cálculo de diferença de datas)
        df['Diferenca_Dias'] = df['diferencas_date']
        print("\nC: Cálculo com Campos de Data")
        print(df[['date_ini', 'date_fim', 'Diferenca_Dias']].head())

        # Salvando o DataFrame original (com as novas colunas) como parquet
        df_init = df.copy()  # Copia para salvar no `ibov_init` posteriormente
        table_init = pa.Table.from_pandas(df_init)
        pq.write_table(table_init, f'{bucket_output}/init_{key.split("/")[-1]}')

        # Convertendo para DynamicFrame e criando a tabela ibov_init no Glue Catalog
        dynamic_frame_init = DynamicFrame.fromDF(df_init, glueContext, "dynamic_frame_init")
        glueContext.write_dynamic_frame.from_catalog(
            frame=dynamic_frame_init,
            database='fiaplab',  # Nome do banco de dados no Glue Catalog
            table_name='ibov_init',  # Nome da tabela para o parquet inicial
            transformation_ctx="init_ctx"
        )

        # Salvando o DataFrame agrupado como Parquet no bucket de output
        table_agg = pa.Table.from_pandas(agg_df)
        pq.write_table(table_agg, f'{bucket_output}/agg_{key.split("/")[-1]}')

        # Copiando o arquivo Parquet original para o bucket de transformados
        s3.copy_object(
            Bucket='s3-ibov-fiap-lab',
            CopySource={'Bucket': 's3-ibov-fiap-lab', 'Key': key},
            Key=f'transform/originals/{key.split("/")[-1]}'
        )

        # Convertendo o DataFrame agrupado para DynamicFrame
        dynamic_frame_agg = DynamicFrame.fromDF(agg_df, glueContext, "dynamic_frame_agg")

        # Criando a tabela ibov_agg no Glue Catalog para os dados agregados
        glueContext.write_dynamic_frame.from_catalog(
            frame=dynamic_frame_agg,
            database='fiaplab',  # Nome do banco de dados no Glue Catalog
            table_name='ibov_agg',  # Nome da tabela para os dados agrupados
            transformation_ctx="agg_ctx"
        )

print("Processamento concluído.")
