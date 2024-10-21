import json
import boto3

def lambda_handler(event, context):
    # Inicializando o cliente do Glue
    glue = boto3.client('glue')
    
    # Bucket fixo e caminho base
    bucket_name = 's3-ibov-fiap-lab'
    base_path = 'upload'
    
    # Pega o nome do arquivo e o caminho completo do evento do S3
    object_key = event['Records'][0]['s3']['object']['key']
    
    # Log para verificar se a função está sendo chamada corretamente
    print(f"Arquivo recebido: {object_key} no bucket {bucket_name}")
    
    # Verifica se o arquivo está na estrutura esperada (ano/mês/dia)
    path_parts = object_key.split('/')
    if len(path_parts) >= 4:  # Verifica se existe ano/mês/dia
        ano = path_parts[1]
        mes = path_parts[2]
        dia = path_parts[3]
        
        # Nome do Glue job - customizável
        glue_job_name = 'glue-ibov-data-transform'
        
        try:
            # Inicia o job no Glue
            response = glue.start_job_run(
                JobName=glue_job_name,
                Arguments={
                    '--s3_input_path': f"s3://{bucket_name}/{base_path}/{ano}/{mes}/{dia}/",
                    '--input_file': object_key
                }
            )
            
            # Log de sucesso
            print(f"Glue job {glue_job_name} iniciado com sucesso. JobRunId: {response['JobRunId']}")
            
        except Exception as e:
            print(f"Erro ao iniciar o Glue job: {str(e)}")
            raise e
    else:
        print(f"A estrutura de caminho {object_key} não segue o formato esperado (base/ano/mês/dia)")
    
    return {
        'statusCode': 200,
        'body': json.dumps('Job iniciado com sucesso!')
    }
