import sys 
sys.path.append(r'C:\Users\SALA443\Desktop\Projetos\Dados_B3 - teste\CVM\airflow')
sys.path.append(r"C:\Users\SALA443\Desktop\Projetos\Dados_B3 - teste\Transform\CVM_env\Lib\site-packages")
# print (sys.path)
from tasks.objects import *
# from objects import *
import datetime as dt
from datetime import datetime,date,time
import zipfile
import requests
from bs4 import BeautifulSoup
import re
import os
import logging
import boto3
import pandas as pd
from botocore.exceptions import ClientError
from pathlib import Path
from dotenv import load_dotenv
from io import StringIO
import io
import s3fs
import gzip
# columns='CNPJ_CIA','DENOM_SOCIAL','DENOM_COMERC','DT_REG','DT_CONST','DT_CANCEL','MOTIVO_CANCEL','SIT','DT_INI_SIT','CD_CVM','SETOR_ATIV','TP_MERC','CATEG_REG','DT_INI_CATEG','SIT_EMISSOR','DT_INI_SIT_EMISSOR','CONTROLE_ACIONARIO','TP_ENDER','LOGRADOURO','COMPL','BAIRRO','MUN','UF','PAIS','CEP','DDD_TEL','TEL','DDD_FAX','FAX','EMAIL','TP_RESP','RESP','DT_INI_RESP','LOGRADOURO_RESP','COMPL_RESP','BAIRRO_RESP','MUN_RESP','UF_RESP','PAIS_RESP','CEP_RESP','DDD_TEL_RESP','TEL_RESP','DDD_FAX_RESP','FAX_RESP','EMAIL_RESP','CNPJ_AUDITOR','AUDITOR'
url = 'https://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv'

'''
This script aims to build the enviroment to receive files from 
CVM DFPs and then save it into S3 bucket.
You can navigate over the script using the commented tags as bellow:
1-Setting up variables
2-Initiate ELT for DFPs files
3-Initiate ELT for CAD files
'''

# 1-Setting up variables
logging.basicConfig(
    
        level=logging.INFO,
        handlers=[logging.FileHandler("dadoseconomicos.log", mode='w'),logging.StreamHandler()],
        format="%(message)s -  %(funcName)s - %(filename)s - %(asctime)s"
        )

'''
The line bellow defines which aws_credential is gonna be used for connect
aws resources
'''
credentials = aws_connection(profile="admin").account
'''
the variable credentials is a dictionary 
this dictionary comes from CVM\airflow\dags\tasks\objects.py [aws_connection()]
to retrieve each value use as shown bellow
IE:
credentials['aws_access_key_id']
credentials['aws_secret_access_key']
credentials['aws_region']'''
'''This variable instatiate s3 client by '''
client = boto3.client('s3',
                        aws_access_key_id=credentials['aws_access_key_id'],
                        aws_secret_access_key=credentials['aws_secret_access_key']
                        )

today = dt.date.today()           
YearMonthDateFolder = folderpath(
    year = str(today.year),
    month = str(today.month).zfill(2),
    day = str(today.day).zfill(2))
'''output example: 2023/11/23/'''

'''
These variables bellow aims to buils local folder structures
and is used only as temporary storage when it is running in container
'''
LandingZone = f"{s3path(innerpath='landing').fullpath}/{YearMonthDateFolder}"
'''output example: landing/2023/11/23/'''

ProcessedZone = f"{s3path(innerpath='processed').fullpath}/{YearMonthDateFolder}"
'''output example: processed/2023/11/23/'''

ConsumeZone = f"{s3path(innerpath='consume').fullpath}/{YearMonthDateFolder}"
'''output example: consume/2023/11/23/'''

EnrichedZone = f"{s3path(innerpath='enriched').fullpath}/{YearMonthDateFolder}"
'''output example: enriched/2023/11/23/'''

url = 'https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/'
html_text = requests.get(url).text
soup = BeautifulSoup(html_text, 'html.parser')

'''The functions bellow were developed to test connections into pytest module'''
def connection_status(url):
    resp = requests.get(url)
    stt = str(resp.status_code)
    if stt == '200':
        logging.info(f'The {url} is available: {stt}')
    else:
        logging.error(f"The {url} isn't available: {stt}. Please check if is there any issue with given {url}")
    return stt

def s3_connection_status():
    try:
        client = boto3.client('s3',
                        aws_access_key_id=credentials['aws_access_key_id'],
                        aws_secret_access_key=credentials['aws_secret_access_key']
                        )
        logging.info(f"AWS access granted for aws_access_key_id {credentials['aws_access_key_id'][:5]} and aws_secret_access_key {credentials['aws_secret_access_key'][:5]} ")
    except:
        logging.error(f"AWS access denied for aws_access_key_id {credentials['aws_access_key_id'][:5]} and aws_secret_access_key {credentials['aws_secret_access_key'][:5]} ")
        

# 2-Initiate ELT for DFPs files
'''
First Step
This piece of code aims to extract data from https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/
and to save into temporary local landing path as zipfiles.
'''
def create_path_folder():
    if not os.path.exists(LandingZone):
        os.makedirs(LandingZone)
        logging(f'Local path was created as {LandingZone}') 
    logging(f'Path {LandingZone} was found.')
         

url_list = list()
def storing_files():
    files_text = soup.get_text()
    file_list = re.findall(r'\b\w+\.zip\b',files_text)
       
    for i in file_list:
        ul = f'https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/{i}'
        # r = requests.get(f'https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/{i}')
        url_list.append(ul)    

    for url in url_list:
        _file = requests.get(url) 
        for fl in file_list:
            with open(f'{LandingZone}{fl}', 'wb') as file:
                file.write(_file.content)



'''The dictionaries bellow define the data schema and NA filling which is 
gonna be used on the aws bucket landing-zone'''
dre = {
    'CNPJ_CIA': str,
    'DT_REFER': str,
    'VERSAO': int,
    'DENOM_CIA': str,
    'CD_CVM': int,
    'GRUPO_DFP': str,
    'MOEDA': str,
    'ESCALA_MOEDA': str,
    'ORDEM_EXERC': str,
    'DT_INI_EXERC': str,
    'DT_FIM_EXERC': str,
    'CD_CONTA': str,
    'DS_CONTA': str,
    'VL_CONTA': float,
    'ST_CONTA_FIXA': str
}

dreNA = {
    'CNPJ_CIA':'00.000.000/0001.00',
    'DT_REFER':'1900-01-01',
    'VERSAO':-1,
    'DENOM_CIA':'Not Available',
    'CD_CVM':-1,
    'GRUPO_DFP':'Not Available',
    'MOEDA':'Not Available',
    'ESCALA_MOEDA':'Not Available',
    'ORDEM_EXERC':'Not Available',
    'DT_INI_EXERC':'1900-01-01',
    'DT_FIM_EXERC':'1900-01-01',
    'CD_CONTA':'Not Available',
    'DS_CONTA':'Not Available',
    'VL_CONTA':-1.0,
    'ST_CONTA_FIXA':'Not Available'
}


def UnzipLandingToRaw():
    '''This function gets the zip files from local landing
    then unzip it into a second layer as csv'''    
    for file_name in os.listdir(LandingZone):
        transform.ToRaw(file_name)



def s3_upload_file(file_name, bucket, object_name=None):
    """Upload a file to an S3 bucket
            :param file_name: File to upload
            :param bucket: Bucket to upload to
            :param object_name: S3 object name. If not specified then file_name is used
            :return: True if file was uploaded, else False
    """
    # If S3 object_name was not specified, use file_name
    if object_name is None:
        object_name = os.path.basename(file_name)

    # Upload the file
    s3_client = boto3.client('s3',
                             aws_access_key_id=credentials['aws_access_key_id'], 
                            aws_secret_access_key=credentials['aws_secret_access_key'], 
                            region_name=credentials['aws_region'])
    try:
        response = s3_client.upload_file(file_name, bucket, f'{YearMonthDateFolder}{object_name}')
    except ClientError as e:
        logging.error(e)
        return False
    return True

def s3_upload_file_iterate_source():   
    
    WINDOWS_LINE_ENDING = b'\r\n'
    UNIX_LINE_ENDING = b'\n'
    
    bucket = folder_builder(
    sourcelayer='landing',
    targetlayer='processed',
    storageOption='s3').storage_selector  
    
    for filename in os.listdir(ProcessedZone):
        f = os.path.join(ProcessedZone, filename)
        with open(f, 'rb') as open_file:
            content = open_file.read()
            content = content.replace(WINDOWS_LINE_ENDING, UNIX_LINE_ENDING)
        
        if os.path.isfile(f):
            df = pd.read_csv(f,sep=';',encoding='Windows-1252')
            buffer=io.StringIO()
            df.to_csv(buffer,sep=';',encoding='utf-8',index=None)
            
            client.put_object(
                Body=buffer.getvalue(),
                Bucket=bucket['source_bucket'],
                Key=f'{YearMonthDateFolder}{filename}') 

# def s3_upload_file_iterate_source():   
    
#     bucket = folder_builder(
#     sourcelayer='landing',
#     targetlayer='processed',
#     storageOption='s3').storage_selector  
    
#     for filename in os.listdir(ProcessedZone):
#         f = os.path.join(ProcessedZone, filename)
    
#         if os.path.isfile(f):
#             s3_upload_file(f, bucket['source_bucket'], object_name=None)
# 3-Initiate ELT for CAD files
def s3_cad_cia_abertaCVM_to_landing():
    
    bucket = folder_builder(
    sourcelayer='landing',
    targetlayer='processed',
    storageOption='s3').storage_selector  
    
    url = 'https://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv'
    # logging checkpoint
    connection_status(url)
       
    df = pd.read_csv(url,sep=';',encoding='Windows-1252')
    rows_count = len(df. index)
    buffer = io.StringIO()    
    df.to_csv(buffer,encoding='utf-8',sep=';',index=None)
    # logging checkpoint
    logging.info(f"(Function:s3_cad_cia_abertaCVM_to_landing) - (Message: Pandas dataframe was successfully buffered by StringIO)")
    
    s3_connection_status()
    
    client.put_object(ACL='private',
              Body=buffer.getvalue(),
              Bucket=bucket['source_bucket'],
              Key=f'{YearMonthDateFolder}cad_cia_aberta.csv')
    
    # logging checkpoint
    logging.info(f"(Function:s3_cad_cia_abertaCVM_to_landing) - (Message:{rows_count} rows for cad_cia_aberta.csv were uploded to s3 bucket {bucket['source_bucket']})")

def s3_cad_cia_abertaCVM_to_processed():
    s3_connection_status()
    bucket = folder_builder(
    sourcelayer='landing',
    targetlayer='processed',
    storageOption='s3').storage_selector 

    key = f'{YearMonthDateFolder}cad_cia_aberta.csv'
    
    # logging checkpoint
    logging.info(f"(Function:s3_cad_cia_abertaCVM_to_processed) - (Message:Data transfer has been configured as {bucket['source_bucket']} to {bucket['target_bucket']}{key})")
    
    data = client.get_object(Bucket=bucket['source_bucket'],Key=key)
    
    df = pd.read_csv(data['Body'],sep=';',encoding='utf-8')
    rows_count = len(df. index)
    df['loaded_toProcessed_date'] = date.today()
    df['loaded_toProcessed_time'] = datetime.now().time()
    buffer = io.StringIO()
    df.to_csv(buffer,encoding='utf-8',sep=';',index=None)    
    logging.info(f"(Function:s3_cad_cia_abertaCVM_to_processed) - (Message: Pandas dataframe was successfully buffered by StringIO)")
    client.put_object(
              ACL='private',
              Body=buffer.getvalue(),
              Bucket=bucket['target_bucket'],
              Key=f'{YearMonthDateFolder}cad_cia_aberta.csv')
    logging.info(f"(Function:s3_cad_cia_abertaCVM_to_processed) - (Message: {rows_count} rows for cad_cia_aberta.csv was sent from {bucket['source_bucket']} to {bucket['target_bucket']})")

def s3_cad_cia_abertaCVM_to_consume():

    bucket = 'de-okkus-processed-dev-727477891012'
    key = f'{YearMonthDateFolder}cad_cia_aberta.csv'
    
    data = client.get_object(Bucket=bucket,Key=key)
    
    df = pd.read_csv(data['Body'],sep=';',encoding='utf-8')
    df['loaded_toConsume_date'] = date.today()
    df['loaded_toConsume_time'] = datetime.now().time()
    buffer = io.StringIO()
    df.to_csv(buffer,encoding='utf-8',sep=';',index=None)    
    
    client.put_object(
              ACL='private',
              Body=buffer.getvalue(),
              Bucket='de-okkus-consume-dev-727477891012',
              Key=f'{YearMonthDateFolder}cad_cia_aberta.csv')

