import datetime
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


'''
This script aims to build the local enviroment to receive files from 
CVM DFPs and then save it into S3 bucket.
You can navigate over the script using the commented tags as bellow:
1-Folder set up
2-Getting DFP zip files
3-Convert to csv
4-Writting it into S3 bucket  
'''
# 1-Folder set up
class folderpath():
    """This object builds folder structure to store daily data 
    within year-month-date
    """
    def __init__(self,year:str,month:str,day:str):
        
        self.year = year
        self.month = month
        self.day = day

    @property
    def daily(self) -> str:
        return f"{self.year}/{self.month}/{self.day}"
    
    def __str__(self) -> str:
        return f"{self.year}/{self.month}/{self.day}/"
    

class s3path():
    """This object builds folder structure of S3 datalake
    """
    def __init__(self,innerpath:str):        
        self.innerpath = innerpath
        
    @property   
    def fullpath(self) -> str:
        if self.innerpath == 'landing':
            return "landing"
            
        elif self.innerpath == 'processed':
            return "processed"
            
        elif self.innerpath == 'consume':
            
            return "consume"
        elif self.innerpath == 'enriched':
            return "enriched"
            
        else:
            return "caminho inexistente"          
       
class transform():
    
    def ToRaw(file:str):
        """
        This method extracts zip files from a folder and stores inside another given folder
        """
        with zipfile.ZipFile(f"{s3path(innerpath='landing').fullpath}/{folderpath(year = str(datetime.date.today().year),month = str(datetime.date.today().month).zfill(2),day = str(datetime.date.today().day).zfill(2))}/{file}", 'r') as zip_ref:
            zip_ref.extractall(f"{s3path(innerpath='processed').fullpath}/{folderpath(year = str(datetime.date.today().year),month = str(datetime.date.today().month).zfill(2),day = str(datetime.date.today().day).zfill(2))}")

class aws_s3_buckets():
    def __init__(self,innerpath:str):        
        self.innerpath = innerpath
        
    @property   
    def fullpath(self) -> str:
        if self.innerpath == 'landing':
            return "de-okkus-landing-dev-727477891012"
        elif self.innerpath == 'processed':
            return "de-okkus-processed-dev-727477891012"
        elif self.innerpath == 'consume':
            return "de-okkus-consume-dev-727477891012"
        elif self.innerpath == 'silver':
            return "de-okkus-silver-dev-727477891012"
        elif self.innerpath == 'scripts':
            return "de-okkus-scripts-dev-727477891012"
        else:
            return "caminho inexistente"  
# ends settings from folder structure and s3 buckets path

# 2-Getting DFP zip files
today = datetime.date.today()           
YearMonthDateFolder = folderpath(year = str(today.year),month = str(today.month).zfill(2),day = str(today.day).zfill(2))

LandingZone = f"{s3path(innerpath='landing').fullpath}/{YearMonthDateFolder}"
RawZone = f"{s3path(innerpath='processed').fullpath}/{YearMonthDateFolder}"
ConsumeZone = f"{s3path(innerpath='consume').fullpath}/{YearMonthDateFolder}"
EnrichedZone = f"{s3path(innerpath='enriched').fullpath}/{YearMonthDateFolder}"

url = 'https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/'
html_text = requests.get(url).text
soup = BeautifulSoup(html_text, 'html.parser')

'''The functions bellow were developed to test connections into pytest module'''
def connection_status(url):
    resp = requests.get(url)
    stt = str(resp.status_code)
    return stt
'''
First Step
This piece aims to extract data from https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/
and to save into landing path as zipfiles
'''
def create_path_folder():
    if not os.path.exists(LandingZone):
        os.makedirs(LandingZone) 
         

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


# 3-Convert to csv

# The dictionaries bellow define the data schema and NA filling which is gonna be used 
# on the aws bucket landing-zone

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
    for file_name in os.listdir(LandingZone):
        transform.ToRaw(file_name)

def FindDREfiles(param:str) -> list:
    """
    Finds and Lists all files that matches with DRE
    """
    FileList = []
    for file in os.listdir(RawZone):
        
        if re.findall(param,file):
            
            FileList.append(file)
           
    return FileList

def ReadDREFilesTransformAndSaveAsParquet():

    fileList = FindDREfiles('DRE')

    for file in fileList[:]:  
        foldername = f"{str(file).strip('.csv')}/"
        schema = dre
        df = pd.read_csv(f'{RawZone}{file}',delimiter=';',encoding='ISO-8859-1')
        df['Transform_Date'] = pd.to_datetime('today').to_datetime64()
        os.makedirs(f"{ConsumeZone}{foldername}",exist_ok=True)
        file = str(file).strip('.csv')
        df.to_parquet(f"{ConsumeZone}{foldername}{file}_{pd.to_datetime('today').date()}.parquet",engine='pyarrow',partition_cols='DT_INI_EXERC')
        file = None
    return None

# 4-Writting it into S3 landing bucket

dotenv_path = Path('C:/Users/SALA443/Desktop/Projetos/DADOS_B3/CVM/airflow/dags/tasks/.env')
load_dotenv(dotenv_path=dotenv_path)
aws_access_key_id = os.getenv("aws_access_key_id")
aws_secret_access_key = os.getenv("aws_secret_access_key")
region_name = 'us-east-2'

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
                             aws_access_key_id=aws_access_key_id, 
                            aws_secret_access_key=aws_secret_access_key, 
                            region_name=region_name)
    try:
        response = s3_client.upload_file(file_name, bucket, f'{YearMonthDateFolder}{object_name}')
    except ClientError as e:
        logging.error(e)
        return False
    return True

bucket = "de-okkus-landing-dev-727477891012"
def s3_upload_file_iterate_source():    
    for filename in os.listdir(RawZone):
        f = os.path.join(RawZone, filename)
    
        if os.path.isfile(f):
            s3_upload_file(f, bucket, object_name=None)
            
# Here it starts the needed data cleaning and transformation.


            

            


