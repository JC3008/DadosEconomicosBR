# Resumo

## Status do projeto:
* IAC (S3)                      Feito!
* IAC (RDS)                     Feito!
* IAC (Lambda)                  Pendente
* Orquestração SourceToS3       Feito!
* Orquestração S3ToSnowflake    Pendente
* Data Warehouse                Pendente

## Objetivo
Esse projeto visa automatizar a coleta de dados abertos de empresas listadas na bolsa de valores. O site que contém os dados é (https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/). De todos os dados disponíveis, este projeto visa coletar as DRE's.
A abordagem é descrita a seguir:

## Tecnologias
* IAC: Terraform
* Metodologia de armazenamento: Datalake        (AWS S3)
* Metodologia de armazenamento: Data Warehouse  (Snowflake)
* Orquestração: Astro-Python-SDK
* Conteinerização: Docker
* ELT: Python & SQL
* Dataviz: Power BI

## Instruções básicas
Para provisionar os recursos necessários na nuvem AWS, será necessário instalar o terraform e o AWS CLI. Configurar um profile no AWS CLI passando as credenciais, região e formato de arquivo. Após configurar, siga os seguintes passos:

### Provisão de recursos na AWS
Navegue até a pasta terraform
inicie o terraform (terraform init)
valide o script (terraform validate)
crie o plano de execução (terraform plan)
aplique o plano de execução (terraform apply "plano")
pronto!

Com esses comandos, espera-se que os recursos estejam criados na AWS dentro da região definida anteriormente.

### Snowflake configuração de ambiente.
Na plataforma Snowflake, crie os seguintes recursos:

* Database:DADOSECONOMICOSBR_DB

* Data Warehouse: DADOSECONOMICOSBR_DW

* Schema: Astro_SDK_Schema

* Tabela: dfp_cia_aberta_DRE_con_2023

create or replace table dfp_cia_aberta_DRE_con_2023
(
    CNPJ_CIA string,
    DT_REFER string,
    VERSAO integer,
    DENOM_CIA string,
    CD_CVM integer,
    GRUPO_DFP string,
    MOEDA string,
    ESCALA_MOEDA string,
    ORDEM_EXERC string,
    DT_INI_EXERC string,
    DT_FIM_EXERC string,
    CD_CONTA string,
    DS_CONTA string,
    VL_CONTA float,
    ST_CONTA_FIXA string
)

* File Format: CSV

create file format CSV
    type = CSV 
    ENCODING = 'ISO-8859-1'
    COMPRESSION = AUTO 
    RECORD_DELIMITER = '\n'
    FIELD_DELIMITER = ';' 
    SKIP_HEADER = 1
    DATE_FORMAT = 'AUTO' 
    TIME_FORMAT = 'AUTO'
    TIMESTAMP_FORMAT = 'AUTO' 
    ESCAPE = 'NONE' 
    FIELD_OPTIONALLY_ENCLOSED_BY = '\042'

* Stage: s3_dfp_consume_zone

CREATE STAGE s3_dfp_consume_zone 
    FILE_FORMAT = CSV
	URL = 's3://de-okkus-consume-dev-727477891012/2023/dfp_cia_aberta_DRE_con_2023.csv' 
	CREDENTIALS = ( AWS_KEY_ID = '********' AWS_SECRET_KEY = '********' ) 
	DIRECTORY = ( ENABLE = true );

### Orquestração no Airflow usando Astro-Python-SDK
No script CVM\airflow\dags\tasks\extract.py existe uma referência ao arquivo CVM\airflow\.env, neste arquivo deve-se manter a access key e a secret key da AWS. Então antes de iniciar o processo de build do astro-python-sdk, insira suas credenciais neste arquivo. E lembre-se de nunca expor suas credenciais em repositórios publicos.

No arquivo Dockerfile, existe referência para o caminho do arquivo .env, e é necessário descomentar e ajustar o caminho para ajustar para o caminho correto.

Inicie o Docker Desktop
Navegue até a pasta airflow
Inicie o projeto com o comando: astro dev start
Dentro da pasta airflow, crie um arquivo chamado .env e insira as credenciais AWS.
Exemplo:
* aws_access_key_id=****************************
* aws_secret_access_key=****************************
No navegador, acesse http://localhost:8080
Será solicitado usuario e senha para UI do Airflow.
* user:admin
* password:admin
Você verá a DAG (elt_dfp_to_s3)
Habilite a DAG
Pronto!

## Airflow connections.

### AWS
* Connection id: aws_default
* Connection Type: amazon_s3
* Extra Args: {"aws_access_key_id":"","aws_secret_access_key":""}

### Snowflake
Para conectar o Airflow ao Snowflake, adicione a conexão conforme a seguir:
* Connection id: snowflake_default
* Connection Type: Snowflake
* Schema: ASTRO_SDK_SCHEMA
* login: [Seu login do snowflake]
* Password: [Seu Password do snowflake]
* Account: [Sua account do Snowflake]
Warehouse: [Nome do DW]
* Database: [Nome do DB]
* Region: [Região em que o Snowflake está configurado]
* Role: ACCOUNTADMIN


# Objective
This project aims to automate the colecting of open data from brazilian companies that are listed on B3 (Brasil, Bolsa, Balcão). The data collection is available on (https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/). Among the available data, this project is focused on DRE's files.

## Project Status:
* IAC (S3)      Done!
* IAC (RDS)     Done!
* IAC (Lambda)  Pending
* Orquestration Pending

## Tecnologies
The aproach is described as follows:

* IAC (Infrasctructure as code): Terraform
* Methodology: Datalake (AWS S3)
* Orquestration: Astro-Python-SDK
* Conteinerization: Docker
* ELT: Python & SQL
* Dataviz: Power BI

## Orquestration on Astro-Python-SDK
Start the Docker Desktop
Navigate to airflow folder
Start the project by running the follow command: astro dev start
Within the airflow folder, create a file called (.env) and type the AWS credentials.
IE:
* aws_access_key_id=****************************
* aws_secret_access_key=****************************
User and password are going to be requested. Type as follow:
user:admin
password:admin
You ll see the DAG (elt_dfp_to_s3)
Enable it.
Done!!

