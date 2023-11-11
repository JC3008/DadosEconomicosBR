#Resumo

##Objetivo
Esse projeto visa automatizar a coleta de dados abertos de empresas listadas na bolsa de valores. O site que contém os dados é (https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/). De todos os dados disponíveis, este projeto visa coletar as DRE's.
A abordagem é descrita a seguir:

##Tecnologias
IAC: Terraform
Metodologia de armazenamento: Datalake (AWS S3)
Orquestração: Astro-Python-SDK
Conteinerização: Docker
ELT: Python & SQL
Dataviz: Power BI

##Instruções básicas
Para provisionar os recursos necessários na nuvem AWS, será necessário instalar o terraform e o AWS CLI. Configurar um profile no AWS CLI passando as credenciais, região e formato de arquivo. Após configurar, siga os seguintes passos:

###Provisão de recursos na AWS
Navegue até a pasta terraform
inicie o terraform (terraform init)
valide o script (terraform validate)
crie o plano de execução (terraform plan)
aplique o plano de execução (terraform apply "plano")
pronto!

Com esses comandos, espera-se que os recursos estejam criados na AWS dentro da região definida anteriormente.

###Orquestração no Airflow usando Astro-Python-SDK
No script CVM\airflow\dags\tasks\extract.py existe uma referência ao arquivo CVM\airflow\.env, neste arquivo deve-se manter a access key e a secret key da AWS. Então antes de iniciar o processo de build do astro-python-sdk, insira suas credenciais neste arquivo. E lembre-se de nunca expor suas credenciais em repositórios publicos.

No arquivo Dockerfile, existe referência para o caminho do arquivo .env, e é necessário descomentar e ajustar o caminho para ajustar para o caminho correto.

Inicie o Docker Desktop
Navegue até a pasta airflow
Inicie o projeto com o comando: astro dev start
Dentro da pasta airflow, crie um arquivo chamado .env e insira as credenciais AWS.
Exemplo:
aws_access_key_id=****************************
aws_secret_access_key=****************************
No navegador, acesse http://localhost:8080
Será solicitado usuario e senha para UI do Airflow.
user:admin
password:admin
Você verá a DAG (elt_dfp_to_s3)
Habilite a DAG
Pronto!

#Objective
This project aims to automate the colecting of open data from brazilian companies that are listed on B3 (Brasil, Bolsa, Balcão). The data collection is available on (https://dados.cvm.gov.br/dados/CIA_ABERTA/DOC/DFP/DADOS/). Among the available data, this project is focused on DRE's files.

##Tecnologies
The aproach is described as follows:

IAC (Infrasctructure as code): Terraform
Methodology: Datalake (AWS S3)
Orquestration: Astro-Python-SDK
Conteinerization: Docker
ELT: Python & SQL
Dataviz: Power BI

##Orquestration on Astro-Python-SDK
Start the Docker Desktop
Navigate to airflow folder
Start the project by running the follow command: astro dev start
Within the airflow folder, create a file called (.env) and type the AWS credentials.
IE:
aws_access_key_id=****************************
aws_secret_access_key=****************************
User and password are going to be requested. Type as follow:
user:admin
password:admin
You ll see the DAG (elt_dfp_to_s3)
Enable it.
Done!!

