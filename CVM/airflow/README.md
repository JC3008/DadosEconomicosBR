https://dados.cvm.gov.br/dataset/cia_aberta-cad

https://dados.cvm.gov.br/dados/CIA_ABERTA/CAD/DADOS/cad_cia_aberta.csv

## Snowflake enviroment setup

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

INSERT INTO dfp_cia_aberta_DRE_con_2023 (CNPJ_CIA,DT_REFER,VERSAO,DENOM_CIA,CD_CVM,GRUPO_DFP,MOEDA,ESCALA_MOEDA,ORDEM_EXERC,DT_INI_EXERC,DT_FIM_EXERC,CD_CONTA,DS_CONTA,VL_CONTA,ST_CONTA_FIXA) values ('02.635.522/0001-95','2023-03-31',2,'JALLES MACHADO S.A.',025496,'DF Consolidado - Demonstração do Resultado','REAL','MIL','PENÚLTIMO','2021-04-01','2022-03-31','3.01','Receita de Venda de Bens e/ou Serviços',1449073.0000000000,'S')

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
    
CREATE STAGE s3_dfp_consume_zone 
    FILE_FORMAT = CSV
	URL = 's3://de-okkus-consume-dev-727477891012/2023/dfp_cia_aberta_DRE_con_2023.csv' 
	CREDENTIALS = ( AWS_KEY_ID = '********' AWS_SECRET_KEY = '********' ) 
	DIRECTORY = ( ENABLE = true );


