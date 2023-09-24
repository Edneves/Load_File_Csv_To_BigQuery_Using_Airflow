# Load_File_Csv_To_BigQuery_Using_Airflow

 Porjeto criado com o intuito de carregar as arquivos csv's que se encontram no Bucket da Cloud Storage para o BigQuery utilizando o Airflow.

1. "schema" diretório que possui o schema da tabela a ser criada no BigQuery;
2. "LOAD_FILE_CSV_TO_BIGQUERY.py" estrutura da DAG do Airflow;
3. "config.yml" arquivo yml que possui as informações de camadas no datalake, nome de arquivo parâmetros de configuração da DAG, dataset no BigQuery, project_id e nome da tabela.

- Tools used:
1. Python
2. Airflow
3. Cloud Storage
4. BigQuery 
