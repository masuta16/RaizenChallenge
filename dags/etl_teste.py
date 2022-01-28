import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta
from airflow.utils.dates import days_ago
import requests
import json

args={'owner': 'Israel'}

default_args = {
    'owner': 'Israel',    
    #'start_date': airflow.utils.dates.days_ago(2),
    # 'end_date': datetime(),
    # 'depends_on_past': False,
    #'email': ['airflow@example.com'],
    #'email_on_failure': False,
    # 'email_on_retry': False,
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    }
 #função le os dados de diesel modela e salva em csv   
def dieselCSV():
  #base de dados 1
  df2 = pd.read_csv('https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vct/vendas-oleo-diesel-tipo-m3-2013-2021.csv',sep=';')
  df2["year_month"] = df2['ANO'].map(str)+ '-' + df2['MÊS'].map(str) 
  df2.drop(["ANO", "MÊS"], axis = 1, inplace = True)
  df2 = df2.rename(columns={'UNIDADE DA FEDERAÇÃO': 'uf', 'PRODUTO': 'product', 'VENDAS':'sales_vol'})
  df2.loc[:,'unit'] = 'm^3'
  df2=df2[["year_month","uf","product","unit","sales_vol"]]
  df2.to_csv('vendas-oleo-diesel-tipo.csv', index=False)
  df.to_parquet('vendas-oleo-diesel-tipo.parquet')

def derivadosCSV(self):
  #base de dados 1
  df = pd.read_csv('https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vendas-derivados-petroleo-e-etanol/vendas-derivados-petroleo-etanol-m3-1990-2021.csv',sep=';' )
  df["year_month"] = df['ANO'].map(str)+ '-' + df['MÊS'].map(str) 
  df.drop(["ANO", "MÊS", "GRANDE REGIÃO"], axis = 1, inplace = True)
  df = df.rename(columns={'UNIDADE DA FEDERAÇÃO': 'uf', 'PRODUTO': 'product', 'VENDAS':'sales_vol'})
  df.loc[:,'unit'] = 'm^3'
  df=df[["year_month","uf","product","unit","sales_vol"]]
  df.to_csv('vendas-derivados-petroleo.csv', index=False)
  df.to_parquet('vendas-derivados-petroleo.parquet')  
  
dag_pandas = DAG(dag_id = "Raizen-ETL",default_args=default_args ,
                 schedule_interval='0 0 * * *',
                 #schedule_interval='@once',	
                 dagrun_timeout=timedelta(minutes=60),
                 description='Raizen challenge solution',
                 start_date = airflow.utils.dates.days_ago(1))   

dag_pandas2 = DAG(dag_id = "Raizen-ETL2",default_args=default_args ,
                 schedule_interval='0 0 * * *',
                 #schedule_interval='@once',	
                 dagrun_timeout=timedelta(minutes=60),
                 description='Raizen challenge solution',
                 start_date = airflow.utils.dates.days_ago(1))

derivadosCSV = PythonOperator(task_id='derivadosCSV', python_callable=derivadosCSV, dag=dag_pandas2)

dieselCSV = PythonOperator(task_id='dieselCSV', python_callable=dieselCSV, dag=dag_pandas)
