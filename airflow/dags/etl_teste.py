import airflow
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
import pandas as pd



args={'owner': 'Israel'}

default_args = {
    'owner': 'Israel',    
    'start_date': airflow.utils.dates.days_ago(2),
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

def extractANP():
  df2 = pd.read_csv('https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vct/vendas-oleo-diesel-tipo-m3-2013-2021.csv',sep=';')
  df2["year_month"] = df2['ANO'].map(str)+ '-' + df2['MÊS'].map(str) 
  df2.drop(["ANO", "MÊS"], axis = 1, inplace = True)
  df2 = df2.rename(columns={'UNIDADE DA FEDERAÇÃO': 'uf', 'PRODUTO': 'product', 'VENDAS':'sales_vol'})
  df2.loc[:,'unit'] = 'm^3'
  df2=df2[["year_month","uf","product","unit","sales_vol"]]
  df = pd.read_csv('https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vendas-derivados-petroleo-e-etanol/vendas-derivados-petroleo-etanol-m3-1990-2021.csv',sep=';' )
  df["year_month"] = df['ANO'].map(str)+ '-' + df['MÊS'].map(str) 
  df.drop(["ANO", "MÊS", "GRANDE REGIÃO"], axis = 1, inplace = True)
  df = df.rename(columns={'UNIDADE DA FEDERAÇÃO': 'uf', 'PRODUTO': 'product', 'VENDAS':'sales_vol'})
  df.loc[:,'unit'] = 'm^3'
  df=df[["year_month","uf","product","unit","sales_vol"]]
  #merge two dataframes with same index
  df = df[~df['product'].str.contains('ETANOL HIDRATADO', na=False)]
  df3=pd.concat([df,df2], axis=0)
  df['volume'] = pd.to_numeric(df['volume'])
  df3.to_csv('solution_raizen.csv', index=False)


def calculaTabela():
  df2 = pd.read_csv('https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vct/vendas-oleo-diesel-tipo-m3-2013-2021.csv',sep=';')
  df = pd.read_csv('https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vendas-derivados-petroleo-e-etanol/vendas-derivados-petroleo-etanol-m3-1990-2021.csv',sep=';' )
  df = df[~df['PRODUTO'].str.contains('ETANOL HIDRATADO', na=False)]
  num = len(df2)+len(df)
  return num

def valida_teste(ti):
  num = ti.xcom_pull(task_ids='calculaTabela')
  if (num==8652):
    return 'valido'
  return 'invalido'


with DAG("Raizen-ETL-teste",default_args=default_args ,
                 schedule_interval='0 0 * * *',
                 #schedule_interval='@once',	
                 dagrun_timeout=timedelta(minutes=60),
                 description='Raizen challenge solution 2',
                 start_date = airflow.utils.dates.days_ago(1)) as dag:
  extractANP = PythonOperator(
      task_id = 'extractANP',
      python_callable = extractANP
  )
  calculaTabela = PythonOperator(
      task_id = 'calculaTabela',
      python_callable = calculaTabela
  )
  valida_teste = BranchPythonOperator(
    task_id = 'valida_teste',
    python_callable = valida_teste
  )
  valido = BashOperator(
      task_id='valido',
      bash_command = "echo 'Quantidade OK'"
  )

  invalido = BashOperator(
      task_id='invalido',
      bash_command = "echo 'Quantidade não OK'"
  )

extractANP >> calculaTabela >> valida_teste >> [valido, invalido]
