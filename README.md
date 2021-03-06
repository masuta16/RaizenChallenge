# Raizen data engineering challenge

Correction: The Raizen data engineering challenge gives wrong data, then the correct data, in fact, is located in another web address.
Here's the link to the database files:
`https://dados.gov.br/dataset/vendas-de-derivados-de-petroleo-e-biocombustiveis`


The metadata was missed too, but here is:

Metadata 1 has petroleum derivates by UF and products:
`https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vendas-derivados-petroleo-e-etanol/metadados-vendas-derivados-petroleo-etanol.pdf`

*The metadata PDF file has a description of columns inside the CSV file required.

Link for CSV equivalent file:
`https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vendas-derivados-petroleo-e-etanol/vendas-derivados-petroleo-etanol-m3-1990-2021.csv`

Metadata 2 contains data information about sales of diesel by UF and type:
`https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vct/metadados-vendas-oleo-diesel-por-tipo.pdf`

Link for CSV equivalent file:
`https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/vdpb/vct/vendas-oleo-diesel-tipo-m3-2013-2021.csv`


To execute this test follow up the instructions bellow:
Firstly you need docker installed on you computer then you should follow the instructions on this link to install docker
https://docs.docker.com/engine/install/
After that type those commands bellow on your terminal
```
git clone https://github.com/masuta16/RaizenChallenge
cd RaizenChallenge 
docker-compose up --build 
access on browser 
http://localhost:8080
```
Here's an image of airflow dag in graph view

![alt text](https://raw.githubusercontent.com/masuta16/RaizenChallenge/main/images/Screenshot%20from%202022-01-30%2002-48-47.png)

## Schema

Data was stored in the following format:

| Column       | Type        |
| :-:          | :-:         |
| `year_month` | `date`      |
| `uf`         | `string`    |
| `product`    | `string`    |
| `unit`       | `string`    |
| `sales_vol`  | `int`       |
<!-- | `created_at` | `timestamp` | -->

## ???? Links

- [Webpage with Raizen challenge](https://github.com/raizen-analytics/data-engineering-test)
- [About Raizen company](https://www.raizen.com.br/sobre-a-raizen)
- [Apache Airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/)
- [Pandas library documentation](https://pandas.pydata.org/docs/reference/)
- [How to install Docker](https://docs.docker.com/engine/install/)
- [Python 3 documentation](https://docs.python.org/3/)
- [Let's talk about DAGs in Apache Airflow(Portuguese)](https://medium.com/@mestre15/vamos-falar-de-dags-no-apache-airflow-3e1d78c968d7)
- [My awesome apache Airflow list in portuguese](https://github.com/masuta16/awesome-apache-airflow-br)







