# RaizenChallenge

Correction: The Raizen challenge gives wrong data, then the correct data, in fact, is located in another web address.
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
git clone https://github.com/masuta16/RaizenChallenge cd etl-raizen 
docker-compose up --build 
access on browser 
http://localhost:8080
```




