## Solução do problema proposto

## Status do projeto
:heavy_check_mark: completo

## Tabela de conteúdos
- [Objetivo](#Objetivo)
- [Processo](#Processo)
- [Desenvolvimento](#Desenvolvimento)
- [Processo de aprendizado](#Processo-de-aprendizado)
- [Melhorias](#Melhorias)

## Objetivo

Criar ELT para transformar dados de COVID-19 para camadas 'trusted' e 'refined' utilizando
o Airflow para arquitetura da pipeline de dados e Pyspark para a transformação desses dados.


## Processo
- Extração dos arquivos na camada 'Raw';
- Criação dos scripts para tratamento das camadas refined e trusted;
- Aplicação das tasks na Dag Airflow;
- Validação da pipeline e testes;


## Desenvolvimento

Para o desafio proposto, foram realizadas algumas modificações em relação a 
estrutura original, sendo elas:

- Modelo de pastas foi organizado para contemplar dags relacionadas a aquele tema: dags/covid19/
- Adicionado uma camada intermediária de dados denominada 'processed'
- File sensor, simulando uma leitura diária de um diretório X
- Adicionado variáveis Airflow
- Adição de 'TaskGroup' para execução da tarefa, tornando o uso da função de processamento mais genérico

#### 1.0 Pastas

A estrutura de pastas ficou da seguinte forma:

![Folders](/_img/organizacao_folders.png?raw=true "Folders")


#### 1.1 Modelagem

Como houve uso de diversas funções de transformações, (exemplo: dataframe melt, regex para encontrar
as datas nas colunas) foi adicionado pasta *utils/* para armazenar scripts de transformação do dado. Tanto a transformação quanto as funções auxiliares para essa modelagem estão no arquivo 
*dags/covid19/utils/preprocess.py*, afim de deixar o arquivo da dag *dags/covid19/dags/covid19.py*
mais legível, dedicando esse arquivo somente para descrever as tasks de execução.
As variáveis que podem ser setadas na UI Airflow estão disponíveis em *dags/covid19/utils/variables.json*
exemplo abaixo:

![variables](/_img/variables.png?raw=true "variables")

Como descrito, foi adicionado uma camada intermediária de dados, 'processed',
essa camada é gerada a partir de uma função genérica onde aceita qualquer tipo de arquivo 
que possui estrutura similar e é armazenado em pastas com seus nomes subsequentes.

Isso ajuda de que forma?
Caso alguma task da Dag venha a falhar, ficaria mais fácil identificar em qual arquivo ocorreu a falha
(Lembrando que não precisaria necessariamente criar uma nova camada, poderia seguir com processamento até as camadas trusted, mas optei seguir dessa forma assegurando o resultado), e ficaria claro na UI do Airflow em qual parte isso poderia ter ocorrido.


#### 1.2 Dag Airflow

A pipeline foi arquitetada da seguinte forma:

1. Checagem se existe diretório com csv's dentro
2. task group (extração e transformação de cada csv)
3. tranformação camada 'trusted'
4. tranformação camada 'refined'

A tarefa em taskgroup ocorre em paralelo, ou seja, os 3 cvs sao processados ao mesmo tempo.
Para as camadas trusted e refined, são somente ativadas se todas as tarefas anteriores resultaram
em sucesso 'all_success'.

![grafo](/_img/grafo.png?raw=true "grafo")


## Processo de aprendizado

A leitura das colunas de datas foi algo que me desafiou e testou como poderia resolver.



### Melhorias
 - Processo de média movel com lag não está agrupado, como não existem instruções no guia acabei deixando sem, mas seria interessante verificar a media se comportando agrupado por pais,estado, etc.
 

## Autor
Lucas Angulski 


  
  



