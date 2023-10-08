Download dataset:
<link> https://postgrespro.com/community/demodb

Steps:
    1. Banco de dados no docker
        <!-- Criação da imagem do postgres -->
        * docker pull postgres:16.0
        * docker build -t my-postgres-db .
        <!-- Alterar o nome do database de 'demo' para 'airlinesdb' no arquivo 'demo-medium-en\demo-medium-en-20170815.sql' -->
        <!-- Cria o conteiner e executa o script criando o airlinesdb -->
        * docker run --name postgresdb -p 8080:5432 -d my-postgres-db
        <!-- Faz conexão com o airlinesdb pelo psql -->
        * docker exec -it postgresdb psql -U postgres -d airlinesdb
        <!-- Conecta ao servidor postgres -->
        * docker exec -it postgresdb bash
        <!-- Usar para verificar os parametros de conexao para conexão do dbeaver -->
        * docker inspect postgresdb
    2. Extração de dados com airflow
    3. Transformação de dados com pandas
    4. Prefect fazendo orquestração
    5. PowerBI consumindo os dados
