Para iniciar os containers basta executar um "docker compose up" na raiz do projeto e toda a estrura do projeto irá executar sozinha! 
Execução total de 3-6 min a depender da maquina.

O desafio é composto de dois containers:

  1 - Debian Python para rodar os scripts
  
  2 - PostgreSQL onde serão armazenados os dados.

Na criação dos containers, são utilizadas as variáveis de ambiente com as credenciais de acesso no arquivo **.env**.

**Obs:** Apesar de ser boas praticas ignorar o arquivo .env com o .gitignore, deixei para que ficasse mais simples a execução.

A estrutura de diretórios visa organizar apenas a estrutura do Debian que executará o python, já que o container PostgreSQL nao tem configurações adicionais.
Dentro do diretório debian vemos o Dockerfile do Debian Python, o diretório scripts que possuem todo projeto que será executado e o diretório data_files que é compartilhado com a maquina local e permite ver os arquivos após Download.

O arquivo mais importante que irá guiar a execução do desafio é o **pipeline.sh**. 
Ele que será o responsável por chamar os scripts python em sua ordem de execução.

**Obs:** Como o desafio tem como objetivo avaliar a capacidade de utilização do python da forma mais direta e simplificada, optei por não utilizar algum framework de orquestração como o Airflow ou DBT e deixei tudo direto no pipeline.sh

Para utilizar o modelo medalhao, considerei que o Download dos arquivos deveriam ser jogados em um Datalake as-is e esta seria a camada **landing** e as camadas **staging** e **analytics** foram criadas no postgreSQL, 
onde a camada **staging** seria responsável pelo cleanse, transformações e afins, e a camada **analytics** já seria a camada utilizada para consumo do dado.

Para auxiliar no processo de validação dos dados segue exemplos de query para ser executada no PostgreSQL na tabela dm_empresa:

select * from analytics.dm_empresa where flag_socio_estrangeiro = true;

select * from analytics.dm_empresa where doc_alvo  = true;
