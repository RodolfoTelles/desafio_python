#!/bin/bash

# Cria e ativa o ambiente virtual
python3 -m venv venv
. venv/bin/activate

# Instala as dependências já no ambiente virtual
pip3 install --no-cache-dir --prefer-binary -r /home/desafio_python/scripts/requirements.txt

# Script que faz Download dos arquivos ZIP e extrai o CSV
# Aqui estou considerando ser a camada Landing do Data Lake
python3 /home/desafio_python/scripts/downloads_csv.py

# Script para criação dos Schemas e tabelas no banco de dados 
python3 /home/desafio_python/scripts/data_modeling.py

# Script lendo da camada Landing e inserindo na Staging já tratando as informações
python3 /home/desafio_python/scripts/staging.py

# Script lendo da Staging e inserindo na camada Analytics já pronto para uso
python3 /home/desafio_python/scripts/analytics.py

echo "Pipeline executado com sucesso!"

echo "Para validar os dados, acesse o PostgreSQL com as credenciais no arquivo .env"