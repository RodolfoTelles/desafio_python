import os
import pandas as pd
from sqlalchemy import create_engine

# Caminho dos arquivos CSV
CSV_PATH = "./data_files/"

engine = create_engine(f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}")

# EMPRESA - Leitura do CSV 
df_emp = pd.read_csv(CSV_PATH + 'K3241.K03200Y2.D50913.EMPRECSV', sep=';', header=None, encoding='latin1', usecols=range(6),
                 names=[
                     'cnpj', 'razao_social', 'natureza_juridica',
                     'qualificacao_responsavel', 'capital_social', 'cod_porte'
                 ])

# Ajuste no capital_social (de "100,00" para 100.00)
df_emp['capital_social'] = df_emp['capital_social'].str.replace(',', '.').astype(float)

# Inserção no banco
df_emp.to_sql('stg_empresa', schema='staging', con=engine, if_exists='append', index=False)

print("Dados de EMPRESA inseridos com sucesso!")

# SOCIO - Leitura do CSV 
df_soc = pd.read_csv(CSV_PATH + 'K3241.K03200Y2.D50913.SOCIOCSV', sep=';', header=None, encoding='latin1', usecols=range(5),
                 names=[
                     'cnpj', 'tipo_socio', 'nome_socio',
                     'documento_socio', 'codigo_qualificacao_socio'
                 ])

# Inserção no banco
df_soc.to_sql('stg_socio', schema='staging', con=engine, if_exists='append', index=False)

print("Dados de SOCIO inseridos com sucesso!")