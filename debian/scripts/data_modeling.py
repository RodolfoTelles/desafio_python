import os
from sqlalchemy import (
    create_engine, MetaData, Table, Column, String, Integer, Float, Boolean, text, PrimaryKeyConstraint
)

DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(DATABASE_URL)
metadata = MetaData()

# Criação dos schemas
with engine.begin() as conn:
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging;"))
    conn.execute(text("CREATE SCHEMA IF NOT EXISTS analytics;"))

# STG_EMPRESA
stg_empresa = Table("stg_empresa", metadata,
    Column("cnpj", String(20), primary_key=True, comment="CONTEM O NÚMERO DE INSCRIÇÃO NO CNPJ (CADASTRO NACIONAL DA PESSOA JURÍDICA)."),
    Column("razao_social", String, comment="CORRESPONDE AO NOME EMPRESARIAL DA PESSOA JURÍDICA"),
    Column("natureza_juridica", Integer, comment="CÓDIGO DA NATUREZA JURÍDICA"),
    Column("qualificacao_responsavel", Integer, comment="QUALIFICAÇÃO DA PESSOA FÍSICA RESPONSÁVEL PELA EMPRESA"),
    Column("capital_social", Float, comment="CAPITAL SOCIAL DA EMPRESA"),
    Column("cod_porte", String(10), comment="CÓDIGO DO PORTE DA EMPRESA"),
    schema="staging"
)
# STG_SOCIO
stg_socio = Table("stg_socio", metadata,
    Column("cnpj", String(20), comment="CONTÉM O NÚMERO DE INSCRIÇÃO NO CNPJ (CADASTRO NACIONAL DA PESSOA JURÍDICA)."),
    Column("tipo_socio", Integer, comment="IDENTIFICADOR DE SÓCIO"),
    Column("nome_socio", String, comment="CORRESPONDE AO NOME DO SÓCIO PESSOA FÍSICA, RAZÃO SOCIAL E/OU NOME EMPRESARIAL DA PESSOA JURÍDICA E NOME DO SÓCIO/RAZÃO SOCIAL DO SÓCIO ESTRANGEIRO"),
    Column("documento_socio", String(20), comment="É PREENCHIDO COM CPF OU CNPJ DO SÓCIO. NO CASO DE SÓCIO ESTRANGEIRO É PREENCHIDO COM “NOVES”. O ALINHAMENTO PARA CPF É FORMATADO COM ZEROS À ESQUERDA."),
    Column("codigo_qualificacao_socio", String(10), comment="CÓDIGO DE QUALIFICAÇÃO DO SÓCIO"),
    schema="staging"
)

# DM_EMPRESA
dm_empresa = Table("dm_empresa", metadata,
    Column("cnpj", String(20), primary_key=True, comment="CONTÉM O NÚMERO DE INSCRIÇÃO NO CNPJ (CADASTRO NACIONAL DA PESSOA JURÍDICA)."),
    Column("qtde_socios", Integer, comment="NUMERO DE SOCIOS PARTICIPANTES NO CNPJ"),
    Column("flag_socio_estrangeiro", Boolean, comment="True: Contém pelo menos 1 sócio estrangeiro. False: Não contém sócios estrangeiros"),
    Column("doc_alvo", Boolean, comment="True: Quando porte da empresa = 03 & qtde_socios > 1. False: Outros"),
    schema="analytics"
)

metadata.create_all(engine)
print("Schemas e tabelas criadas com sucesso!")
