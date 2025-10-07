import psycopg2
import os

# Conexão com o banco
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS")
)

with conn.cursor() as cur:
    # Criação do schema staging
    cur.execute("CREATE SCHEMA IF NOT EXISTS staging;")
    # Criação do schema analytics
    cur.execute("CREATE SCHEMA IF NOT EXISTS analytics;")

    conn.commit()

    # EMPRESA
    cur.execute("""
        CREATE TABLE IF NOT EXISTS staging.stg_empresa (
            cnpj VARCHAR(20) PRIMARY KEY,
            razao_social TEXT,
            natureza_juridica INTEGER,
            qualificacao_responsavel INTEGER,
            capital_social FLOAT,
            cod_porte VARCHAR(10)
        );
    """)

    # Comentários nas colunas
    cur.execute("""
        COMMENT ON COLUMN staging.stg_empresa.cnpj IS 'CONTEM O NÚMERO DE INSCRIÇÃO NO CNPJ (CADASTRO NACIONAL DA PESSOA JURÍDICA).';
        COMMENT ON COLUMN staging.stg_empresa.razao_social IS 'CORRESPONDE AO NOME EMPRESARIAL DA PESSOA JURÍDICA';
        COMMENT ON COLUMN staging.stg_empresa.natureza_juridica IS 'CÓDIGO DA NATUREZA JURÍDICA';
        COMMENT ON COLUMN staging.stg_empresa.qualificacao_responsavel IS 'QUALIFICAÇÃO DA PESSOA FÍSICA RESPONSÁVEL PELA EMPRESA';
        COMMENT ON COLUMN staging.stg_empresa.capital_social IS 'CAPITAL SOCIAL DA EMPRESA';
        COMMENT ON COLUMN staging.stg_empresa.cod_porte IS 'CÓDIGO DO PORTE DA EMPRESA';
    """)

    conn.commit()

    # SOCIO
    cur.execute("""
        CREATE TABLE IF NOT EXISTS staging.stg_socio (
            cnpj VARCHAR(20),
            tipo_socio INTEGER,
            nome_socio TEXT,
            documento_socio VARCHAR(20),
            codigo_qualificacao_socio VARCHAR(10),
            PRIMARY KEY (cnpj, nome_socio)
        );
    """)

    # Comentários nas colunas
    cur.execute("""
        COMMENT ON COLUMN staging.stg_socio.cnpj IS 'CONTÉM O NÚMERO DE INSCRIÇÃO NO CNPJ (CADASTRO NACIONAL DA PESSOA JURÍDICA).';
        COMMENT ON COLUMN staging.stg_socio.tipo_socio IS 'IDENTIFICADOR DE SÓCIO';
        COMMENT ON COLUMN staging.stg_socio.nome_socio IS 'CORRESPONDE AO NOME DO SÓCIO PESSOA FÍSICA, RAZÃO SOCIAL E/OU NOME EMPRESARIAL DA PESSOA JURÍDICA E NOME DO SÓCIO/RAZÃO SOCIAL DO SÓCIO ESTRANGEIRO';
        COMMENT ON COLUMN staging.stg_socio.documento_socio IS 'É PREENCHIDO COM CPF OU CNPJ DO SÓCIO. NO CASO DE SÓCIO ESTRANGEIRO É PREENCHIDO COM “NOVES”. O ALINHAMENTO PARA CPF É FORMATADO COM ZEROS À ESQUERDA.';
        COMMENT ON COLUMN staging.stg_socio.codigo_qualificacao_socio IS 'CÓDIGO DE QUALIFICAÇÃO DO SÓCIO';
    """)

    conn.commit()

    # DM_EMPRESA
    cur.execute("""
        CREATE TABLE IF NOT EXISTS analytics.dm_empresa (
            cnpj VARCHAR(20) PRIMARY KEY,
            qtde_socios INTEGER,
            flag_socio_estrangeiro BOOLEAN,
            doc_alvo BOOLEAN
        );
    """)

    # Comentários nas colunas
    cur.execute("""
        COMMENT ON COLUMN analytics.dm_empresa.cnpj IS 'CONTÉM O NÚMERO DE INSCRIÇÃO NO CNPJ (CADASTRO NACIONAL DA PESSOA JURÍDICA).';
        COMMENT ON COLUMN analytics.dm_empresa.qtde_socios IS 'NUMERO DE SOCIOS PARTICIPANTES NO CNPJ ';
        COMMENT ON COLUMN analytics.dm_empresa.flag_socio_estrangeiro IS 'True: Contém pelo menos 1 sócio estrangeiro. False: Não contém sócios estrangeiros';
        COMMENT ON COLUMN analytics.dm_empresa.doc_alvo IS 'True: Quando porte da empresa = 03 & qtde_socios > 1. False: Outros';;
    """)

    conn.commit()

print("Schemas e tabelas criadas com sucesso!")

conn.close()
