import os
import re
import pandas as pd
from sqlalchemy import create_engine

# Conexão com o banco
DATABASE_URL = f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASS')}@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
engine = create_engine(DATABASE_URL)

# Leitura das tabelas
df_empresa = pd.read_sql_table("stg_empresa", con=engine, schema="staging")
df_socio = pd.read_sql_table("stg_socio", con=engine, schema="staging")

# Join entre empresa e sócio
df_joined = pd.merge(df_empresa, df_socio, on="cnpj", how="inner")

# Agrupamento por empresa
df_aggregated = df_joined.groupby("cnpj").agg(
    qtde_socios=("nome_socio", "count"),
    flag_socio_estrangeiro=("documento_socio", lambda x: any(re.search(r".*999999.*", str(doc)) for doc in x))
).reset_index()

# Coluna doc_alvo
df_aggregated = pd.merge(df_aggregated, df_empresa[["cnpj", "cod_porte"]], on="cnpj", how="left")
df_aggregated["doc_alvo"] = df_aggregated.apply(
    lambda row: row["cod_porte"] == "3" and row["qtde_socios"] > 1,
    axis=1
)

# Seleção final
df_final = df_aggregated[["cnpj", "qtde_socios", "flag_socio_estrangeiro", "doc_alvo"]]

# Inserção no banco (schema analytics)
df_final.to_sql("dm_empresa", con=engine, schema="analytics", if_exists="replace", index=False, chunksize=1000, method="multi")

print("Tabela dm_empresa criada com sucesso!")