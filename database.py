import pandas as pd
import mysql.connector
import numpy as np

def create_connection():
    return mysql.connector.connect(
        host="host name",
        user="user",
        password="password",
        database="comgas"
    )

def create_table(cursor, table_name, columns):
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    cursor.execute(f"CREATE TABLE {table_name} ({columns})")

def insert_data(cursor, table_name, data, batch_size=1000):
    placeholders = ", ".join(["%s"] * len(data.columns))
    columns = ", ".join(data.columns)
    sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    for i in range(0, len(data), batch_size):
        batch = data.iloc[i:i+batch_size]
        cursor.executemany(sql, batch.replace({np.nan: None, '': None}).values.tolist())

def main():
    conn = create_connection()
    cursor = conn.cursor()

    # dm_status.csv
    df_status = pd.read_csv("CASE DE/dm_status.csv").fillna('')
    create_table(cursor, "dm_status", "sk_status BIGINT PRIMARY KEY, ds_status VARCHAR(255)")
    insert_data(cursor, "dm_status", df_status)

    # dm_subprojeto.csv
    df_subprojeto = pd.read_csv("CASE DE/dm_subprojeto.csv").fillna('')
    create_table(cursor, "dm_subprojeto", "sk_subprojeto BIGINT PRIMARY KEY, ds_subprojeto VARCHAR(255), cd_pep VARCHAR(255)")
    insert_data(cursor, "dm_subprojeto", df_subprojeto)

    # ft_lead.csv
    df_lead = pd.read_csv("CASE DE/ft_lead.csv").replace('', np.nan).fillna(np.nan)
    create_table(cursor, "ft_lead", "id_lead VARCHAR(255) PRIMARY KEY, dt_created DATE, is_converted BOOLEAN, vl_qtd_apartamentos INT, vl_m2_apartamentos DECIMAL(10, 2), vl_qtd_casas INT, sk_status BIGINT, sk_imovel BIGINT, sk_localizacao_lead BIGINT, sk_subprojeto BIGINT")
    insert_data(cursor, "ft_lead", df_lead)

    # dm_imovel.csv
    df_imovel = pd.read_csv("CASE DE/dm_imovel.csv").fillna('')
    create_table(cursor, "dm_imovel", "sk_imovel BIGINT PRIMARY KEY, ds_segmento VARCHAR(255), ds_tipo_imovel VARCHAR(255)")
    insert_data(cursor, "dm_imovel", df_imovel)

    # dm_localizacao_lead.csv
    df_localizacao = pd.read_csv("CASE DE/dm_localizacao_lead.csv").fillna('')
    create_table(cursor, "dm_localizacao_lead", "sk_localizacao_lead BIGINT PRIMARY KEY, ds_cidade VARCHAR(255), ds_bairro VARCHAR(255)")
    insert_data(cursor, "dm_localizacao_lead", df_localizacao)

    conn.commit()
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()
