import mysql.connector
from mysql.connector import Error

def criar_conexao():
    return mysql.connector.connect(
        host="localhost",
        user="root",
        password="Sh@dow2001",
        database="comgas"
    )

def executar_consulta(consulta):
    try:
        with criar_conexao() as conn:
            with conn.cursor() as cursor:
                cursor.execute(consulta)
                resultado = cursor.fetchall()
                return resultado
    except Error as e:
        print(f"Erro ao executar a consulta: {e}")
        return None

# Dicionário de consultas
consultas = {
    "numero_de_leads_qualificados": """
        SELECT COUNT(*) as numero_de_leads_qualificados
        FROM ft_lead 
        JOIN dm_status ON ft_lead.sk_status = dm_status.sk_status
        WHERE dm_status.ds_status = 'Qualificado'
        AND ft_lead.dt_created BETWEEN '2020-12-01' AND '2022-08-01';
    """,
    "media_apartamentos_por_predio": """
        SELECT AVG(vl_qtd_apartamentos) as media_apartamentos
        FROM ft_lead
        JOIN dm_imovel ON ft_lead.sk_imovel = dm_imovel.sk_imovel
        WHERE dm_imovel.ds_segmento = 'NH2P_Predios';
    """,
    "cidade_bairro_com_mais_leads_qualificados": """
        SELECT dm_localizacao_lead.ds_cidade, dm_localizacao_lead.ds_bairro, COUNT(*) as numero_de_leads
        FROM ft_lead
        JOIN dm_status ON ft_lead.sk_status = dm_status.sk_status
        JOIN dm_localizacao_lead ON ft_lead.sk_localizacao_lead = dm_localizacao_lead.sk_localizacao_lead
        WHERE dm_status.ds_status = 'Qualificado'
        GROUP BY dm_localizacao_lead.ds_cidade, dm_localizacao_lead.ds_bairro
        ORDER BY numero_de_leads DESC
        LIMIT 1;
    """,
    "subprojeto_com_maior_media_m2": """
        SELECT dm_subprojeto.ds_subprojeto, AVG(ft_lead.vl_m2_apartamentos) as media_m2
        FROM ft_lead
        JOIN dm_subprojeto ON ft_lead.sk_subprojeto = dm_subprojeto.sk_subprojeto
        WHERE ft_lead.vl_m2_apartamentos IS NOT NULL
        GROUP BY dm_subprojeto.ds_subprojeto
        ORDER BY media_m2 DESC
        LIMIT 1;
    """,
    "top5_subprojetos_com_mais_bairros": """
    SELECT 
        dm_subprojeto.ds_subprojeto, 
        COUNT(DISTINCT dm_localizacao_lead.ds_bairro) AS numero_de_bairros
    FROM ft_lead
    JOIN dm_subprojeto 
        ON ft_lead.sk_subprojeto = dm_subprojeto.sk_subprojeto
    JOIN dm_localizacao_lead 
        ON ft_lead.sk_localizacao_lead = dm_localizacao_lead.sk_localizacao_lead
    WHERE dm_localizacao_lead.ds_bairro IS NOT NULL
    AND dm_localizacao_lead.ds_bairro NOT IN ('', 'LIMPEZA DE BASE')
    AND dm_subprojeto.ds_subprojeto IS NOT NULL -- Exclui valores nulos
    AND dm_subprojeto.ds_subprojeto NOT IN ('', '-', '.', '..', 'LIMPEZA DE BASE - CLIENTES NA REDE - CASAS') -- Exclui valores inválidos específicos
    AND dm_subprojeto.ds_subprojeto NOT REGEXP '^[0-9]+$' -- Exclui valores compostos apenas por números
    AND dm_subprojeto.ds_subprojeto REGEXP '[A-Za-z]' -- Garante que o valor contenha pelo menos uma letra
    AND dm_subprojeto.ds_subprojeto NOT LIKE 'QTD UDAS:%' -- Exclui valores que começam com "QTD UDAS"
    GROUP BY dm_subprojeto.ds_subprojeto
    ORDER BY numero_de_bairros DESC
    LIMIT 5;
    """
}

def obter_resultado(chave_consulta):
    consulta = consultas.get(chave_consulta)
    if consulta:
        return executar_consulta(consulta)
    else:
        print(f"Consulta com chave '{chave_consulta}' não encontrada.")
        return None

if __name__ == "__main__":
    quantidade = obter_resultado("numero_de_leads_qualificados")
    if quantidade:
        print("\nNúmero de leads qualificados criados entre 01/12/2020 e 01/08/2022:")
        print(f"  {quantidade[0][0]}")

    media_apartamentos = obter_resultado("media_apartamentos_por_predio")
    if media_apartamentos:
        print("\nMédia de quantidade de apartamentos por prédio (ds_segmento = 'NH2P_Predios'):")
        print(f"  {media_apartamentos[0][0]}")

    cidade_bairro = obter_resultado("cidade_bairro_com_mais_leads_qualificados")
    if cidade_bairro:
        cidade, bairro, numero_de_leads = cidade_bairro[0]
        print("\nCidade e bairro com o maior número de leads qualificados:")
        print(f"  Cidade: {cidade}")
        print(f"  Bairro: {bairro}")
        print(f"  Número de leads: {numero_de_leads}")

    subprojeto_media_m2 = obter_resultado("subprojeto_com_maior_media_m2")
    if subprojeto_media_m2:
        subprojeto, media_m2 = subprojeto_media_m2[0]
        print("\nSubprojeto com maior média de m² de apartamento:")
        print(f"  Subprojeto: {subprojeto}")
        print(f"  Média de m²: {media_m2}")

    top5_subprojetos = obter_resultado("top5_subprojetos_com_mais_bairros")
    if top5_subprojetos:
        print("\nTop 5 subprojetos com o maior número de bairros diferentes:")
        for subprojeto, numero_de_bairros in top5_subprojetos:
            print(f"  {subprojeto}: {numero_de_bairros} bairros")