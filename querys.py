import mysql.connector
from mysql.connector import Error

def criar_conexao():
    return mysql.connector.connect(
        host="host name",
        user="user",
        password="senha",
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

### 1.1) Qual o número de leads qualificados (ds_status = 'Qualificado') criados entre 01/12/2020 e 01/08/2022? ###
def obter_numero_de_leads_qualificados():
    consulta = """
    SELECT COUNT(*) as numero_de_leads_qualificados
    FROM ft_lead 
    JOIN dm_status ON ft_lead.sk_status = dm_status.sk_status
    WHERE dm_status.ds_status = 'Qualificado'
    AND ft_lead.dt_created BETWEEN '2020-12-01' AND '2022-08-01';
    """
    return executar_consulta(consulta)

### 1.2) Qual é a média de quantidade de apartamentos por prédio (ds_segmento = 'NH2P_Predios')? ###
def obter_media_apartamentos_por_predio():
    consulta = """
    SELECT AVG(vl_qtd_apartamentos) as media_apartamentos
    FROM ft_lead
    JOIN dm_imovel ON ft_lead.sk_imovel = dm_imovel.sk_imovel
    WHERE dm_imovel.ds_segmento = 'NH2P_Predios';
    """
    return executar_consulta(consulta)

### 1.3) Qual é a cidade e bairro com o maior número de leads qualificados? ###
def obter_cidade_bairro_com_mais_leads_qualificados():
    consulta = """
    SELECT dm_localizacao_lead.ds_cidade, dm_localizacao_lead.ds_bairro, COUNT(*) as numero_de_leads
    FROM ft_lead
    JOIN dm_status ON ft_lead.sk_status = dm_status.sk_status
    JOIN dm_localizacao_lead ON ft_lead.sk_localizacao_lead = dm_localizacao_lead.sk_localizacao_lead
    WHERE dm_status.ds_status = 'Qualificado'
    GROUP BY dm_localizacao_lead.ds_cidade, dm_localizacao_lead.ds_bairro
    ORDER BY numero_de_leads DESC
    LIMIT 1;
    """
    return executar_consulta(consulta)

### 1.4) Qual é o subprojeto com maior média de m² de apartamento nas linhas onde essa coluna foi preenchida? ###
def obter_subprojeto_com_maior_media_m2():
    consulta = """
    SELECT dm_subprojeto.ds_subprojeto, AVG(ft_lead.vl_m2_apartamentos) as media_m2
    FROM ft_lead
    LEFT JOIN dm_subprojeto ON ft_lead.sk_subprojeto = dm_subprojeto.sk_subprojeto
    WHERE ft_lead.vl_m2_apartamentos IS NOT NULL
    GROUP BY dm_subprojeto.ds_subprojeto
    ORDER BY media_m2 DESC
    LIMIT 1;
    """
    return executar_consulta(consulta)

### 1.5) Quais são os 5 subprojetos com o maior número de bairros diferentes? ###
def obter_top5_subprojetos_com_mais_bairros():
    consulta = """
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
    AND dm_subprojeto.ds_subprojeto NOT LIKE 'QTD UDAS:%'
    GROUP BY dm_subprojeto.ds_subprojeto
    ORDER BY numero_de_bairros DESC
    LIMIT 5;
    """
    return executar_consulta(consulta)

if __name__ == "__main__":
    quantidade = obter_numero_de_leads_qualificados()
    if quantidade is not None and len(quantidade) > 0:
        print(f"\nNúmero de leads qualificados criados entre 01/12/2020 e 01/08/2022: {quantidade[0][0]}")

    media_apartamentos = obter_media_apartamentos_por_predio()
    if media_apartamentos is not None and len(media_apartamentos) > 0:
        print(f"\nMédia de quantidade de apartamentos por prédio (ds_segmento = 'NH2P_Predios'): {media_apartamentos[0][0]}")
    
    cidade_bairro = obter_cidade_bairro_com_mais_leads_qualificados()
    if cidade_bairro is not None and len(cidade_bairro) > 0:
        cidade, bairro, numero_de_leads = cidade_bairro[0]
        print(f"\nCidade e bairro com o maior número de leads qualificados: {cidade}, {bairro} com {numero_de_leads} leads")
    
    subprojeto_media_m2 = obter_subprojeto_com_maior_media_m2()
    if subprojeto_media_m2 is not None and len(subprojeto_media_m2) > 0:
        subprojeto, media_m2 = subprojeto_media_m2[0]
        print(f"\nSubprojeto com maior média de m² de apartamento: {subprojeto} com média de {media_m2} m²")

    top5_subprojetos = obter_top5_subprojetos_com_mais_bairros()
    if top5_subprojetos is not None:
        print("\nTop 5 subprojetos com o maior número de bairros diferentes:")
        for subprojeto, numero_de_bairros in top5_subprojetos:
            print(f"{subprojeto}: {numero_de_bairros} bairros")

    
