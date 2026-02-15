# ============================================================================================================================================
# ANALISA CONVERSAS DOS LEADS COM AI E SUGERE UM NOVO STATUS PRO LEAD
# ============================================================================================================================================
import os, sys, aiohttp, json, asyncio, uvloop, re, unicodedata
from pathlib import Path
from datetime import datetime, time, timezone

_app = Path(__file__).resolve().parent
for _sub in ("logging", "db", "utils", "api"):
    sys.path.append(str(_app / _sub))
from log_config import logger, log_ip, log_host, log_action, log_metadata
from db_connections_async import conecta_leaper_db_core, conecta_leaper_db_gateway, conecta_leaper_db_evo
from utils import serializa_metadata
from leaper_core_apis import get_auth_token_company_leaper, change_lead_status, send_lead_conversion_value


# ========================================================================================================================================================================
# CONFIGURAÇÕES DE AMBIENTE
# ========================================================================================================================================================================
# projeto
SERVER_IP = os.getenv("SERVER_IP")

# serviço
SERVER_HOST = os.getenv("SERVER_HOST")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
WORKERS_GLOBAIS = 4



# ================================================================================================================================================================================
# VARIÁVEIS
# ================================================================================================================================================================================

# QTD MÁX DE LEADS POR EXECUÇÃO ------------------------------------------------------------------------------------------------------------------------------------------------
MAX_LEADS_POR_RODADA = 100

# DEFAULT DE CONTEXTO DO NEGÓCIO ---------------------------------------------------------------------------------------------------------------------------------------------
DEFAULT_BUSINESS_CONTEXT = "Negócio local que atende via WhatsApp. Interesse real dos clientes pelos produtos/serviços se manifesta através de perguntas específicas sobre o produto/serviço, não curiosidade geral."

# DEFAULT DE DESCRIÇÃO PARA CADA STATUS -----------------------------------------------------------------------------------------------------------------------------------
DEFAULT_STATUS_CODE_DESCRIPTIONS = {
    "LEAD_START": "Status inicial de todo lead. É o primeiro contato, geralmente.",
    "CONTATO_INICIADO": "A conversa começou a se desenrolar e realmente é um lead verdadeiro (não é spam etc)",
    "QUALIFICACAO": "O lead é qualificado",
    "NEGOCIACAO": "Existe uma negociação acontecendo entre lead e empresa",
    "PROPOSTA_ENVIADA": "Foi enviada uma proposta/orçamento ao lead",
    "END_WON": "Lead foi convertido (pagou de fato) - só quando vc tiver certeza com evidências",
    "END_LOST": "Lead não foi convertido - status final caso tenha certeza que o lead está perdido"
}

# STATUS DE EMOJIS ------------------------------------------------------------------------------------------------------------------------------------------------------------------------
STATUS_EMOJIS = {
    "LEAD_START": "⏳",
    "CONTATO_INICIADO": "▶️",
    "QUALIFICACAO": "📝",
    "NEGOCIACAO": "💱",
    "PROPOSTA_ENVIADA": "➡️",
    "END_WON": "✅",
    "END_LOST": "❌"
}

# GEMINI ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
GEMINI_TIMEOUT = 120
GEMINI_MAX_RETRIES = 3
GEMINI_MODELO = "gemini-3-flash-preview"
GEMINI_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODELO}:generateContent"
LIMITE_TOKENS_MODELO = 1000000
QTD_MEDIA_CARACTERES_POR_TOKEN = 4

# PROMPT --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
PROMPT = f"""Você é especialista em analisar troca de mensagens entre lead (potencial cliente) e empresa, o seu objetivo é classificar a conversa em status pré-determinados de acordo com o desenrolar da conversa.

# Regras
- NUNCA pule etapas.
- Para executar o seu objetivo, você deve seguir de forma meticulosa e rigorosa os passos um a um do <workflow>.
- Na hora de classificar uma conversa, existem apenas esses tipos de classificação, cada um se referindo a um determinado momento da negociação: PLACEHOLDER_STATUS_DISPONIVEIS_COM_DESCRICAO.

# Workflow 
Siga os passos abaixo para classificar corretamente cada conversa.

<workflow>
Passo #1 - Recebimento das Mensagens  
Toda vez que você for acionado, receberá dados no seguinte formato (exemplo de valores):

STATUS_ATUAL: EM ANDAMENTO  
EMPRESA (dd/mm/yyyy hh:mm:ss): Mensagem 01  
LEAD (dd/mm/yyyy hh:mm:ss): Mensagem 02  
EMPRESA (dd/mm/yyyy hh:mm:ss): Mensagem 03  
LEAD (dd/mm/yyyy hh:mm:ss): Mensagem 04  

Importante: STATUS_ATUAL contém o status atual da conversa.

Passo #2 - Análise das Mensagens  
Para analisar se o STATUS_ATUAL será alterado ou não:
1. Analise/considere TODAS as mensagens disponíveis, de TODOS os remetentes. Sempre considere TODO o contexto, e não apenas palavras isoladas.
2. Ao analisar, seja crítico e considere não apenas o conteúdo das mensagens, mas principalmente quem as enviou e o intervalo de tempo entre as mensagens da empresa e do lead. Ou seja, considere a sequência e profundidade das mensagens.
3. Julgue, a partir da sua especialidade, se é mais coerente manter o STATUS_ATUAL ou alterar o status para uma das possibilidades de classificação mencionadas anteriormente.
 
Passo #3 - Conclusão Final  
1. Após receber o STATUS_ATUAL e as mensagens conforme o modelo descrito no Passo #1, analise conforme orientado no Passo #2 e, por fim, emita uma conclusão final.
2. A sua conclusão final da análise deve ser exatamente igual a uma das opções a seguir, somente elas e nunca nada diferente: PLACEHOLDER_EXEMPLO_STATUS.
3. Além da conclusão da análise, você deve também extrair o nome do lead. Caso não encontre, deixe o output vazio. Jamais invente um nome.
4. Você também deve extrair o preço do orçamento/oferta enviado pela EMPRESA, essa informação deve estar explícita, evite quaisquer suposições, só extraia se tiver certeza. Caso não encontre, não adicione a chave "valor" no JSON. Caso encontre um valor, use-o como valor float da chave "valor". Jamais invente um valor. 
5. Você precisa também preencher o campo ai_confidence_level_output, que remete à quanto, de 0 a 100, você acredita que a sua análise é confiável. Quanto mais faltarem informações para você classificar em algum dos status, mais próximo de 0 deve ser. Quanto mais completas forem as informações e mais clareza tiver sobre sua classificação, mais próximo de 100 deve ser.
6. Preencha também o campo "analise_ai", com um resumo da sua análise e motivos considerados para escolher o status que escolheu.
7. Sua resposta deve SEMPRE ser em JSON e seguir rigorosamente o <formato> a partir do <exemplo> abaixo.

Exemplo:
<exemplo>
{{"ai_suggestion_status_name": "EM ANDAMENTO", "nome_lead": "João", "valor": 500.99, "ai_confidence_level_output": "80", "analise_ai": "Parece que João ainda está escolhendo os produtos que quer comprar"}}
</exemplo>
 
Formato:
<formato>
{{"ai_suggestion_status_name": "PLACEHOLDER_EXEMPLO_STATUS", "nome_lead": "Nome do lead caso encontrar, se não, deixe vazio", "valor": 200.00, "ai_confidence_level_output": "String contendo número com até duas casas decimais, referente à confiança da resposta em relação ao contexto das mensagens fornecido", "analise_ai": "Descrição da sua análise, com os motivos pelos quais escolheu a opção que escolheu"}}
</formato>

</workflow>

<contexto_empresa>
Considere este contexto a respeito da empresa e de suas atividades para que você possa tomar melhores decisões: PLACEHOLDER_BUSINESS_CONTEXT
</contexto_empresa>

# Importante
- A sua conclusão deve conter APENAS uma das opções (PLACEHOLDER_EXEMPLO_STATUS); NUNCA dê observações ou comentários adicionais.
- SEMPRE o output deve seguir o <formato> a partir do <exemplo>.
- SEMPRE responda em JSON.
- Em seu output você SEMPRE deve informar qual o "ai_suggestion_status_name" (SEMPRE), "ai_confidence_level_output" (SEMPRE), "analise_ai" (SEMPRE), o "nome_lead" (se disponível), e "valor" (se disponível).
"""



# ================================================================================================================================================================================
# CHECA SE ESTA DENTRO DO HORARIO
# ================================================================================================================================================================================
async def checa_se_esta_dentro_do_horario():
    """Garante que o serviço só rode em horário comercial (seg-sex, 9h-19h)."""
    now = datetime.now()
    weekday = now.weekday()  # segunda=0, sexta=4
    hora_atual = now.time()
    dentro_do_horario = (
        0 <= weekday <= 4 and  # Segunda a Sexta
        time(9, 0) <= hora_atual <= time(19, 0)                
    )
    return dentro_do_horario
    
    
# ================================================================================================================================================================================
# LIMITAR HISTÓRICO DE CONVERSAS A N CARACTERES
# ================================================================================================================================================================================
def limitar_mensagens(mensagens_formatadas):
    """Limita histórico enviado à IA pelo tamanho máximo de contexto do modelo."""

    # Define o limite de caracteres baseado no modelo
    limite_caracteres = (LIMITE_TOKENS_MODELO / QTD_MEDIA_CARACTERES_POR_TOKEN) - 100

    # Junta todas as mensagens em um único texto
    texto_completo = "\n".join(mensagens_formatadas)

    # Se o texto completo estiver dentro do limite, retorne-o
    if len(texto_completo) <= limite_caracteres:
        return texto_completo

    # Caso contrário, limita as mensagens do fim para o início, considerando somente as linhas que
    # começam com "EMPRESA" ou "LEAD"
    linhas_validas = []
    for linha in reversed(mensagens_formatadas):
        if linha.startswith("EMPRESA") or linha.startswith("LEAD"):
            # Insere no início da lista
            linhas_validas.insert(0, linha)
            # Verifica se o texto atual excede o limite
            if len("\n".join(linhas_validas)) > limite_caracteres:
                linhas_validas.pop(0)  # Remove a mensagem mais antiga, se ultrapassar o limite
                break

    return "\n".join(linhas_validas)
    


# Normaliza textos para comparação de keywords: remove acentos, pontuação e emojis,
# mantém apenas letras, números e espaços, e colapsa múltiplos espaços.
def normaliza_texto_para_kw(texto):
    """Remove acentos/pontuação para comparação de keywords insensível a caso/acentos."""
    if not texto:
        return ""
    texto = unicodedata.normalize("NFKD", texto)
    partes = []
    for ch in texto.lower():
        categoria = unicodedata.category(ch)
        if categoria.startswith("L") or categoria.startswith("N"):
            partes.append(ch)
        elif ch.isspace():
            partes.append(" ")
        else:
            partes.append(" ")
    texto_normalizado = "".join(partes)
    texto_normalizado = re.sub(r"\s+", " ", texto_normalizado).strip()
    return texto_normalizado


def to_naive_datetime(dt):
    """Converte datetimes para naive (UTC) para comparações simples."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def extrai_valor_conversao(texto):
    """Extrai valor monetário flexível (R$, ponto/vírgula, com/sem decimais) de um texto."""
    if not texto:
        return None
    texto = texto.lower()
    match = re.search(r"(?:r\\$\\s*)?([0-9]{1,3}(?:[\\.,][0-9]{3})*(?:[\\.,][0-9]{2})|[0-9]+(?:[\\.,][0-9]{2})?)", texto)
    if not match:
        return None
    bruto = match.group(1)
    # Identifica separador decimal como o último ponto ou vírgula
    last_dot = bruto.rfind(".")
    last_comma = bruto.rfind(",")
    if last_dot == -1 and last_comma == -1:
        try:
            return float(bruto)
        except Exception:
            return None
    if last_dot > last_comma:
        sep = "."
        pos = last_dot
    else:
        sep = ","
        pos = last_comma
    int_part = re.sub(r"[\\.,]", "", bruto[:pos])
    dec_part = re.sub(r"[\\.,]", "", bruto[pos + 1 :])
    if dec_part == "":
        dec_part = "0"
    try:
        return float(f"{int_part}.{dec_part}")
    except Exception:
        return None



# ================================================================================================================================================================================
# CONSULTA LISTA DE INSTANCIAS 
# ================================================================================================================================================================================
async def consulta_instancias_wpp(db_gateway):
    """
    Consulta todas as companies e suas instâncias associadas,
    retornando agrupadas no formato esperado:
    [
        {"evo_db_host": "db-whatsapp-postgresql", "evo_db_name": "evolution_n2", "companies_ids": [1, 2, 3]},
        {"evo_db_host": "db-evolution-intances-v1", "evo_db_name": "evolution_n4", "companies_ids": [4, 5]},
        ...
    ]
    """
    # -------------------------------------------------------------------
    # 1. Query: busca todas as instâncias válidas
    # -------------------------------------------------------------------
    query_gateway = """
        SELECT *
        FROM (
          SELECT DISTINCT ON (company_id)
            company_id,
            json_raw::json->>'instanceHost' AS instance_host,
            json_raw::json->>'instanceNode' AS instance_node
          FROM "instance"
          WHERE company_id IS NOT NULL
            AND company_id != '304dc0de-000d-4449-8602-60cc7ab9dbf8'    -- BRAINCO
            -- AND company_id = '00b4190c-9c5f-4afe-aaf7-47bbd9d5356f'     -- CREATTIVE
          ORDER BY company_id, created_at DESC, id DESC
        ) t
        ORDER BY instance_host NULLS LAST, instance_node NULLS LAST
    """

    async with db_gateway.connection() as conn:
        rows = await asyncio.wait_for(
            conn.fetch_all(query=query_gateway),
            timeout=30
        )

    # -------------------------------------------------------------------
    # 2. Normaliza resultados e agrupa por (host, nome do banco)
    # -------------------------------------------------------------------
    agrupado = {}  # chave: (evo_db_host, evo_db_name) -> lista de company_id

    for row in rows:
        company_id = str(row["company_id"])
        instance_node = (row["instance_node"] or "").strip() if row["instance_node"] else None
        instance_host = (row["instance_host"] or "").strip() if row["instance_host"] else None

        # Fallback do host conforme regras
        if not instance_host:
            instance_host = "db-evolution-intances-v1-do-user-16037229-0.i"

        # Deriva o nome do DB a partir do node (mantendo tua lógica atual)
        if not instance_node:
            evo_db_name = "defaultdb"
        else:
            node_suffix = instance_node.split(".")[-1]  # ex: v1.n2 -> "n2"
            evo_db_name = f"evolution_{node_suffix}"

        chave = (instance_host, evo_db_name)
        agrupado.setdefault(chave, []).append(company_id)

    # -------------------------------------------------------------------
    # 3. Monta lista final no formato esperado
    # -------------------------------------------------------------------
    instancias_companies = [
        {
            "evo_db_host": host,
            "evo_db_name": dbname,
            "companies_ids": companies
        }
        for (host, dbname), companies in agrupado.items()
    ]

    # Caso nenhuma instância encontrada
    if not instancias_companies:
        instancias_companies = [{
            "evo_db_host": "db-whatsapp-postgresql",
            "evo_db_name": "defaultdb",
            "companies_ids": []
        }]

    return instancias_companies
    

# ================================================================================================================================================================================
# PROVISÓRIO - QUERY PRA GARANTIR QUE USER MICROSERVICE TENHA AUTH EM TODAS COMPANIES
# ================================================================================================================================================================================
async def add_user_microservice_companies(db_core):

    user_id_microservice = "49e3a5ec-24a2-4ce4-b707-6ddbec07d8f0"

    query = """
        INSERT INTO user_role_company (user_id, company_id, user_role)
        SELECT
            :user_id,
            c.id,
            'admin'
        FROM company c
        LEFT JOIN user_role_company urc
            ON urc.company_id = c.id AND urc.user_id = :user_id
        WHERE urc.company_id IS NULL
        RETURNING company_id;
    """

    try:
        async with db_core.connection() as conn:
            # Executa e retorna todas as company_id inseridas
            rows = await asyncio.wait_for(
                conn.fetch_all(query=query, values={"user_id": user_id_microservice}),
                timeout=300
            )

            if not rows:
                #logger.info("Nenhuma nova empresa precisava ser vinculada ao microservice.")
                return []

            company_ids = [r["company_id"] for r in rows]
            logger.info(f"Usuário microservice adicionado como admin em {len(company_ids)} empresas.")
            return company_ids

    except Exception as e:
        logger.error(f"Erro ao adicionar microservice nas companies: {e}")
        return []
        



    
        
# ================================================================================================================================================================================
# CONSULTA LEADS NO DB
# ================================================================================================================================================================================
async def consulta_leads_disponiveis_para_classificar(db_core, company_id, max_leads_por_rodada):
    try:
        async with db_core.connection() as conn_core:
            # Query de leads aptos: respeita período, janelas de reexecução e flag da company.
            query_core = """
                WITH ult_exec AS (
                    SELECT DISTINCT ON (lead_id)
                        lead_id,
                        execution_date AS last_execution_date,
                        execution_date_kw AS last_kw_execution_date,
                        CASE
                            WHEN message_sent_date IS NOT NULL AND response_date IS NULL THEN INTERVAL '24 hours'
                            ELSE INTERVAL '9 hours'
                        END AS min_reprocess_interval
                    FROM lead_status_transition
                    WHERE company_id = :company_id
                    ORDER BY lead_id, execution_date DESC
                )
                SELECT
                    l.company_id AS company_id,
                    (c.metadata->>'service_ai_config_phonenumber') AS tel_resp_company,
                    c.business_context AS business_context,
                    COALESCE((c.metadata->>'ai_analysis_period')::int, 30) AS ai_analysis_period,
                    l.id AS lead_id,
                    l.phone AS tel_lead,
                    l.lid AS lid,
                    s.status AS pre_status_name,
                    s.id AS pre_status_id,
                    s.code AS pre_status_code,
                    u.last_execution_date,
                    u.last_kw_execution_date,
                    l.created_at AS dt_abertura_lead
                FROM lead l
                JOIN company c ON l.company_id = c.id
                JOIN lead_status ls ON l.id = ls.lead_id
                JOIN status s ON ls.status_id = s.id
                LEFT JOIN ult_exec u ON l.id = u.lead_id
                WHERE l.company_id = :company_id
                  AND (
                        (c.metadata->>'service_ai_config_only_tracked') = 'false'
                        OR l.source IN ('METAADS_SITE', 'METAADS_MSG', 'GOOGLE_SITE')
                      )
                  -- AND l.source IN ('METAADS_SITE', 'METAADS_MSG', 'GOOGLE_SITE')
                  AND s.code NOT IN ('END_WON', 'END_LOST')
                  AND (c.metadata->>'status_transition_auto') = 'true'
                  AND COALESCE((c.metadata->>'disabled')::boolean, false) = false
                  AND (c.metadata->>'service_ai_config_phonenumber') IS NOT NULL
                  AND (
                        (l.phone IS NOT NULL AND trim(l.phone) <> '')
                        OR (l.lid IS NOT NULL AND trim(l.lid) <> '')
                      )
                  AND (
                        (u.last_execution_date IS NULL AND l.created_at < NOW() - INTERVAL '3 hours')
                        OR (
                            u.last_execution_date IS NOT NULL
                            AND u.last_execution_date < NOW() - u.min_reprocess_interval
                        )
                      )
                  AND l.created_at >= NOW() - (
                        COALESCE((c.metadata->>'ai_analysis_period')::int, 30) * INTERVAL '1 day'
                      )
                ORDER BY u.last_execution_date ASC NULLS FIRST
                LIMIT :max_leads_por_rodada;
            """

 
            values = {
                "company_id": company_id,
                "max_leads_por_rodada": max_leads_por_rodada
            }

            rows = await asyncio.wait_for(
                conn_core.fetch_all(query=query_core, values=values),
                timeout=300
            )
            return [serializa_metadata(dict(row)) for row in rows]

    except Exception as e:
        logger.error(f"Erro ao buscar leads disponíveis para classificar: {e}")
        return []
        

    
# ================================================================================================================================================================================
# BUSCA MENSAGENS LEAD
# ================================================================================================================================================================================
async def busca_mensagens_lead(db_evo, company_id, tel_lead, lid, dt_abertura_lead):

    # Monta condições de match dinamicamente (phone e/ou lid)
    match_conditions = []
    values = {"company_id": company_id, "dt_abertura_lead": dt_abertura_lead}

    if tel_lead and str(tel_lead).strip():
        tel_lead_jid = f"{tel_lead}@s.whatsapp.net"
        values["tel_lead_jid"] = tel_lead_jid
        match_conditions.append("a.key->>'remoteJid' = :tel_lead_jid")
        match_conditions.append("a.key->>'remoteJidAlt' = :tel_lead_jid")

    if lid and str(lid).strip():
        lid_jid = f"{lid}@lid"
        values["lid_jid"] = lid_jid
        match_conditions.append("a.key->>'remoteJid' = :lid_jid")
        match_conditions.append("a.key->>'remoteJidAlt' = :lid_jid")

    if not match_conditions:
        return []

    match_clause = " OR ".join(match_conditions)

    query_evo = f"""
        SELECT
            CASE
                WHEN (a.key->>'fromMe') = 'true' THEN 'EMPRESA'
                ELSE 'LEAD'
            END AS de,
            to_timestamp(a."messageTimestamp") AT TIME ZONE 'America/Sao_Paulo' AS data_hora,
            a.message->>'conversation' AS mensagem,
            COALESCE(
                a.key->>'remoteJid',
                a.key->>'remoteJidAlt'
            ) AS jid_encontrado
        FROM "Message" a
        JOIN "Instance" b
            ON a."instanceId" = b.id
        WHERE a."messageType" = 'conversation'
          AND a.message->>'conversation' IS NOT NULL
          AND b."name" = :company_id
          AND ({match_clause})
          AND a."messageTimestamp" > EXTRACT(EPOCH FROM (:dt_abertura_lead AT TIME ZONE 'America/Sao_Paulo'))
        ORDER BY a."messageTimestamp" ASC
    """

    async with db_evo.connection() as conn:  # pega e devolve conexão
        rows = await asyncio.wait_for(
            conn.fetch_all(query=query_evo, values=values),
            timeout=300
        )

    return [serializa_metadata(dict(row)) for row in rows]


    
    
# ================================================================================================================================================================================
# CONSULTA ID DO STATUS SUGERIDO PELA AI
# ================================================================================================================================================================================
async def consulta_id_status(db_core, company_id, status_value, tipo_status):
    
    try:
        
        if tipo_status == "status_code":
            query = """
                SELECT id
                FROM status
                WHERE company_id = :company_id AND code = :status_code
                LIMIT 1;
            """
            values = {"company_id": company_id, "status_code": status_value}
        
        elif tipo_status == "status_name":
            query = """
                SELECT id
                FROM status
                WHERE company_id = :company_id AND status = :status_name
                LIMIT 1;
            """
            values = {"company_id": company_id, "status_name": status_value}
            
            

        async with db_core.connection() as conn:
            row = await asyncio.wait_for(
                conn.fetch_one(query=query, values=values),
                timeout=300
            )

        return serializa_metadata(row["id"]) if row else None

    except Exception as e:
        logger.error(f"Erro ao consultar id do status '{status_value}' para a empresa {company_id}: {e}")
        return None
        
        
        
      
# ================================================================================================================================================================================
# BUSCA CONFIGS DE STATUS - TRAZ SOMENTE STATUS PERMITIDOS PARA A AI UTILIZAR
# ================================================================================================================================================================================
async def busca_status_configs(db_core, company_id):
    try:
        query = """
            WITH norm AS (
              SELECT
                status AS status_name,
                code   AS status_code,
                status_ai_identification AS status_description,

                /* aiAutomationMode: usa o valor; '', 'null' => default por código */
                COALESCE(
                  NULLIF(NULLIF(metadata->>'aiAutomationMode',''), 'null'),
                  CASE WHEN code IN ('END_WON','END_LOST')
                       THEN 'always_send_confirmation'
                       ELSE 'auto_update_high_confidence'
                  END
                ) AS ai_automation_mode,

                /* aiSuggestion: default true; só fica false se explicitamente false */
                CASE
                  WHEN metadata ? 'aiSuggestion' THEN
                    COALESCE(
                      CASE
                        WHEN (metadata->>'aiSuggestion') ~* '^(true|false)$'
                          THEN LOWER(metadata->>'aiSuggestion')::boolean
                        WHEN NULLIF(LOWER(metadata->>'aiSuggestion'),'null') IS NULL
                             OR metadata->>'aiSuggestion' = '' THEN NULL
                        ELSE NULL
                      END,
                      TRUE
                    )
                  ELSE TRUE
                END AS ai_suggestion,

                /* aiMinimalConfidence: inteiro; '', 'null' ou não-numérico => default por código */
                COALESCE(
                  CASE
                    WHEN NULLIF(LOWER(metadata->>'aiMinimalConfidence'),'null') IS NULL
                         OR metadata->>'aiMinimalConfidence' = '' THEN NULL
                    WHEN (metadata->>'aiMinimalConfidence') ~ '^[0-9]+$'
                         THEN (metadata->>'aiMinimalConfidence')::int
                    ELSE NULL
                  END,
                  CASE WHEN code IN ('END_WON','END_LOST') THEN 0 ELSE 80 END
                ) AS ai_confidence_level_min_config,

                /* kwAnalysis: ativa o fluxo de keywords */
                COALESCE(
                  CASE
                    WHEN metadata ? 'kwAnalysis' THEN
                      CASE
                        WHEN (metadata->>'kwAnalysis') ~* '^(true|false)$'
                          THEN LOWER(metadata->>'kwAnalysis')::boolean
                        ELSE FALSE
                      END
                    ELSE FALSE
                  END,
                  FALSE
                ) AS kw_analysis,
                metadata->>'kwKeyphrase' AS kw_keyphrase
              FROM status
              WHERE company_id = :company_id
                AND code IS DISTINCT FROM 'LEAD_START'
            )
            SELECT DISTINCT
              status_name,
              status_code,
              status_description,
              ai_automation_mode,
              ai_suggestion,
              ai_confidence_level_min_config,
              kw_analysis,
              kw_keyphrase
            FROM norm
            /* equivalente à sua regra: só filtra fora quem for explicitamente false */
            WHERE ai_suggestion IS DISTINCT FROM FALSE OR kw_analysis;

        """
        values = {"company_id": company_id}

        async with db_core.connection() as conn:
            rows = await asyncio.wait_for(
                conn.fetch_all(query=query, values=values),
                timeout=300
            )

        result = []
        if not rows:
            return []

        for row in rows:
            result.append({
                "status_name": row["status_name"],           # legível
                "status_code": row["status_code"],           # universal
                "status_description": row["status_description"],
                "ai_automation_mode": row["ai_automation_mode"],
                "ai_suggestion": row["ai_suggestion"],
                "ai_confidence_level_min_config": row["ai_confidence_level_min_config"],
                "kw_analysis": row["kw_analysis"],
                "kw_keyphrase": row["kw_keyphrase"],
            })

        return result

    except Exception as e:
        logger.exception(f"Erro ao buscar status configs para a empresa {company_id}: {e}")
        return []
        
        
        
        
# ================================================================================================================================================================================
# INSERE REGISTRO NA TB DE TRACKING
# ================================================================================================================================================================================
async def insere_registro_ai_tracking(db_core, company_id, lead_id, execution_date, execution_date_ai, execution_date_kw, message_schedule_date, metadata):
    
    # PARA SALVAR NA TB DE TRACKING USA OS NAMES AO INVÉS DE CODES OU IDS
    pre_status = metadata.get("pre_status_name")
    sugested_status_ai = metadata.get("ai_suggestion_status_name")
    pos_status_kw = metadata.get("pos_status_kw")
    
    # Define message_status:
    # - 'pending' se houver agendamento de envio
    # - 'n/a' se for execução por keyword (não envia mensagem)
    # - None para outros casos
    if message_schedule_date:
        message_status = 'pending'
    elif metadata.get("executor") == "keyword":
        message_status = 'n/a'
    else:
        message_status = None

    try:
        query = """
            INSERT INTO lead_status_transition (
                company_id,
                lead_id,
                execution_date,
                pre_status,
                sugested_status_ai,
                execution_date_ai,
                execution_date_kw,
                message_schedule_date,
                message_status,
                pos_status_kw,
                metadata,
                created_at,
                updated_at
            )
            VALUES (
                :company_id,
                :lead_id,
                :execution_date,
                :pre_status,
                :sugested_status_ai,
                :execution_date_ai,
                :execution_date_kw,
                :message_schedule_date,
                :message_status,
                :pos_status_kw,
                CAST(:metadata AS JSONB),
                NOW(),
                NOW()
            )
            RETURNING id
        """

        values = {
            "company_id": company_id,
            "lead_id": lead_id,
            "execution_date": execution_date,
            "pre_status": pre_status,
            "sugested_status_ai": sugested_status_ai,
            "execution_date_ai": execution_date_ai,
            "execution_date_kw": execution_date_kw,
            "message_schedule_date": message_schedule_date,
            "message_status": message_status,
            "pos_status_kw": pos_status_kw,
            "metadata": json.dumps(serializa_metadata(metadata), ensure_ascii=False)
        }

        async with db_core.connection() as conn:  # abre e devolve conexão automaticamente
            row = await asyncio.wait_for(
                conn.fetch_one(query=query, values=values),
                timeout=300
            )

        return serializa_metadata(row["id"]) if row else None

    except Exception as e:
        logger.error(f"Erro ao inserir registro em lead_status_transition: {e}")
        return None


        
        
        
# ================================================================================================================================================================================
# CLASSIFICA HISTÓRICO DE CONVERSA COM AI
# ================================================================================================================================================================================
async def classifica_historico_com_ai(session, prompt_injetado, user_input):

    headers = {
        "Content-Type": "application/json",
    }

    payload = {
        "system_instruction": {
            "parts": [{"text": prompt_injetado}]
        },
        "contents": [
            {"role": "user", "parts": [{"text": user_input}]}
        ],
        "generationConfig": {
            "responseMimeType": "application/json",
            "responseSchema": {
                "type": "OBJECT",
                "properties": {
                    "ai_suggestion_status_name": {"type": "STRING"},
                    "nome_lead": {"type": "STRING"},
                    "valor": {"type": "NUMBER"},
                    "ai_confidence_level_output": {"type": "STRING"},
                    "analise_ai": {"type": "STRING"}
                },
                "required": ["ai_suggestion_status_name", "ai_confidence_level_output", "analise_ai"]
            }
        }
    }

    url = f"{GEMINI_URL}?key={GEMINI_API_KEY}"

    for tentativa in range(1, GEMINI_MAX_RETRIES + 1):
        try:
            async with session.post(
                url,
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=GEMINI_TIMEOUT)
            ) as response:

                if response.status == 200:
                    data = await response.json()
                    resposta_ai = data["candidates"][0]["content"]["parts"][0]["text"]
                    usage = data.get("usageMetadata", {})
                    prompt_tokens = usage.get("promptTokenCount", 0)
                    completion_tokens = usage.get("candidatesTokenCount", 0)
                    total_tokens = usage.get("totalTokenCount", 0)
                    logger.info(f"Sucesso API Gemini: {prompt_tokens} prompt_tokens; {completion_tokens} completion_tokens; {total_tokens} total_tokens")
                    logger.info(f"Resposta AI: {resposta_ai}")
                    return resposta_ai
                else:
                    error_message = await response.text()
                    log = logger.warning if tentativa < GEMINI_MAX_RETRIES else logger.error
                    log(f"Erro {response.status} ao chamar Gemini (tentativa {tentativa}/{GEMINI_MAX_RETRIES}): {error_message}")
                    if response.status in (429, 500, 502, 503) and tentativa < GEMINI_MAX_RETRIES:
                        await asyncio.sleep(2 ** tentativa)
                        continue
                    return ""

        except asyncio.TimeoutError as e:
            log = logger.warning if tentativa < GEMINI_MAX_RETRIES else logger.error
            log(f"Timeout ao chamar Gemini (tentativa {tentativa}/{GEMINI_MAX_RETRIES}): {e}")
        except aiohttp.ClientError as e:
            log = logger.warning if tentativa < GEMINI_MAX_RETRIES else logger.error
            log(f"Erro de cliente aiohttp ao chamar Gemini (tentativa {tentativa}/{GEMINI_MAX_RETRIES}): {e}")
        except Exception as e:
            log = logger.warning if tentativa < GEMINI_MAX_RETRIES else logger.error
            log(f"Erro genérico ao chamar Gemini (tentativa {tentativa}/{GEMINI_MAX_RETRIES}): {e}")

        if tentativa < GEMINI_MAX_RETRIES:
            await asyncio.sleep(2 ** tentativa)

    return ""
    


# ================================================================================================================================================================================
# AGENDA MENSAGEM PARA CONFIRMAR ALTERAÇÃO DE STATUS (APENAS TRACKING - ENVIO FEITO PELO SERVIÇO send_messages)
# ================================================================================================================================================================================
async def agenda_confirmacao_alteracao_status(db_core, company_id, lead_id, tel_resp_company, tel_lead, nome_lead, pre_status_id, pre_status_name, pre_status_code, ai_suggestion_status_id, ai_suggestion_status_name, ai_suggestion_status_code, reversed_ai_suggestion_status_id, reversed_ai_suggestion_status_name, reversed_ai_suggestion_status_code, valor_conversao, start_datetime, ai_datetime, status_configs_ai, metadata_tracking):
    """
    Agenda uma mensagem de confirmação de alteração de status.
    O envio efetivo é feito pelo serviço send_messages.
    """

    # regra de valor é no CODE, mas usamos legível pra exibir
    config_sugestoes_por_status = next((s for s in status_configs_ai if s["status_name"] == ai_suggestion_status_name), None)
    ai_suggestion_status_code = config_sugestoes_por_status["status_code"] if config_sugestoes_por_status else None

    if valor_conversao:
        metadata_tracking["lead_value"] = valor_conversao

    schedule_datetime = datetime.now()

    # Tracking - grava message_schedule_date para o serviço send_messages processar
    inserted_id = await insere_registro_ai_tracking(db_core, company_id, lead_id, start_datetime, ai_datetime, None, schedule_datetime, metadata_tracking)

    if inserted_id:
        logger.info(f"Mensagem de confirmação agendada com sucesso (id: {inserted_id})")
        return True

    logger.error("Erro ao agendar mensagem de confirmação")
    return False


# ================================================================================================================================================================================
# PROCESSA COM KEYPHRASES
# ================================================================================================================================================================================
async def processa_keywords(db_core, http_session, lead_info, status_configs, mensagens, auth_token_company):
    """Fluxo de keywords: detecta frases pré-configuradas e altera status/valor automaticamente."""
    company_id = lead_info.get("company_id", "")
    tel_resp_company = lead_info.get("tel_resp_company", "")
    lead_id = lead_info.get("lead_id", "")
    tel_lead = lead_info.get("tel_lead", "")
    lid = lead_info.get("lid", "")

    pre_status_id = lead_info.get("pre_status_id", "")
    pre_status_name = lead_info.get("pre_status_name", "")
    pre_status_code = lead_info.get("pre_status_code", "")

    # Monta lista de keywords válidas (ignora se não houver frase ou se não estiver ativada)
    kw_entries = []
    for status in status_configs:
        if not status.get("kw_analysis"):
            continue
        raw_phrase = status.get("kw_keyphrase") or ""
        capture_value = "{{valor_conversao}}" in raw_phrase
        phrase_for_match = raw_phrase.replace("{{valor_conversao}}", "")
        normalized_phrase = normaliza_texto_para_kw(phrase_for_match)
        if not normalized_phrase:
            continue
        kw_entries.append({
            "status_name": status["status_name"],
            "status_code": status["status_code"],
        "kw_keyphrase": raw_phrase,
        "kw_keyphrase_normalized": normalized_phrase,
        "capture_value": capture_value
    })

    if not kw_entries:
        return {"status": "none"}

    # Janela de análise de KW: da última execução de KW ou da abertura do lead
    dt_ultima_kw_exec = lead_info.get("last_kw_execution_date") or lead_info.get("dt_abertura_lead")
    dt_ultima_kw_exec_naive = to_naive_datetime(dt_ultima_kw_exec)

    # Filtra apenas mensagens da empresa, posteriores ao último run de KW
    mensagens_kw = []
    for mensagem in mensagens:
        if mensagem.get("de") != "EMPRESA":
            continue
        msg_dt_raw = mensagem.get("data_hora")
        msg_dt = to_naive_datetime(msg_dt_raw)
        if msg_dt is None:
            continue
        if dt_ultima_kw_exec_naive and msg_dt <= dt_ultima_kw_exec_naive:
            continue
        texto = mensagem.get("mensagem") or ""
        if not texto.strip():
            continue
        mensagens_kw.append({
            "datetime": msg_dt,
            "raw_datetime": msg_dt_raw,
            "texto": texto
        })

    if not mensagens_kw:
        return {"status": "none"}

    if not auth_token_company:
        logger.error("auth_token_company ausente antes de processar keywords")
        return {"status": "error"}

    execution_date = datetime.now()
    status_id_cache = {}
    async def resolve_status_id(status_name):
        if status_name in status_id_cache:
            return status_id_cache[status_name]
        status_id = await consulta_id_status(db_core, company_id, status_name, "status_name")
        status_id_cache[status_name] = status_id
        return status_id

    changes = 0
    total_messages = len(mensagens_kw)

    for msg_index, mensagem in enumerate(mensagens_kw):
        texto_normalizado = normaliza_texto_para_kw(mensagem["texto"])
        if not texto_normalizado:
            continue
        for status in kw_entries:
            occurrences = texto_normalizado.count(status["kw_keyphrase_normalized"])
            for occurrence_index in range(occurrences):
                # Resolve ID e aplica status via API
                target_status_id = await resolve_status_id(status["status_name"])
                if not target_status_id:
                    logger.error(f"não encontrou id para status {status['status_name']} (keyword)")
                    return {"status": "error"}

                token = auth_token_company
                if not token:
                    logger.error("falha ao obter auth_token para keywords")
                    return {"status": "error"}

                status_change = await change_lead_status(http_session, token, lead_id, target_status_id)
                if not status_change:
                    logger.error(f"falha ao atualizar status {status['status_name']} via keyword")
                    return {"status": "error"}

                # Captura valor (quando placeholder presente) e envia conversão se for END_WON
                valor_conversao = None
                if status.get("capture_value"):
                    valor_conversao = extrai_valor_conversao(mensagem["texto"])

                    if valor_conversao not in (None, "", " ", 0, "0"):
                        status_send_conversion_value = await send_lead_conversion_value(http_session, token, lead_id, valor_conversao)
                        if not status_send_conversion_value:
                            logger.error("Erro ao enviar conversion_value via keyword")
                        else:
                            logger.info("Valor de conversão enviado via keyword")

                metadata_tracking = {
                    "executor": "keyword",
                    "manager_phone": tel_resp_company,
                    "lead_phone": tel_lead,
                    "lead_lid": lid,
                    "pre_status_id": pre_status_id,
                    "pre_status_name": pre_status_name,
                    "pre_status_code": pre_status_code,
                    "pos_status_kw": status["status_name"],
                    "kw_action": "auto_update",
                    "kw_keyphrase": status["kw_keyphrase"],
                    "kw_status_id": target_status_id,
                    "kw_status_name": status["status_name"],
                    "kw_status_code": status["status_code"],
                    "kw_message_timestamp": mensagem["raw_datetime"].isoformat(),
                    "kw_active": bool(status.get("kw_analysis")),
                    "ai_active": bool(status.get("ai_suggestion")),
                }
                if valor_conversao not in (None, "", " ", 0, "0"):
                    metadata_tracking["lead_value"] = valor_conversao
                    metadata_tracking["kw_value_captured"] = valor_conversao

                await insere_registro_ai_tracking(
                    db_core,
                    company_id,
                    lead_id,
                    execution_date,
                    None,
                    execution_date,
                    None,
                    metadata_tracking
                )

                logger.info(f"keyword '{status['kw_keyphrase']}' acionou status {status['status_name']} (mensagem {msg_index + 1}/{total_messages})")

                pre_status_id = target_status_id
                pre_status_name = status["status_name"]
                pre_status_code = status["status_code"]
                changes += 1

    if changes:
        return {"status": "triggered", "changes": changes}

    return {"status": "none"}


        
# ================================================================================================================================================================================
# PROCESSA AI + WHATSAPP
# ================================================================================================================================================================================
async def processa_ai(db_core, http_session, lead_info, status_configs_ai, mensagens_limitadas, auth_token_company):
    """Fluxo de IA: monta prompt, consulta modelo, decide auto-update ou confirmação."""

    start_datetime = datetime.now()
    
    # REFERÊNCIAS DA COMPANY
    company_id       = lead_info.get("company_id", "")
    tel_resp_company = lead_info.get("tel_resp_company", "")
    business_context = lead_info.get("business_context", None) or ""
      
    # REFERÊNCIAS DO LEAD
    lead_id  = lead_info.get("lead_id", "")
    tel_lead = lead_info.get("tel_lead", "")
    lid      = lead_info.get("lid", "")

    # STATUS ATUAL (legível + id)
    pre_status_name = lead_info.get("pre_status_name", "")
    pre_status_code = lead_info.get("pre_status_code", "")
    pre_status_id = lead_info.get("pre_status_id", "")
    
    # INPUT PARA A IA
    user_input = f"--- STATUS_ATUAL: {pre_status_name} ---\n\n{mensagens_limitadas}"
    
    # PROMPT ----------------------------------------------------------------------------------------------------------
    business_context = business_context if business_context.strip() not in (None, "", "0", 0) else DEFAULT_BUSINESS_CONTEXT
    prompt_injetado  = PROMPT.replace("PLACEHOLDER_BUSINESS_CONTEXT", business_context)

    # monta lista de status disponíveis
    status_parts, status_names = [], []
    for s in status_configs_ai:
        desc = s["status_description"]
        if desc is None or str(desc).strip() in ("", "0", 0):
            desc = DEFAULT_STATUS_CODE_DESCRIPTIONS.get(s["status_code"], "")
        status_parts.append(f"{s['status_name']}: {desc}")
        status_names.append(s["status_name"])

    prompt = prompt_injetado.replace(
        "PLACEHOLDER_STATUS_DISPONIVEIS_COM_DESCRICAO", "; ".join(status_parts)
    ).replace(
        "PLACEHOLDER_EXEMPLO_STATUS", " / ".join(status_names)
    )
    
    # CHAMADA À GEMINI ------------------------------------------------------------------------------------------------
    ai_datetime = datetime.now()
    resposta_ai = await classifica_historico_com_ai(http_session, prompt, user_input)
    
    if not resposta_ai:
        logger.error("Resposta vazia da IA")
        return False

    resposta_ai = resposta_ai.replace("```json", "").replace("```", "").strip()
    
    try:
        resposta_json_ai, _ = json.JSONDecoder().raw_decode(resposta_ai)
    except json.JSONDecodeError as e:
        logger.error(f"Erro ao converter a resposta da IA para JSON: {e}")
        return False

    # EXTRAI RETORNO --------------------------------------------------------------------------------------------------
    ai_suggestion_status_name = resposta_json_ai.get("ai_suggestion_status_name", "")
    nome_lead       = resposta_json_ai.get("nome_lead", "")
    ai_confidence_level_output = resposta_json_ai.get("ai_confidence_level_output", 0)
    analise_ai      = resposta_json_ai.get("analise_ai", "")

    try:
        ai_confidence_level_output = float(ai_confidence_level_output)
    except Exception:
        ai_confidence_level_output = 0.0
    
    valor_raw       = resposta_json_ai.get("valor", 0)
    valor_conversao = float(valor_raw) if valor_raw not in (None, "", " ") else 0.0
    valor_conversao_informado = valor_conversao not in (None, "", " ", 0, "0")
    
    if not ai_suggestion_status_name:
        logger.error("AI não sugeriu um status para o lead, ignorando lead")
        return False
    if not nome_lead:
        logger.info("AI não conseguiu encontrar o nome_lead")

    if valor_conversao_informado:
        auth_token_app_leaper = auth_token_company
        if not auth_token_app_leaper:
            auth_token_app_leaper = await get_auth_token_company_leaper(http_session, company_id)
        if not auth_token_app_leaper:
            logger.error("Não foi possível obter auth_token_app_leaper para enviar conversion_value")
        else:
            status_send_conversion_value = await send_lead_conversion_value(http_session, auth_token_app_leaper, lead_id, valor_conversao)
            if status_send_conversion_value:
                logger.info("Valor de conversão do lead atualizado com sucesso")
            else:
                logger.error("Erro ao enviar conversion_value")
            auth_token_company = auth_token_app_leaper

    # DE-PARA: pega config do status legível e extrai code -------------------------------------------------------------
    config_sugestoes_por_status = next((s for s in status_configs_ai if s["status_name"] == ai_suggestion_status_name), None) or {}
    ai_suggestion_status_code = config_sugestoes_por_status["status_code"] if config_sugestoes_por_status else None

    # BUSCA ID PELO STATUS LEGÍVEL ------------------------------------------------------------------------------------
    ai_suggestion_status_id = await consulta_id_status(db_core, company_id, ai_suggestion_status_name, "status_name")

    # REGRAS DE INVERSÃO (baseadas em CODE) ---------------------------------------------------------------------------
    # reversed a partir do CODE; NAME sempre legível
    reversed_ai_suggestion_status_name = None
    reversed_ai_suggestion_status_id = None
    reversed_ai_suggestion_status_code = None
    if ai_suggestion_status_code in ("END_WON", "END_LOST"):
        # 1) inverte o CODE
        reversed_ai_suggestion_status_code = "END_LOST" if ai_suggestion_status_code == "END_WON" else "END_WON"
        # 2) pega o NAME legível a partir do CONFIG pelo CODE
        config_reversed = next((s for s in status_configs_ai if s.get("status_code") == reversed_ai_suggestion_status_code),{})
        reversed_ai_suggestion_status_name = config_reversed.get("status_name")  # ex.: "Finalizado Perdido" / "Finalizado Convertido"
        # 3) busca o ID pelo CODE (aqui sim, passa code e diz que o campo é "status_code")
        reversed_ai_suggestion_status_id = await consulta_id_status(db_core, company_id, reversed_ai_suggestion_status_code, "status_code")

    # NÍVEL DE CONFIANÇA PARAMETRIZADO NO FRONT
    ai_confidence_level_min_config = float(config_sugestoes_por_status.get("ai_confidence_level_min_config") or 0)                                                                               # FALLBACK = 0

    # METADATA TRACKING ------------------------------------------------------------------------------------------------
    metadata_tracking = {
        "executor": "ai",
        "manager_phone": tel_resp_company,
        "lead_phone": tel_lead,
        "lead_lid": lid,
        "lead_name": nome_lead,
        "pre_status_id": pre_status_id,
        "pre_status_name": pre_status_name,
        "pre_status_code": pre_status_code,
        "ai_suggestion_available_names": status_names,
        "ai_suggestion_status_id": ai_suggestion_status_id,
        "ai_suggestion_status_name": ai_suggestion_status_name,
        "ai_suggestion_status_code": ai_suggestion_status_code,  
        "ai_confidence_level_min_config": ai_confidence_level_min_config,
        "ai_confidence_level_output": ai_confidence_level_output,
        "ai_analysis": analise_ai,
        "ai_active": bool(config_sugestoes_por_status.get("ai_suggestion")),
        "kw_active": bool(config_sugestoes_por_status.get("kw_analysis")),
    }
    if reversed_ai_suggestion_status_name:
        metadata_tracking["reversed_ai_suggestion_status_id"]   = reversed_ai_suggestion_status_id
        metadata_tracking["reversed_ai_suggestion_status_name"] = reversed_ai_suggestion_status_name
        metadata_tracking["reversed_ai_suggestion_status_code"] = reversed_ai_suggestion_status_code
        

    # SE O STATUS SUGERIDO É IGUAL AO STATUS ATUAL, MARCA COMO KEEP_SAME_STATUS E ENCERRA EXECUÇÃO
    if ai_suggestion_status_id == pre_status_id:
        metadata_tracking["ai_action"] = "keep_same_status"
        await insere_registro_ai_tracking(db_core, company_id, lead_id, start_datetime, ai_datetime, None, None, metadata_tracking)
        logger.info("status_sugerido_ai = pre_status_name, não enviando solicitação de confirmação")
        return "keep_same_status"
        
    # SEGUE COM AS REGRAS:
        
    # DECISÃO AUTOMÁTICA X CONFIRMAÇÃO --------------------------------------------------------------------------------
    ai_automation_mode    = config_sugestoes_por_status.get("ai_automation_mode") if config_sugestoes_por_status else "always_send_confirmation"        # FALLBACK = sempre enviar confirmação
    
    # REGRA PARA ATUALIZAR AUTOMÁTICO (SEM PRECISAR ENVIAR CONFIRMAÇÃO)
    auto_update = (ai_automation_mode == "auto_update_high_confidence" and ai_confidence_level_output >= ai_confidence_level_min_config)


    # SE FOR AUTO_UPDATE SELECIONADO PELO USER ------------------------------------------------------------------
    if auto_update:
        metadata_tracking["ai_action"] = "auto_update"
        logger.info(f"Atualizando status automaticamente para {ai_suggestion_status_name} (confiança {ai_confidence_level_output} >= {ai_confidence_level_min_config})")

        auth_token_app_leaper = auth_token_company
        if not auth_token_app_leaper:
            logger.error("Token da company ausente para auto_update")
            return False

        # ALTERA STATUS LEAD - APENAS SE FOR DIFERENTE DO STATUS ATUAL, PRA NÃO FICAR VÁRIOS UPDATES IGUAIS NO HISTÓRICO
        status_change = await change_lead_status(http_session, auth_token_app_leaper, lead_id, ai_suggestion_status_id)
        if status_change:
            logger.info("Status do lead atualizado com sucesso")
        else:
            logger.error("Erro ao atualizar status do lead")

        # Tracking (sem confirmação)
        await insere_registro_ai_tracking(db_core, company_id, lead_id, start_datetime, ai_datetime, None, None, metadata_tracking)
        return "auto_update"


    # CASO CONTRÁRIO: ENVIAR CONFIRMAÇÃO ----------------------------------------------------------------------
    else:
        
        # TIPO DE COFIRMAÇÃO (PQ FOI BAIXA CONFIANÇA OU PQ TA CONFIGURADO PARA SEMPRE ENVIAR) ?
        if ai_automation_mode == "auto_update_high_confidence":
            metadata_tracking["ai_action"] = "confirm_when_low_confidence" 
        else:
            metadata_tracking["ai_action"] = "confirm_always"

        logger.info("status_sugerido_ai != pre_status_name, agendando solicitação de confirmação...")

        # AGENDA MSG DE CONFIRMAÇÃO (envio feito pelo serviço send_messages)
        agendou = await agenda_confirmacao_alteracao_status(db_core, company_id, lead_id, tel_resp_company, tel_lead, nome_lead, pre_status_id, pre_status_name, pre_status_code, ai_suggestion_status_id, ai_suggestion_status_name, ai_suggestion_status_code, reversed_ai_suggestion_status_id, reversed_ai_suggestion_status_name, reversed_ai_suggestion_status_code, valor_conversao, start_datetime, ai_datetime, status_configs_ai, metadata_tracking)
        return "confirmation_scheduled" if agendou else False

    return False
    


# ================================================================================================================================================================================
# PROCESSA 1 LEAD
# ================================================================================================================================================================================
async def processa_lead(db_core, db_evo, session, lead_info, auth_token_company):
    """Processa um lead: busca mensagens, roda keywords primeiro, depois IA se precisar."""
    try:
        company_id       = lead_info.get("company_id", "")
        tel_resp_company = lead_info.get("tel_resp_company", "")
        lead_id          = lead_info.get("lead_id", "")
        tel_lead         = lead_info.get("tel_lead", "")
        lid              = lead_info.get("lid", "")
        dt_abertura_lead = lead_info.get("dt_abertura_lead", "")
        pre_status_name  = lead_info.get("pre_status_name", "")
        pre_status_id    = lead_info.get("pre_status_id", "")
        pre_status_code  = lead_info.get("pre_status_code", "")

        log_metadata.set({
            "company_id": company_id,
            "tel_resp_company": tel_resp_company,
            "lead_id": lead_id,
            "tel_lead": tel_lead,
            "lid": lid,
            "dt_abertura_lead": dt_abertura_lead,
            "pre_status_name": pre_status_name,
            "pre_status_id": pre_status_id,
            "pre_status_code": pre_status_code
        })

        logger.info("Processando lead...")

        # -------------------------------------------------------------------
        # BUSCA MENSAGENS DO LEAD
        # -------------------------------------------------------------------
        mensagens = await busca_mensagens_lead(db_evo, company_id, tel_lead, lid, dt_abertura_lead)
        
        if not mensagens:
            logger.warning("Lead ignorado: sem mensagens.")
            metadata_tracking = {
                "manager_phone": tel_resp_company,
                "lead_phone": tel_lead,
                "lead_lid": lid,
                "message": "Lead without messages"
            }
            await insere_registro_ai_tracking(
                db_core, company_id, lead_id, datetime.now(), None, None, None, metadata_tracking
            )
            return {"processado": False, "agendadas": 0}

        # -------------------------------------------------------------------
        # FORMATA HISTÓRICO DE MENSAGENS
        # -------------------------------------------------------------------
        mensagens_formatadas = [
            f"{m['de'].upper()} ({m['data_hora'].strftime('%d/%m/%Y %H:%M:%S')}):\n {m['mensagem']}\n"
            for m in mensagens
        ]
        mensagens_limitadas = limitar_mensagens(mensagens_formatadas)

        # conta qtd de mensagnes e faz regra cruzada com dt hr abertura lead / ult data execuccao'
            #  (x.last_execution_date < NOW() - INTERVAL '6 hours')
            #   OR (x.last_execution_date IS NULL AND lead.created_at < NOW() - INTERVAL '3 hours')
        
        
        # se nao passar, nao rodar ai

          
        # -------------------------------------------------------------------
        # BUSCA CONFIGS DE STATUS
        # -------------------------------------------------------------------
        status_configs = await busca_status_configs(db_core, company_id)

        # -------------------------------------------------------------------
        # PROCESSA KEYWORDS
        # -------------------------------------------------------------------
        kw_result = await processa_keywords(db_core, session, lead_info, status_configs, mensagens, auth_token_company)
        if kw_result.get("status") == "error":
            return {"processado": False, "agendadas": 0}
        if kw_result.get("status") == "triggered":
            return {
                "processado": True,
                "agendadas": kw_result.get("changes", 0),
                "via": "kw"
            }

        # -------------------------------------------------------------------
        # PROCESSA COM AI
        # -------------------------------------------------------------------
        status_configs_ai = [s for s in status_configs if s.get("ai_suggestion")]
        if not status_configs_ai:
            logger.info("sem status habilitados para AI")
            return {"processado": False, "agendadas": 0}

        resultado_ai = await processa_ai(db_core, session, lead_info, status_configs_ai, mensagens_limitadas, auth_token_company)

        # -------------------------------------------------------------------
        # RETORNO PADRONIZADO
        # -------------------------------------------------------------------
        # Só conta como "agendada" quando realmente agendou mensagem de confirmação
        agendada = 1 if resultado_ai == "confirmation_scheduled" else 0
        processado = resultado_ai in ("keep_same_status", "auto_update", "confirmation_scheduled")

        return {
            "processado": processado,
            "agendadas": agendada,
            "via": "ai"
        }

    except Exception as e:
        logger.exception(f"Erro ao processar lead: {e}", exc_info=True)
        return {"processado": False, "agendadas": 0}
        
        
        
        
        
        
# ================================================================================================================================================================================
# PROCESSA COMPANY
# ================================================================================================================================================================================
async def processa_company(db_core, db_evo, session, company_id, sem_company):
    """
    Processa todos os leads de uma company em paralelo (máx. 4 por vez),
    captura erros individuais sem interromper o restante e retorna um resumo final.
    """
    log_metadata.set({"company_id": company_id})

    async with sem_company:
        try:
            logger.info(f"Iniciando processamento da company {company_id}")

            # -------------------------------------------------------------------
            # 1. Carrega leads disponíveis para processamento -> fazer regras mais amplas e dps cruzar com qtd de mensagens pra limitar mais
            # -------------------------------------------------------------------
            leads_infos = await consulta_leads_disponiveis_para_classificar(db_core, company_id, MAX_LEADS_POR_RODADA)

            if not leads_infos:
                logger.info(f"Sem leads disponíveis")
                return {"company_id": company_id, "leads": 0, "processados": 0, "agendados": 0}

            auth_token_company = await get_auth_token_company_leaper(session, company_id)
            if not auth_token_company:
                logger.error(f"{company_id}: não foi possível obter auth_token_company, pulando processamento")
                return {"company_id": company_id, "leads": len(leads_infos), "processados": 0, "agendados": 0}

            # -------------------------------------------------------------------
            # 2. Cria subtarefas paralelas com limite de 4 leads simultâneos
            # -------------------------------------------------------------------
            sem_leads = asyncio.Semaphore(4)

            async def processa_lead_com_limite(lead_info):
                async with sem_leads:
                    try:
                        return await processa_lead(db_core, db_evo, session, lead_info, auth_token_company)
                    except Exception as e:
                        logger.exception(f"Erro ao processar lead {lead_info.get('lead_id')}: {e}", exc_info=True)
                        return {"processado": False, "agendadas": 0}

            tasks = [
                asyncio.create_task(processa_lead_com_limite(lead_info))
                for lead_info in leads_infos
            ]

            results = await asyncio.gather(*tasks, return_exceptions=False)

            # -------------------------------------------------------------------
            # 3. Calcula totais
            # -------------------------------------------------------------------
            total_leads = len(leads_infos)
            processados_total = 0
            processados_kw = 0
            processados_ai = 0
            agendados_total = 0

            for result in results:
                if isinstance(result, dict):
                    if result.get("processado"):
                        processados_total += 1
                        via = result.get("via")
                        if via == "kw":
                            processados_kw += 1
                        elif via == "ai":
                            processados_ai += 1
                    agendados_total += result.get("agendadas", 0)
                else:
                    logger.warning(f"Resultado inesperado ao processar lead: {result}")

            # -------------------------------------------------------------------
            # 4. Log e retorno final
            # -------------------------------------------------------------------
            logger.info(
                f"Company concluída | Leads: {total_leads} | "
                f"Processados: {processados_total} (kw: {processados_kw} | ai: {processados_ai}) | "
                f"Agendados: {agendados_total}"
            )

            return {
                "company_id": company_id,
                "leads": total_leads,
                "processados": processados_total,
                "processados_kw": processados_kw,
                "processados_ai": processados_ai,
                "agendados": agendados_total,
            }

        except Exception as e:
            logger.exception(f"Erro geral no processamento da company: {e}", exc_info=True)
            return {"company_id": company_id, "leads": 0, "processados": 0, "agendados": 0}

   

# ================================================================================================================================================================================
# PROCESSA INSTANCIA
# ================================================================================================================================================================================
async def processa_instancia_wpp(db_core, db_evo, evo_db_host, evo_db_name, session, companies_ids):
    """
    Executa o processamento de múltiplas companies em paralelo, capturando
    apenas os erros individuais de cada subtask sem interromper as demais.
    """
    

    try:

        logger.info(f"Iniciando processamento da instância")

        # -------------------------------------------------------------------
        # 2. Cria subtarefas para companies (máx. 4 simultâneas)
        # -------------------------------------------------------------------
        sem_company = asyncio.Semaphore(4)

        tasks = [
            asyncio.create_task(processa_company(db_core, db_evo, session, company_id, sem_company))
            for company_id in companies_ids
        ]

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # -------------------------------------------------------------------
        # 3. Captura erros individuais e soma totais
        # -------------------------------------------------------------------
        total_companies = len(companies_ids)
        leads_total = 0
        processados_total = 0
        processados_kw_total = 0
        processados_ai_total = 0
        agendados_total = 0

        for company_id, result in zip(companies_ids, results):
            if isinstance(result, Exception):
                logger.error(f"Company {company_id} falhou: {result}")
            elif isinstance(result, dict):
                leads_total += result.get("leads", 0)
                processados_total += result.get("processados", 0)
                processados_kw_total += result.get("processados_kw", 0)
                processados_ai_total += result.get("processados_ai", 0)
                agendados_total += result.get("agendados", 0)
            else:
                logger.warning(f"Company {company_id} retornou None ou formato inesperado")

        # -------------------------------------------------------------------
        # 4. Log final da instância
        # -------------------------------------------------------------------
        logger.info(
            f"Instância finalizada | Companies: {total_companies} | Leads: {leads_total} | "
            f"Processados: {processados_total} (kw: {processados_kw_total} | ai: {processados_ai_total}) | "
            f"Agendados: {agendados_total}"
        )

        return {
            "evo_db_host": evo_db_host,
            "evo_db_name": evo_db_name,
            "companies": total_companies,
            "leads": leads_total,
            "processados": processados_total,
            "processados_kw": processados_kw_total,
            "processados_ai": processados_ai_total,
            "agendados": agendados_total,
        }

    except Exception as e:
        logger.error(f"Erro geral no processamento da instância {evo_db_host} - {evo_db_name}: {e}")
        return {
            "evo_db_host": evo_db_host,
            "evo_db_name": evo_db_name,
            "companies": len(companies_ids),
            "leads": 0,
            "processados": 0,
            "agendados": 0,
        }
    
    
    
# ================================================================================================================================================================================
# MAIN
# ================================================================================================================================================================================
async def main():

    log_ip.set(SERVER_IP)
    log_host.set(SERVER_HOST)
    log_action.set('ai_suggestions')

    t0 = datetime.now()
    logger.info("Iniciando processo contínuo...")

    # Cria pools e sessão HTTP uma vez só
    db_core = await conecta_leaper_db_core(min_size=1, max_size=WORKERS_GLOBAIS*2)
    
    db_gateway = await conecta_leaper_db_gateway(min_size=1, max_size=WORKERS_GLOBAIS*2)
    
    
    async with aiohttp.ClientSession() as session:
        try:
        
            while True:
            
                # ------------------------------------------------------------------------------------------------------------------------------------------
                # PROVISÓRIO: garante que user microservice está autorizado em todas companies
                # ------------------------------------------------------------------------------------------------------------------------------------------
                await add_user_microservice_companies(db_core)
                
                
                # ------------------------------------------------------------------------------------------------------------------------------------------
                # CHECA SE ESTÁ DENTRO DO HORARIO
                # ------------------------------------------------------------------------------------------------------------------------------------------
                dentro_do_horario = await checa_se_esta_dentro_do_horario()
                
                # dentro_do_horario = True
                
                if dentro_do_horario:
                    
                    # ------------------------------------------------------------------------------------------------------------------------------------------
                    # PROCESSA POR INSTANCIA DE EVOLUTION (INSTANCIA -> COMPANY -> LEADS)
                    # ------------------------------------------------------------------------------------------------------------------------------------------
                    instancias_companies = await consulta_instancias_wpp(db_gateway)

                    # Variável de controle para reconectar apenas quando trocar de instância
                    evo_db_name_atual = None
                    db_evo = None

                    logger.info(instancias_companies)

                    for instancia_companies in instancias_companies:
                    
                        evo_db_host = instancia_companies.get("evo_db_host")
                        evo_db_name = instancia_companies.get("evo_db_name")
                        companies_ids = instancia_companies.get("companies_ids", [])

                        if not evo_db_name or not companies_ids:
                            #logger.info(f"Ignorando instância vazia ou sem companies: {instancia_companies}")
                            continue

                        # Reabre conexão apenas se mudar o nome da instância
                        if evo_db_name != evo_db_name_atual:
                            # Fecha conexão anterior, se houver
                            if db_evo:
                                try:
                                    await db_evo.disconnect()
                                    logger.info(f"Conexão com {evo_db_name_atual} encerrada.")
                                except Exception as e:
                                    logger.error(f"Erro ao encerrar conexão com {evo_db_name_atual}: {e}")

                            # Abre nova conexão
                            log_metadata.set({"evo_db_host": evo_db_host, "evo_db_name": evo_db_name})
                            logger.info(f"Conectando à instância {evo_db_host} - {evo_db_name}...")
                            db_evo = await conecta_leaper_db_evo(evo_db_host, evo_db_name, min_size=1, max_size=1)
                            evo_db_name_atual = evo_db_name

                        # Processa normalmente as companies da instância atual
                        try:
                            await processa_instancia_wpp(db_core, db_evo, evo_db_host, evo_db_name, session, companies_ids)
                        except Exception as e:
                            logger.error(f"Erro ao processar instância {evo_db_host} - {evo_db_name}: {e}")

                    # Fecha a última conexão usada
                    if db_evo:
                        try:
                            await db_evo.disconnect()
                            logger.info(f"Conexão final com {evo_db_name_atual} encerrada.")
                        except Exception as e:
                            logger.error(f"Erro ao encerrar última conexão com {evo_db_name_atual}: {e}")

                        
                        
                # ------------------------------------------------------------------------------------------------------------------------------------------
                # TIMER ATÉ PROXIMA EXECUÇÃO -> 5 minutos = 300 segundos
                # ------------------------------------------------------------------------------------------------------------------------------------------
                await asyncio.sleep(300) 
                

        except Exception:
            logger.exception("Erro inesperado no loop principal")
            raise
        finally:
            logger.info("Encerrando conexões...")

         
            try:
                await db_core.disconnect()
            except Exception:
                logger.exception("Erro ao desconectar db_core")

            
            # TIME ELAPSED
            elapsed_td = datetime.now() - t0
            logger.info(f"Processo encerrado após {elapsed_td.total_seconds():.2f} segundos")


            

# ================================================================================================================================================================================
# DEFINE UVLOOP COMO LOOP DE EVENTOS
# ================================================================================================================================================================================
uvloop.install()

                
# ================================================================================================================================================================================
# INICIA O LOOP
# ================================================================================================================================================================================
if __name__ == "__main__":
    asyncio.run(main())
