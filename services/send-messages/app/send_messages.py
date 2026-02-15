# ============================================================================================================================================
# ENVIA MENSAGENS AGENDADAS PELO STATUS_ANALYZER
# ============================================================================================================================================
import os, sys, aiohttp, json, asyncio, uvloop
from pathlib import Path
from datetime import datetime, timedelta, time

_app = Path(__file__).resolve().parent
for _sub in ("logging", "db", "messaging"):
    sys.path.append(str(_app / _sub))
from log_config import logger, log_ip, log_host, log_action, log_metadata
from db_connections_async import conecta_leaper_db_core
from envio_mensagens import envia_mensagem_com_botao_whatsapp

sys.path.append(str(_app / "scheduling"))
from envia_pendentes_company import envia_pendentes_company, reverte_sending_travados


# ========================================================================================================================================================================
# CONFIGURAÇÕES DE AMBIENTE
# ========================================================================================================================================================================
SERVER_IP = os.getenv("SERVER_IP")
SERVER_HOST = os.getenv("SERVER_HOST")
WORKERS_GLOBAIS = 4

# Intervalo de execução em segundos (5 minutos)
INTERVALO_EXECUCAO = 300

# Janela de 24h da API Meta (23h50min para ter margem)
JANELA_META_HORAS = 23
JANELA_META_MINUTOS = 50


# ================================================================================================================================================================================
# CONSULTA COMPANIES COM MENSAGENS PENDENTES (QUERY ÚNICA COM JOIN)
# ================================================================================================================================================================================
async def consulta_companies_com_pendentes(db_core):
    """
    Busca companies ativas (not disabled) que têm mensagens pendentes.
    Retorna company_id, service_ai_last_response_at, tel_resp_company e qtd de leads pendentes (distinct).
    """
    try:
        query = """
            SELECT
                c.id AS company_id,
                c.metadata->>'service_ai_last_response_at' AS service_ai_last_response_at,
                c.metadata->>'service_ai_config_phonenumber' AS tel_resp_company,
                c.metadata->>'service_ai_last_opening_msg_sent_at' AS service_ai_last_opening_msg_sent_at,
                COUNT(DISTINCT lst.lead_id) AS qtd_leads_pendentes
            FROM company c
            INNER JOIN lead_status_transition lst ON lst.company_id = c.id
            WHERE COALESCE((c.metadata->>'disabled')::boolean, false) = false
              AND lst.message_status = 'pending'
              -- AND c.id = '00b4190c-9c5f-4afe-aaf7-47bbd9d5356f'
            GROUP BY c.id, c.metadata->>'service_ai_last_response_at', c.metadata->>'service_ai_config_phonenumber', c.metadata->>'service_ai_last_opening_msg_sent_at'
            ORDER BY c.id
        """

        async with db_core.connection() as conn:
            rows = await asyncio.wait_for(
                conn.fetch_all(query=query),
                timeout=30
            )

        return [dict(row) for row in rows]

    except Exception as e:
        logger.error(f"Erro ao consultar companies com pendentes: {e}")
        return []




# ================================================================================================================================================================================
# ATUALIZA METADATA DA COMPANY COM DATA DE ENVIO DA MSG ABERTURA
# ================================================================================================================================================================================
async def atualiza_opening_msg_sent_at(db_core, company_id):
    """Grava service_ai_last_opening_msg_sent_at no metadata da company após envio da msg_abertura."""
    try:
        query = """
            UPDATE company
            SET metadata = jsonb_set(
                COALESCE(metadata, '{}'::jsonb),
                '{service_ai_last_opening_msg_sent_at}',
                to_jsonb(to_char(NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo', 'YYYY-MM-DD"T"HH24:MI:SS.US'))
            ),
            updated_at = NOW()
            WHERE id = :company_id
        """

        async with db_core.connection() as conn:
            await asyncio.wait_for(
                conn.execute(query=query, values={"company_id": company_id}),
                timeout=30
            )

        logger.info(f"service_ai_last_opening_msg_sent_at atualizado para company {company_id}")
        return True

    except Exception as e:
        logger.error(f"Erro ao atualizar opening_msg_sent_at da company {company_id}: {e}")
        return False


# ================================================================================================================================================================================
# MONTA PAYLOAD DO WHATSAPP (MENSAGEM DE ABERTURA DE JANELA - JANELA FECHADA)
# ================================================================================================================================================================================
def monta_payload_msg_abertura(tel_resp_company, company_id, qtd_leads_pendentes):
    """
    Monta o payload para envio de mensagem de abertura de janela quando a janela de 24h está fechada.
    """
    # FORMATA QTD LEADS (ex: "7 leads")
    qtd_leads_formatado = f"{qtd_leads_pendentes} leads"

    # DETERMINA O TEMPLATE
    template_name = "pending_approval_summary"

    # PAYLOAD DO BOTÃO
    payload_open_window = json.dumps({
        "company_id": str(company_id),
        "action": "open_24h_window"
    })

    # MONTA COMPONENTS (body com qtd de leads + botão)
    components = [
        {
            "type": "body",
            "parameters": [
                {"type": "text", "parameter_name": "leads", "text": qtd_leads_formatado}
            ]
        },
        {
            "type": "button",
            "sub_type": "quick_reply",
            "index": 0,
            "parameters": [{"type": "payload", "payload": payload_open_window}]
        }
    ]

    # MONTAGEM DO PAYLOAD FINAL
    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": str(tel_resp_company),
        "type": "template",
        "template": {
            "name": template_name,
            "language": {"code": "pt_BR"},
            "components": components
        }
    }

    return payload


# ================================================================================================================================================================================
# VERIFICA SE JANELA DE 24H ESTÁ ABERTA
# ================================================================================================================================================================================
def verifica_janela_aberta(service_ai_last_response_at):
    """
    Verifica se a janela de 24h da API Meta está aberta.

    Retorna:
    - "aberta": janela aberta, pode enviar normalmente
    - "fechada": passou 23h50min, enviar mensagem de abertura
    - "nova": não existe last_response_at, janela nova (pode enviar)
    """
    # Verifica se não existe, é vazio, ou é "null" (string)
    if not service_ai_last_response_at or service_ai_last_response_at in ("null", "None", ""):
        return "nova"

    try:
        # Tenta parsear a data (pode vir como string do metadata)
        if isinstance(service_ai_last_response_at, str):
            # Tenta diferentes formatos
            for fmt in ["%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"]:
                try:
                    last_response = datetime.strptime(service_ai_last_response_at, fmt)
                    break
                except ValueError:
                    continue
            else:
                logger.warning(f"Não foi possível parsear service_ai_last_response_at: {service_ai_last_response_at}")
                return "nova"
        else:
            last_response = service_ai_last_response_at

        # Calcula tempo decorrido
        agora = datetime.now()
        tempo_decorrido = agora - last_response
        limite_janela = timedelta(hours=JANELA_META_HORAS, minutes=JANELA_META_MINUTOS)

        if tempo_decorrido >= limite_janela:
            return "fechada"
        else:
            return "aberta"

    except Exception as e:
        logger.error(f"Erro ao verificar janela: {e}")
        return "nova"


# ================================================================================================================================================================================
# PROCESSA COMPANY
# ================================================================================================================================================================================
async def processa_company(db_core, session, company_info):
    """
    Processa uma company:
    1. Verifica se janela de 24h está aberta
    2. Se fechada: envia mensagem de abertura de janela
    3. Se aberta: envia mensagens individuais (mais recente por lead)
    """
    company_id = company_info.get("company_id")
    service_ai_last_response_at = company_info.get("service_ai_last_response_at")
    service_ai_last_opening_msg_sent_at = company_info.get("service_ai_last_opening_msg_sent_at")
    tel_resp_company = company_info.get("tel_resp_company")
    qtd_leads_pendentes = company_info.get("qtd_leads_pendentes", 0)

    log_metadata.set({
        "company_id": company_id,
        "tel_resp_company": tel_resp_company,
        "qtd_leads_pendentes": qtd_leads_pendentes
    })

    logger.info(f"Processando company {company_id} ({qtd_leads_pendentes} leads pendentes)")

    try:
        # Verifica status da janela
        status_janela = verifica_janela_aberta(service_ai_last_response_at)
        logger.info(f"Status da janela: {status_janela}")

        if status_janela in ("fechada", "nova"):
            # Verifica se já enviou msg_abertura hoje
            ja_enviou_hoje = False
            if service_ai_last_opening_msg_sent_at and service_ai_last_opening_msg_sent_at not in ("null", "None", ""):
                try:
                    for fmt in ["%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"]:
                        try:
                            last_opening = datetime.strptime(service_ai_last_opening_msg_sent_at, fmt)
                            break
                        except ValueError:
                            continue
                    else:
                        last_opening = None

                    if last_opening and last_opening.date() == datetime.now().date():
                        ja_enviou_hoje = True
                except Exception:
                    pass

            if ja_enviou_hoje:
                logger.info(f"Msg abertura já enviada hoje para company {company_id}, pulando")
                return {"company_id": company_id, "enviadas": 0, "abertura_enviada": False}

            # Janela fechada ou nova - envia mensagem de abertura
            payload_abertura = monta_payload_msg_abertura(tel_resp_company, company_id, qtd_leads_pendentes)

            if payload_abertura:
                status_envio = await envia_mensagem_com_botao_whatsapp(session, payload_abertura)
                if status_envio:
                    await atualiza_opening_msg_sent_at(db_core, company_id)
                    logger.info(f"Mensagem de abertura enviada para company {company_id} (janela: {status_janela})")
                    return {"company_id": company_id, "enviadas": 0, "abertura_enviada": True}
                else:
                    logger.error(f"Falha ao enviar mensagem de abertura para company {company_id}")

            return {"company_id": company_id, "enviadas": 0, "abertura_enviada": False}

        # Janela aberta - envia mensagens individuais via módulo compartilhado
        result_envio = await envia_pendentes_company(db_core, session, company_id)
        enviadas = result_envio.get("enviadas", 0)
        falhas = result_envio.get("falhas", 0)

        logger.info(f"Company {company_id} concluída | Enviadas: {enviadas} | Falhas: {falhas}")

        return {"company_id": company_id, "enviadas": enviadas, "falhas": falhas, "abertura_enviada": False}

    except Exception as e:
        logger.exception(f"Erro ao processar company {company_id}: {e}")
        return {"company_id": company_id, "enviadas": 0, "abertura_enviada": False}


# ================================================================================================================================================================================
# MAIN
# ================================================================================================================================================================================
async def main():

    log_ip.set(SERVER_IP)
    log_host.set(SERVER_HOST)
    log_action.set('send_messages')

    t0 = datetime.now()
    logger.info("Iniciando serviço de envio de mensagens...")

    # Conecta ao banco
    db_core = await conecta_leaper_db_core(min_size=1, max_size=WORKERS_GLOBAIS)

    async with aiohttp.ClientSession() as session:
        try:
            while True:

                # Verifica horário comercial (seg-sex, 9h-19h)
                now = datetime.now()
                dentro_do_horario = (
                    0 <= now.weekday() <= 4 and
                    time(9, 0) <= now.time() <= time(19, 0)
                )

                if not dentro_do_horario:
                    logger.debug("Fora do horário comercial, aguardando próximo ciclo")
                    await asyncio.sleep(INTERVALO_EXECUCAO)
                    continue

                # Safety net: reverte registros 'sending' travados há mais de 5 minutos
                await reverte_sending_travados(db_core)

                # Consulta companies com mensagens pendentes (query única com JOIN)
                companies = await consulta_companies_com_pendentes(db_core)

                if companies:
                    logger.info(f"Encontradas {len(companies)} companies com mensagens pendentes")

                    # Processa companies sequencialmente (para respeitar limites da API)
                    total_enviadas = 0
                    total_aberturas = 0

                    for company_info in companies:
                        result = await processa_company(db_core, session, company_info)
                        total_enviadas += result.get("enviadas", 0)
                        if result.get("abertura_enviada"):
                            total_aberturas += 1

                    logger.info(f"Ciclo concluído | Companies: {len(companies)} | Enviadas: {total_enviadas} | Aberturas: {total_aberturas}")

                else:
                    logger.debug("Nenhuma company com mensagens pendentes")

                # Timer até próxima execução -> 5 minutos
                await asyncio.sleep(INTERVALO_EXECUCAO)

        except Exception:
            logger.exception("Erro inesperado no loop principal")
            raise
        finally:
            logger.info("Encerrando conexões...")

            try:
                await db_core.disconnect()
            except Exception:
                logger.exception("Erro ao desconectar db_core")

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
