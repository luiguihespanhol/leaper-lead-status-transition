# ============================================================================================================================================
# MÓDULO COMPARTILHADO: ENVIO DE MENSAGENS PENDENTES POR COMPANY
# Usado por: send_messages e webhook (ao clicar em open_24h_window)
# ============================================================================================================================================
import json, asyncio
from log_config import logger, log_metadata
from envio_mensagens import envia_mensagem_com_botao_whatsapp


# ================================================================================================================================================================================
# STATUS DE EMOJIS
# ================================================================================================================================================================================
STATUS_EMOJIS = {
    "LEAD_START": "⏳",
    "CONTATO_INICIADO": "▶️",
    "QUALIFICACAO": "📝",
    "NEGOCIACAO": "💱",
    "PROPOSTA_ENVIADA": "➡️",
    "END_WON": "✅",
    "END_LOST": "❌"
}


# ================================================================================================================================================================================
# CONSULTA MENSAGENS PENDENTES POR COMPANY (APENAS A MAIS RECENTE POR LEAD)
# ================================================================================================================================================================================
async def consulta_e_reserva_mensagens_pendentes(db_core, company_id):
    """
    Para cada lead da company, seleciona o registro mais recente (por message_schedule_date)
    onde message_status = 'pending', e atomicamente marca como 'sending' para evitar
    que outro processo (webhook ou send_messages) pegue as mesmas linhas.
    """
    try:
        query = """
            WITH ranked AS (
                SELECT
                    lst.id
                FROM lead_status_transition lst
                WHERE lst.company_id = :company_id
                  AND lst.message_status = 'pending'
            ),
            top_per_lead AS (
                SELECT DISTINCT ON (lst.lead_id) lst.id
                FROM lead_status_transition lst
                INNER JOIN ranked r ON r.id = lst.id
                ORDER BY lst.lead_id, lst.message_schedule_date DESC
                LIMIT 20
            ),
            reserved AS (
                UPDATE lead_status_transition
                SET message_status = 'sending',
                    updated_at = NOW()
                WHERE id IN (SELECT id FROM top_per_lead)
                RETURNING *
            )
            SELECT * FROM reserved
            ORDER BY message_schedule_date ASC
        """

        async with db_core.connection() as conn:
            rows = await asyncio.wait_for(
                conn.fetch_all(query=query, values={"company_id": company_id}),
                timeout=30
            )

        return [dict(row) for row in rows]

    except Exception as e:
        logger.error(f"Erro ao consultar/reservar mensagens pendentes da company {company_id}: {e}")
        return []


# ================================================================================================================================================================================
# REVERTE REGISTRO PARA 'pending' EM CASO DE FALHA NO ENVIO
# ================================================================================================================================================================================
async def reverte_para_pending(db_core, registro_id):
    """Reverte um registro de 'sending' para 'pending' quando o envio falha."""
    try:
        query = """
            UPDATE lead_status_transition
            SET message_status = 'pending',
                updated_at = NOW()
            WHERE id = :registro_id
              AND message_status = 'sending'
        """
        async with db_core.connection() as conn:
            await asyncio.wait_for(
                conn.execute(query=query, values={"registro_id": str(registro_id)}),
                timeout=30
            )
        logger.info(f"Registro {registro_id} revertido para 'pending'")
        return True
    except Exception as e:
        logger.error(f"Erro ao reverter registro {registro_id} para 'pending': {e}")
        return False


# ================================================================================================================================================================================
# SAFETY NET: REVERTE REGISTROS 'sending' TRAVADOS HÁ MAIS DE 5 MINUTOS
# ================================================================================================================================================================================
async def reverte_sending_travados(db_core):
    """Reverte para 'pending' registros que ficaram em 'sending' por mais de 5 minutos (processo morreu)."""
    try:
        query = """
            UPDATE lead_status_transition
            SET message_status = 'pending',
                updated_at = NOW()
            WHERE message_status = 'sending'
              AND updated_at < NOW() - INTERVAL '5 minutes'
            RETURNING id
        """
        async with db_core.connection() as conn:
            rows = await asyncio.wait_for(
                conn.fetch_all(query=query),
                timeout=30
            )
        if rows:
            logger.info(f"Safety net: {len(rows)} registros revertidos de 'sending' para 'pending'")
        return len(rows)
    except Exception as e:
        logger.error(f"Erro ao reverter registros 'sending' travados: {e}")
        return 0


# ================================================================================================================================================================================
# ATUALIZA STATUS DAS MENSAGENS APÓS ENVIO
# ================================================================================================================================================================================
async def atualiza_mensagem_enviada(db_core, registro_id, company_id, lead_id):
    """
    Após envio bem-sucedido:
    - Na linha enviada: message_sent_date = NOW(), message_status = 'sent'
    - Nas demais linhas do mesmo lead-company com status 'pending': message_status = 'ignored'
    """
    try:
        query_sent = """
            UPDATE lead_status_transition
            SET message_sent_date = NOW(),
                message_status = 'sent',
                updated_at = NOW()
            WHERE id = :registro_id
        """

        query_ignored = """
            UPDATE lead_status_transition
            SET message_status = 'ignored',
                updated_at = NOW()
            WHERE company_id = :company_id
              AND lead_id = :lead_id
              AND message_status = 'pending'
              AND id != :registro_id
        """

        async with db_core.connection() as conn:
            await asyncio.wait_for(
                conn.execute(query=query_sent, values={"registro_id": str(registro_id)}),
                timeout=30
            )
            await asyncio.wait_for(
                conn.execute(query=query_ignored, values={
                    "company_id": str(company_id),
                    "lead_id": str(lead_id),
                    "registro_id": str(registro_id)
                }),
                timeout=30
            )

        return True

    except Exception as e:
        logger.error(f"Erro ao atualizar status das mensagens (registro {registro_id}): {e}")
        return False


# ================================================================================================================================================================================
# MONTA PAYLOAD DO WHATSAPP (MENSAGEM INDIVIDUAL DE CONFIRMAÇÃO - INTERACTIVE BUTTONS)
# ================================================================================================================================================================================
def monta_payload_whatsapp(registro):
    """Monta o payload para envio de mensagem de confirmação de alteração de status via WhatsApp (API oficial Meta - interactive buttons)."""

    metadata = registro.get("metadata", {})
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except:
            metadata = {}

    # Extrai dados do metadata
    tel_resp_company = metadata.get("manager_phone", "")
    tel_lead = metadata.get("lead_phone", "")
    lead_lid = metadata.get("lead_lid", "")
    nome_lead = metadata.get("lead_name", "")

    pre_status_name = metadata.get("pre_status_name", "")
    pre_status_code = metadata.get("pre_status_code", "")

    ai_suggestion_status_name = metadata.get("ai_suggestion_status_name", "")
    ai_suggestion_status_code = metadata.get("ai_suggestion_status_code", "")

    reversed_ai_suggestion_status_id = metadata.get("reversed_ai_suggestion_status_id")
    reversed_ai_suggestion_status_name = metadata.get("reversed_ai_suggestion_status_name")
    reversed_ai_suggestion_status_code = metadata.get("reversed_ai_suggestion_status_code")

    inserted_id = registro.get("id")

    # EMOJIS
    pre_status_emoji = STATUS_EMOJIS.get(pre_status_code, "")
    ai_suggestion_status_emoji = STATUS_EMOJIS.get(ai_suggestion_status_code, "")

    # STATUS COM EMOJI (negrito e maiúsculo) para o body - remove prefixo "FINALIZADO - "
    pre_status_body = pre_status_name.upper().replace("FINALIZADO - ", "") if pre_status_name else ""
    ai_suggestion_body = ai_suggestion_status_name.upper().replace("FINALIZADO - ", "") if ai_suggestion_status_name else ""
    pre_status_emoji_and_name = f"*{pre_status_emoji} {pre_status_body}*" if pre_status_name else ""
    ai_suggestion_status_emoji_and_name = f"*{ai_suggestion_status_emoji} {ai_suggestion_body}*" if ai_suggestion_status_name else ""

    # MONTA IDENTIFICAÇÃO DO LEAD NO BODY
    tem_nome = nome_lead not in (None, "", " ", "0")
    tel_lead_formatado = f"+{tel_lead}" if tel_lead else ""

    if tem_nome:
        lead_identificacao = f"*{nome_lead}* ({tel_lead_formatado})" if tel_lead_formatado else f"*{nome_lead}*"
    elif tel_lead_formatado:
        lead_identificacao = f"O lead ({tel_lead_formatado})"
    elif lead_lid:
        lead_identificacao = f"O lead ({lead_lid})"
    else:
        lead_identificacao = "O lead"

    # MONTA BODY TEXT
    body_text = f"{lead_identificacao} recebeu uma sugestão de alteração de status, de {pre_status_emoji_and_name} para {ai_suggestion_status_emoji_and_name}."

    # BOTÕES
    pre_status_btn = pre_status_name.upper().replace("FINALIZADO - ", "")
    ai_suggestion_btn = ai_suggestion_status_name.upper().replace("FINALIZADO - ", "")

    buttons = [
        {
            "type": "reply",
            "reply": {
                "id": f"KEEP|{inserted_id}",
                "title": f"{pre_status_emoji} {pre_status_btn}"[:20]
            }
        },
        {
            "type": "reply",
            "reply": {
                "id": f"CHANGE|{inserted_id}",
                "title": f"{ai_suggestion_status_emoji} {ai_suggestion_btn}"[:20]
            }
        }
    ]

    # ADICIONA TERCEIRO BOTÃO PARA END_WON E END_LOST (reversed)
    if ai_suggestion_status_code in ("END_WON", "END_LOST") and reversed_ai_suggestion_status_id and reversed_ai_suggestion_status_name:
        reversed_emoji = STATUS_EMOJIS.get(reversed_ai_suggestion_status_code, "")
        reversed_btn = reversed_ai_suggestion_status_name.upper().replace("FINALIZADO - ", "")
        buttons.append({
            "type": "reply",
            "reply": {
                "id": f"REVERSED|{inserted_id}",
                "title": f"{reversed_emoji} {reversed_btn}"[:20]
            }
        })

    # MONTAGEM DO PAYLOAD FINAL (interactive buttons)
    payload = {
        "messaging_product": "whatsapp",
        "recipient_type": "individual",
        "to": str(tel_resp_company),
        "type": "interactive",
        "interactive": {
            "type": "button",
            "body": {
                "text": body_text
            },
            "action": {
                "buttons": buttons
            }
        }
    }

    return payload


# ================================================================================================================================================================================
# PROCESSA UMA MENSAGEM AGENDADA
# ================================================================================================================================================================================
async def processa_mensagem(db_core, session, registro):
    """Processa uma mensagem agendada: monta payload, envia e atualiza status."""

    registro_id = registro.get("id")
    company_id = registro.get("company_id")
    lead_id = registro.get("lead_id")

    metadata = registro.get("metadata", {})
    if isinstance(metadata, str):
        try:
            metadata = json.loads(metadata)
        except:
            metadata = {}

    tel_resp_company = metadata.get("manager_phone", "")

    log_metadata.set({
        "registro_id": registro_id,
        "company_id": company_id,
        "lead_id": lead_id,
        "tel_resp_company": tel_resp_company
    })

    logger.info(f"Processando mensagem (id: {registro_id})")

    try:
        payload = monta_payload_whatsapp(registro)
        status_envio = await envia_mensagem_com_botao_whatsapp(session, payload)

        if status_envio:
            atualizado = await atualiza_mensagem_enviada(db_core, registro_id, company_id, lead_id)
            if atualizado:
                logger.info(f"Mensagem enviada e registrada com sucesso (id: {registro_id})")
                return True
            else:
                logger.error(f"Mensagem enviada mas falhou ao atualizar registro (id: {registro_id})")
                return False
        else:
            logger.error(f"Falha ao enviar mensagem (id: {registro_id})")
            await reverte_para_pending(db_core, registro_id)
            return False

    except Exception as e:
        logger.exception(f"Erro ao processar mensagem (id: {registro_id}): {e}")
        await reverte_para_pending(db_core, registro_id)
        return False


# ================================================================================================================================================================================
# ENVIA TODAS AS MENSAGENS PENDENTES DE UMA COMPANY
# ================================================================================================================================================================================
async def envia_pendentes_company(db_core, session, company_id):
    """
    Busca e envia todas as mensagens pendentes de uma company específica.
    Retorna dict com qtd enviadas e falhas.
    """
    try:
        mensagens = await consulta_e_reserva_mensagens_pendentes(db_core, company_id)

        if not mensagens:
            logger.info(f"Sem mensagens pendentes para company {company_id}")
            return {"enviadas": 0, "falhas": 0}

        logger.info(f"[envia_pendentes_company] {len(mensagens)} mensagens pendentes para company {company_id}")

        enviadas = 0
        falhas = 0

        for i, registro in enumerate(mensagens):
            try:
                result = await processa_mensagem(db_core, session, registro)
                if result:
                    enviadas += 1
                else:
                    falhas += 1
            except Exception as e:
                logger.exception(f"Erro ao processar mensagem: {e}")
                falhas += 1

            # Delay de 0.5s entre envios (não aplica após o último)
            if i < len(mensagens) - 1:
                await asyncio.sleep(0.5)

        logger.info(f"[envia_pendentes_company] Company {company_id} | Enviadas: {enviadas} | Falhas: {falhas}")
        return {"enviadas": enviadas, "falhas": falhas}

    except Exception as e:
        logger.exception(f"Erro ao enviar pendentes da company {company_id}: {e}")
        return {"enviadas": 0, "falhas": 0}
