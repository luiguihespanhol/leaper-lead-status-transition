# ========================================================================================================================================================================
# ========================================================================================================================================================================
# WEBHOOK QUE RECEBE RETORNO DA MENSAGEM ENVIADA AO RESPONSÁVEL PELA COMPANY (META WHATSAPP API)
# ========================================================================================================================================================================
# ========================================================================================================================================================================

import json, logging, sys, os, aiohttp, hmac, hashlib
from fastapi import APIRouter, FastAPI, Request, Depends
from fastapi.responses import JSONResponse, PlainTextResponse
from pathlib import Path
from datetime import datetime
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parent / "shared"))
from log_config import logger, log_metadata
from leaper_core_apis import get_auth_token_company_leaper, change_lead_status

sys.path.append(str(Path(__file__).resolve().parent / "shared_ai"))
from envia_pendentes_company import envia_pendentes_company



# ========================================================================================================================================================================
# INICIALIZA
# ========================================================================================================================================================================
router = APIRouter()

META_APP_SECRET = os.getenv("META_APP_SECRET")


async def get_db(request: Request):
    return request.app.conn

async def get_http_resources(request: Request):
    return request.app.http_session, request.app.http_semaphore


# ========================================================================================================================================================================
# VALIDAÇÃO HMAC-SHA256 DO WEBHOOK META
# ========================================================================================================================================================================
def validate_meta_signature(raw_body: bytes, signature_header: str) -> bool:
    """
    Valida a assinatura HMAC-SHA256 da requisição do webhook Meta.
    Retorna True se válida, False caso contrário.
    """
    if not META_APP_SECRET:
        logger.error("META_APP_SECRET não configurado")
        return False

    if not signature_header:
        return False

    # Remove prefixo "sha256=" do header
    if not signature_header.startswith("sha256="):
        return False

    expected_signature = signature_header[7:]  # Remove "sha256="

    # Calcula HMAC-SHA256
    computed_signature = hmac.new(
        key=META_APP_SECRET.encode('utf-8'),
        msg=raw_body,
        digestmod=hashlib.sha256
    ).hexdigest().lower()

    # Comparação segura contra timing attacks
    return hmac.compare_digest(computed_signature, expected_signature.lower())
    


# ========================================================================================================================================================================
# BUSCA DADOS DO TRACKING NO DB PELO ID
# ========================================================================================================================================================================
async def get_tracking_data_by_id(conn, tracking_id):
    """Busca os dados do lead_status_transition pelo ID para recuperar informações necessárias."""
    try:
        query = """
            SELECT
                id,
                company_id,
                lead_id,
                metadata
            FROM lead_status_transition
            WHERE id = :id
        """
        result = await conn.fetch_one(query=query, values={"id": tracking_id})
        return result
    except Exception as e:
        logger.error(f"Erro ao buscar lead_status_transition pelo id {tracking_id}: {e}")
        return None


# ========================================================================================================================================================================
# UPDATE TRACKING AI NO DB
# ========================================================================================================================================================================
async def update_lead_status_response(conn, id_tb_tracking, status_name, metadata_to_update):
    try:
        query = """
            UPDATE lead_status_transition
            SET
                status_choose   = :status_choose,
                response_date   = :response_date,
                message_status  = 'answered',
                metadata        = (metadata::jsonb || CAST(:metadata_update AS jsonb))::json,
                updated_at      = NOW()
            WHERE id = :id
        """
        values = {
            "status_choose":    status_name,
            "response_date":    datetime.now(),
            "metadata_update":  json.dumps(metadata_to_update, ensure_ascii=False),
            "id":               id_tb_tracking
        }
        await conn.execute(query, values=values)

    except Exception as e:
        logger.error(f"Erro ao atualizar lead_status_transition para id {id_tb_tracking}: {e}")

    
# ========================================================================================================================================================================
# ATUALIZA service_ai_last_response_at NO METADATA DA COMPANY (RENOVA JANELA 24H)
# ========================================================================================================================================================================
async def atualiza_service_ai_last_response_at(conn, company_id):
    """Grava service_ai_last_response_at = NOW() no metadata da company. Qualquer clique de botão renova a janela de 24h."""
    try:
        query = """
            UPDATE company
            SET metadata = jsonb_set(
                COALESCE(metadata, '{}'::jsonb),
                '{service_ai_last_response_at}',
                to_jsonb(to_char(NOW() AT TIME ZONE 'UTC' AT TIME ZONE 'America/Sao_Paulo', 'YYYY-MM-DD"T"HH24:MI:SS.US'))
            ),
            updated_at = NOW()
            WHERE id = :company_id
        """
        await conn.execute(query=query, values={"company_id": str(company_id)})
        logger.info(f"service_ai_last_response_at atualizado para company {company_id}")
    except Exception as e:
        logger.error(f"Erro ao atualizar service_ai_last_response_at da company {company_id}: {e}")


# ========================================================================================================================================================================
# PROCESSA MENSAGEM DE RETORNO (META WHATSAPP API)
# ========================================================================================================================================================================
async def processa_msg_retorno(conn, http_session, message_content_json):
    """
    Processa o webhook da Meta WhatsApp API quando um botão é clicado.
    Suporta 3 cenários:
    1. Interactive buttons (novo): type="interactive", button_reply.id = "ACTION|inserted_id"
    2. Template buttons (antigo): type="button", button.payload = JSON com action e inserted_id
    3. Msg abertura (template): type="button", button.payload = JSON com action="open_24h_window"
    """

    # EXTRAI A MENSAGEM DO FORMATO META WEBHOOK
    try:
        entry = message_content_json.get("entry", [])
        if not entry:
            return

        changes = entry[0].get("changes", [])
        if not changes:
            return

        value = changes[0].get("value", {})
        messages = value.get("messages", [])
        if not messages:
            return

        message = messages[0]
        msg_type = message.get("type")

        # EXTRAI TELEFONE DO CONTATO (SE DISPONÍVEL)
        contact_phone = ""
        contacts = value.get("contacts", [])
        if contacts:
            contact_phone = contacts[0].get("wa_id", "")

        # =============================================================================================================
        # CENÁRIO 1: INTERACTIVE BUTTONS (novo formato - msgs de confirmação)
        # type="interactive", button_reply.id = "KEEP|123" / "CHANGE|123" / "REVERSED|123"
        # =============================================================================================================
        if msg_type == "interactive":
            interactive = message.get("interactive", {})
            if interactive.get("type") != "button_reply":
                return

            button_reply = interactive.get("button_reply", {})
            button_id = button_reply.get("id", "")

            if "|" not in button_id:
                logger.warning(f"Formato de button_reply.id inesperado: {button_id}")
                return

            acao_escolhida, inserted_id = button_id.split("|", 1)
            logger.info(f"[Interactive] Ação: {acao_escolhida}, inserted_id: {inserted_id}")

        # =============================================================================================================
        # CENÁRIO 2 e 3: TEMPLATE BUTTONS (formato antigo + msg_abertura)
        # type="button", button.payload = JSON
        # =============================================================================================================
        elif msg_type == "button":
            button_data = message.get("button", {})
            button_payload_str = button_data.get("payload")

            if not button_payload_str:
                return

            try:
                button_payload = json.loads(button_payload_str)
            except json.JSONDecodeError:
                logger.error(f"Erro ao fazer parse do payload do botão: {button_payload_str}")
                return

            # CENÁRIO 3: MSG ABERTURA (open_24h_window)
            if button_payload.get("action") == "open_24h_window":
                company_id = button_payload.get("company_id")
                if not company_id:
                    logger.warning("company_id não encontrado no payload open_24h_window")
                    return

                logger.info(f"[Abertura] Clique no botão de abertura de janela - company: {company_id}")
                await atualiza_service_ai_last_response_at(conn, company_id)

                # Envia imediatamente as mensagens pendentes dessa company
                result_envio = await envia_pendentes_company(conn, http_session, company_id)
                logger.info(f"[Abertura] Envio imediato company {company_id}: {result_envio}")
                return

            # CENÁRIO 2: TEMPLATE ANTIGO (compatibilidade)
            acao_escolhida = button_payload.get("action")
            inserted_id = button_payload.get("inserted_id")

            if not acao_escolhida or not inserted_id:
                logger.warning("action ou inserted_id não encontrados no payload do botão")
                return

            logger.info(f"[Template] Ação: {acao_escolhida}, inserted_id: {inserted_id}")

        else:
            # Tipo de mensagem não suportado (ex: text, image, etc.)
            return

        # =============================================================================================================
        # PROCESSAMENTO COMUM (KEEP / CHANGE / REVERSED)
        # =============================================================================================================

        # BUSCA OS DADOS DO TRACKING NO DB
        tracking_data = await get_tracking_data_by_id(conn, inserted_id)

        if not tracking_data:
            logger.error(f"Registro não encontrado para inserted_id: {inserted_id}")
            return

        id_tb_tracking = str(tracking_data["id"])
        id_company = str(tracking_data["company_id"])
        id_lead = str(tracking_data["lead_id"])
        metadata = tracking_data["metadata"]

        # PARSE DO METADATA JSON
        if isinstance(metadata, str):
            metadata = json.loads(metadata)

        # EXTRAI OS DADOS DO METADATA BASEADO NA AÇÃO
        if acao_escolhida == "KEEP":
            choice_status_id = metadata.get("pre_status_id")
            choice_status_name = metadata.get("pre_status_name")
            choice_status_code = metadata.get("pre_status_code")
        elif acao_escolhida == "CHANGE":
            choice_status_id = metadata.get("ai_suggestion_status_id")
            choice_status_name = metadata.get("ai_suggestion_status_name")
            choice_status_code = metadata.get("ai_suggestion_status_code")
        elif acao_escolhida == "REVERSED":
            choice_status_id = metadata.get("reversed_ai_suggestion_status_id")
            choice_status_name = metadata.get("reversed_ai_suggestion_status_name")
            choice_status_code = metadata.get("reversed_ai_suggestion_status_code")
        else:
            logger.warning(f"Ação desconhecida: {acao_escolhida}")
            return

        # LOGS
        log_metadata.set({
            "sender_phone": contact_phone,
            "company_id": id_company,
            "lead_id": id_lead,
            "tracking_ai_id": id_tb_tracking,
            "choice": acao_escolhida,
            "choice_status_id": choice_status_id,
            "choice_status_name": choice_status_name,
            "choice_status_code": choice_status_code
        })

        # ATUALIZA service_ai_last_response_at (RENOVA JANELA 24H)
        await atualiza_service_ai_last_response_at(conn, id_company)

        # GRAVA TRACKING NO DB
        metadata_to_update = {"choice_status_id": choice_status_id, "choice_status_name": choice_status_name, "choice_status_code": choice_status_code, "user_action": acao_escolhida}
        await update_lead_status_response(conn, id_tb_tracking, choice_status_name, metadata_to_update)

        # CASO O GESTOR MANTENHA O STATUS --------------------------------------------------------------------------------------------------
        if acao_escolhida == "KEEP":
            logger.info("Gestor optou por manter o status atual")
            return

        # CASO SEJA ALTERAÇÃO (CHANGE OU REVERSED) -----------------------------------------------------------------------------------------
        auth_token_app_leaper = await get_auth_token_company_leaper(http_session, id_company)

        if not auth_token_app_leaper:
            logger.error("Não foi possível obter auth_token_app_leaper")
            return

        # ALTERA STATUS LEAD
        status_change = await change_lead_status(http_session, auth_token_app_leaper, id_lead, choice_status_id)
        if status_change:
            logger.info("Status do lead atualizado com sucesso")
        else:
            logger.error("Erro ao atualizar status do lead")

    except Exception as e:
        logger.exception(f"Erro ao processar webhook Meta: {e}")

        
        

# ========================================================================================================================================================================
# ROTA GET PARA VERIFICAÇÃO DO WEBHOOK (META REQUER PARA VALIDAÇÃO)
# ========================================================================================================================================================================
# @router.get('/')
# async def verify_webhook(request: Request):
#     hub_challenge = request.query_params.get("hub.challenge", "")
#     return PlainTextResponse(content=hub_challenge, status_code=200)


# ========================================================================================================================================================================
# ROTA POST PARA RECEBER MENSAGENS
# ========================================================================================================================================================================
@router.post('')
@router.post('/')
async def receive_message(request: Request, conn = Depends(get_db), http_resources=Depends(get_http_resources)):

    http_session, http_semaphore = http_resources

    # Lê o raw body antes de qualquer processamento
    raw_body = await request.body()

    # Valida assinatura HMAC-SHA256
    signature_header = request.headers.get("x-hub-signature-256", "")
    has_signature = bool(signature_header)
    is_valid = validate_meta_signature(raw_body, signature_header)

    logger.info(f"HMAC validation - header_present: {has_signature}, body_size: {len(raw_body)}, match: {is_valid}")

    if not is_valid:
        logger.warning("Requisição rejeitada: assinatura HMAC inválida")
        return JSONResponse(content={"status": "error", "message": "Invalid signature"}, status_code=403)

    # Processa o payload
    message_content_decoded = raw_body.decode('utf-8')
    message_content_json = json.loads(message_content_decoded)

    logger.info(f"Payload recebido: {message_content_json}")

    try:
        async with http_semaphore:
            json_response = await processa_msg_retorno(conn, http_session, message_content_json)
            return JSONResponse(content=json_response)
    except Exception as e:
        logger.exception(f"Erro: {e}")
        erro = {"status": "error", "message": f"{e}"}
        return JSONResponse(content=erro)
       
     
