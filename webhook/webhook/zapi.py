# ========================================================================================================================================================================
# ========================================================================================================================================================================
# WEBHOOK QUE RECEBE RETORNO DA MENSAGEM ENVIADA AO RESPONSÁVEL PELA COMPANY (Z-API)
# ========================================================================================================================================================================
# ========================================================================================================================================================================

import json, logging, sys, os, aiohttp
from fastapi import APIRouter, FastAPI, Request, Depends, HTTPException
from fastapi.responses import JSONResponse
from pathlib import Path
from datetime import datetime
from sqlalchemy import text

sys.path.append(str(Path(__file__).resolve().parent / "shared"))
from log_config import logger, log_metadata
from leaper_core_apis import get_auth_token_company_leaper, change_lead_status

sys.path.append(str(Path(__file__).resolve().parent / "shared_ai"))




# ========================================================================================================================================================================
# VARIÁVEIS DE AMBIENTE
# ========================================================================================================================================================================
WHATSAPP_AUTH_TOKEN = os.environ.get("WHATSAPP_AUTH_TOKEN")


# ========================================================================================================================================================================
# INICIALIZA
# ========================================================================================================================================================================
router = APIRouter()

async def get_db(request: Request):
    return request.app.conn

async def get_http_resources(request: Request):
    return request.app.http_session, request.app.http_semaphore


# ========================================================================================================================================================================
# VALIDAÇÃO DE TOKEN (APENAS PARA Z-API)
# ========================================================================================================================================================================
async def validate_zapi_token(request: Request):
    receive_auth_token = request.query_params.get("receive_auth_token")

    if not receive_auth_token:
        logger.error("Tentativa de acesso sem token receive_auth_token")
        raise HTTPException(status_code=401, detail="Unauthorized")

    if receive_auth_token != WHATSAPP_AUTH_TOKEN:
        logger.error("Tentativa de acesso com token inválido")
        raise HTTPException(status_code=401, detail="Unauthorized")
    


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
# PROCESSA MENSAGEM DE RETORNO
# ========================================================================================================================================================================
async def processa_msg_retorno(conn, http_session, message_content_json):
   
    #logger.info(message_content_json)
    
    contact_phone = message_content_json.get("phone", "")
    
    # MENSAGEM É DE GRUPO
    if '-' in contact_phone or '@' in contact_phone:
        # logger.warning("Mensagem vem de grupo, ignorando")
        return

    # MENSAGEM NÃO É DE BOTÃO
    buttons_response = message_content_json.get("buttonsResponseMessage")
    if not buttons_response:
        # logger.warning("buttonsResponseMessage não encontrado na mensagem")
        return

    # MENSAGEM NÃO TEM O BUTTON ID
    button_id = buttons_response.get("buttonId")
    if not button_id:
        # logger.warning("buttonId não encontrado na mensagem")
        return

    # PROSSEGUE CASO TENHA OS CAMPOS NECESSÁRIOS
    logger.info(f"button_id: {button_id}")
    partes = button_id.split("@")

    acao_escolhida   = partes[0]
    id_tb_tracking   = partes[1]
    id_company       = partes[2]
    id_lead          = partes[3]
    choice_status_id        = partes[4]
    choice_status_name      = partes[5]
    choice_status_code      = partes[6]
    conversion_value = partes[7] if len(partes) >= 8 else ""
    
    # LOGS
    log_metadata.set({
        "sender_phone": contact_phone,
        "company_id": id_company,
        "lead_id": id_lead,
        "tracking_ai_id": id_tb_tracking,
        "choice": acao_escolhida,
        "choice_status_id": choice_status_id,
        "choice_status_name": choice_status_name,
        "choice_status_code": choice_status_code,
        "conversion_value": conversion_value
    })
    
    # GRAVA TRACKING NO DB
    metadata_to_update = {"choice_status_id": choice_status_id,  "choice_status_name": choice_status_name, "choice_status_code": choice_status_code}
    await update_lead_status_response(conn, id_tb_tracking, choice_status_name, metadata_to_update)
    
    # CASO O GESTOR MANTENHA O STATUS --------------------------------------------------------------------------------------------------
    if acao_escolhida == "KEEP":
        logger.info("Gestor optou por manter o status atual")
        return
    
    # CASO SEJA ALTERAÇÃO -------------------------------------------------------------------------------------------------------------
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

        
        

# ========================================================================================================================================================================
# ROTA PARA RECEBER MENSAGENS (Z-API)
# ========================================================================================================================================================================
@router.post('')
@router.post('/')
async def receive_message(request: Request, conn = Depends(get_db), http_resources=Depends(get_http_resources), _auth = Depends(validate_zapi_token)):
    
    http_session, http_semaphore = http_resources
    
    message_content = await request.body()
    message_content_decoded = message_content.decode('utf-8')
    message_content_json = json.loads(message_content_decoded)
    
    try:
        async with http_semaphore:
            json_response = await processa_msg_retorno(conn, http_session, message_content_json)
            return JSONResponse(content=json_response)
    except Exception as e:
        logger.exception(f"Erro: {e}")
        erro = {"status": "error", "message": f"{e}"}
        return JSONResponse(content=erro)
       
     
