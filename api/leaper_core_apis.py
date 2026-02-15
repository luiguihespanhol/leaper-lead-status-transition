# ========================================================================================================================================================================
# ========================================================================================================================================================================
# AUTENTICA USUÁRIO DE SERVIÇO NA APLICAÇÃO DA LEAPER
# ========================================================================================================================================================================
# ========================================================================================================================================================================


# ============================================================================================================================================
# IMPORTS
# ============================================================================================================================================
import os
from pathlib import Path
from dotenv import load_dotenv
from aiohttp import ClientResponseError, ClientError

from log_config import logger


# ========================================================================================================================================================================
# CONFIGURAÇÕES DE AMBIENTE
# ========================================================================================================================================================================
dotenv_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path)
USER_AI_SERVICE_LEAPER = os.environ.get("USER_AI_SERVICE_LEAPER")
PASSWORD_AI_SERVICE_LEAPER = os.environ.get("PASSWORD_AI_SERVICE_LEAPER")
LEAPER_API_URL_BASE = os.getenv("LEAPER_API_URL_BASE")


# ================================================================================================================================================================================
# AUTENTICA USUÁRIO DE SERVIÇO
# ================================================================================================================================================================================
async def auth_service_user_leaper(session, user, password, max_retries: int = 3):
    url = f"{LEAPER_API_URL_BASE}/auth/login"
    payload = {"email": user, "password": password}

    last_error = ""
    for attempt in range(1, max_retries + 1):
        try:
            async with session.post(url, json=payload, timeout=10) as resp:
                body = await resp.json()
                token = body.get("accessToken")

                if not token:
                    last_error = body.get("error") or body.get("message") or "accessToken não encontrado"
                else:
                    return {"status": "success", "accessToken": token}

        except ClientResponseError as http_err:
            last_error = str(http_err)
        except ClientError as req_err:
            last_error = str(req_err)
        except Exception as e:
            last_error = str(e)

        logger.warning(f"auth_service_user_leaper tentativa {attempt}/{max_retries} falhou: {last_error}")

    return {"status": "error", "accessToken": "", "message": last_error or "Falha ao autenticar usuário de serviço"}


# ================================================================================================================================================================================
# AUTENTICA USUÁRIO NA COMPANY
# ================================================================================================================================================================================
async def auth_company_leaper(session, auth_token_user_leaper, id_company, max_retries: int = 3):
   
    url = f"{LEAPER_API_URL_BASE}/api/company-login"
    headers = {"Authorization": auth_token_user_leaper}
    payload = {"companyId": id_company}

    last_error = ""
    for attempt in range(1, max_retries + 1):
        try:
            async with session.post(url, headers=headers, json=payload, timeout=10) as resp:
                body = await resp.json()
                token = body.get("accessToken")
        
                if not token:
                    last_error = body.get("error") or body.get("message") or "accessToken não encontrado"
                else:
                    return {"status": "success", "accessToken": token}

        except ClientResponseError as http_err:
            last_error = str(http_err)
        except ClientError as req_err:
            last_error = str(req_err)
        except Exception as e:
            last_error = str(e)

        logger.warning(f"auth_company_leaper tentativa {attempt}/{max_retries} falhou: {last_error}")

    return {"status": "error", "accessToken": "", "message": last_error or "Falha ao autenticar company"}
        
        


# ====================================================================================================
# FUNÇÃO UTILITÁRIA PARA OBTER O TOKEN APP LEAPER (service-user + company)
# ====================================================================================================
async def get_auth_token_company_leaper(session, id_company):

    try:
        # AUTENTICA USUÁRIO
        auth_service_response = await auth_service_user_leaper(session, USER_AI_SERVICE_LEAPER, PASSWORD_AI_SERVICE_LEAPER)
        auth_token_user_leaper = auth_service_response.get("accessToken", "")

        if not auth_token_user_leaper:
            logger.error("Falha na autenticação do usuário de serviço (Leaper)")
            return None

        # AUTENTICA NA COMPANY
        auth_company_response = await auth_company_leaper(session, auth_token_user_leaper, id_company)
        auth_token_app_leaper = auth_company_response.get("accessToken", "")

        if not auth_token_app_leaper:
            logger.error("Falha na autenticação da company (Leaper)")
            return None

        return auth_token_app_leaper

    except Exception as e:
        logger.error(f"Erro em get_auth_token_company_leaper: {e}", exc_info=True)
        return None
        
        
# ========================================================================================================================================================================
# ALTERA STATUS LEAD
# ========================================================================================================================================================================
async def change_lead_status(session, auth_token_app_leaper, id_lead, id_status, max_retries: int = 3):
    
    url = f"{LEAPER_API_URL_BASE}/api/v1/lead-status/{id_lead}"
    headers = {"Authorization": auth_token_app_leaper}
    payload = {"status_id": id_status}

    for attempt in range(1, max_retries + 1):
        try:
            async with session.put(url, headers=headers, json=payload, timeout=10) as resp:
                texto = await resp.text()
                if resp.status // 100 == 2:
                    return True

                error_msg = texto or f"HTTP {resp.status}"
                logger.error(f"change_lead_status tentativa {attempt}/{max_retries} falhou: {error_msg}")

        except ClientError as e:
            logger.error(f"change_lead_status tentativa {attempt}/{max_retries} falhou: {e}")

        if attempt == max_retries:
            return False

    return False


# ========================================================================================================================================================================
# ENVIA VALOR DE CONVERSÃO DA 'COMPRA' DO LEAD
# ========================================================================================================================================================================
async def send_lead_conversion_value(session, auth_token_app_leaper, id_lead, conversion_value, max_retries: int = 3):
    
    url = f"{LEAPER_API_URL_BASE}/api/v1/lead/{id_lead}"
    headers = {"Authorization": auth_token_app_leaper}
    payload = {"lead_value": conversion_value}

    for attempt in range(1, max_retries + 1):
        try:
            async with session.put(url, headers=headers, json=payload, timeout=10) as resp:
                texto = await resp.text()
                if resp.status // 100 == 2:
                    return True

                error_msg = texto or f"HTTP {resp.status}"
                logger.error(f"send_lead_conversion_value tentativa {attempt}/{max_retries} falhou: {error_msg}")

        except ClientError as e:
            logger.error(f"send_lead_conversion_value tentativa {attempt}/{max_retries} falhou: {e}")

        if attempt == max_retries:
            return False

    return False
        


        
