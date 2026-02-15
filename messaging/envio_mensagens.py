# ============================================================================================================================================
# IMPORTS
# ============================================================================================================================================
import os, asyncio, aiohttp
from pathlib import Path
import requests
from dotenv import load_dotenv

from log_config import logger

# ========================================================================================================================================================================
# CONFIGURAÇÕES DE AMBIENTE
# ========================================================================================================================================================================
dotenv_path = Path(__file__).resolve().parent.parent / '.env'
load_dotenv(dotenv_path)
ZAPI_URL = os.getenv("ZAPI_URL")
ZAPI_CLIENT_TOKEN = os.getenv("ZAPI_CLIENT_TOKEN")

# META WHATSAPP API
META_WHATSAPP_API_URL = "https://graph.facebook.com/v22.0/1018455111342194/messages"
META_WHATSAPP_API_TOKEN = "EAAMYasuodTgBQrjq0cdq5hLJAjYRrFtkIRPKYuBVcp2AzCdfsGzMDFlkY2vFXEEQJVZBX1p2EbyMcttswOpKnnr0EgsiIeMzLZClwgJvyfoJ0kKi6ohJyQMnZB0VZAeSbXWRyAVxhZAswzJhmtIGSvrfdr9sKa8ZAQSgMZAWwgD6WFFzEkfBjxZCZB8j0jGq9lweGjQZDZD"  # TODO: definir token manualmente



# ================================================================================================================================================================================
# ENVIA MENSAGEM (API OFICIAL META)
# ================================================================================================================================================================================
async def envia_mensagem_com_botao_whatsapp(session, payload, max_tentativas=3):

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {META_WHATSAPP_API_TOKEN}"
    }

    for tentativa in range(1, max_tentativas + 1):
        is_ultima = tentativa == max_tentativas
        log_fn = logger.error if is_ultima else logger.warning

        try:
            async with session.post(
                META_WHATSAPP_API_URL,
                json=payload,
                headers=headers,
                timeout=aiohttp.ClientTimeout(total=30)
            ) as response:
                response_data = await response.json()

                if response.status == 200:
                    messages = response_data.get("messages", [])
                    if messages:
                        logger.info(f"Mensagem enviada com sucesso via Meta API: {response_data}")
                        return response_data
                    else:
                        log_fn(f"[Tentativa {tentativa}/{max_tentativas}] Resposta 200 sem messages da Meta API: {response_data}")
                else:
                    log_fn(f"[Tentativa {tentativa}/{max_tentativas}] Falha ao enviar mensagem via Meta API: status {response.status} - resposta: {response_data}")

        except aiohttp.ClientError as e:
            log_fn(f"[Tentativa {tentativa}/{max_tentativas}] Erro de cliente ao enviar mensagem via Meta API: {e}")
        except Exception as e:
            log_fn(f"[Tentativa {tentativa}/{max_tentativas}] Erro ao enviar mensagem via Meta API: {e}")

        if not is_ultima:
            await asyncio.sleep(tentativa)

    return None