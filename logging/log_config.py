# =================================================================================================
# CONFIGURAÇÕES DE LOGGER
# =================================================================================================

import logging
import os
from contextvars import ContextVar
from datetime import date, datetime, time
from pathlib import Path

# ---------------------------------------------------------------------------------
# Carrega .env relativo a este arquivo: ../.env (raiz do projeto)
# /home/leaper_dev/app_leaper_py/shared/log_config.py --> /home/leaper_dev/app_leaper_py/.env
# ---------------------------------------------------------------------------------
def _load_env_file_relative():
    # se já vieram do docker-compose, não tenta carregar arquivo
    if os.getenv("ALERTS_URL") and os.getenv("ALERTS_AUTH"):
        return

    env_path = (Path(__file__).resolve().parent.parent / ".env")
    if not env_path.exists():
        return
    try:
        with env_path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip('"').strip("'")
                os.environ.setdefault(k, v)  # não sobrescreve valores já definidos no ambiente
    except Exception:
        # silencioso: não quebra o app se .env tiver problemas
        pass


_load_env_file_relative()

# ---------------------------------------------------------------------------------
# Contexto
# ---------------------------------------------------------------------------------
log_ip = ContextVar("LOG_IP", default="")
log_host = ContextVar("LOG_HOST", default="")
log_action = ContextVar("LOG_ACTION", default="")
log_metadata = ContextVar("LOG_METADATA", default={})

class TaskIdFilter(logging.Filter):
    def filter(self, record):
        record.ip = log_ip.get("")
        record.host = log_host.get("")
        record.action = log_action.get("")
        metadata = log_metadata.get({})
        if not isinstance(metadata, dict):
            metadata = {}
        metadata = metadata.copy()
        metadata["message"] = record.getMessage()
        record.metadata = metadata
        return True

logging.basicConfig(
    format="%(asctime)s | %(ip)s | %(host)s | %(action)s | %(levelname)s | %(metadata)s",
    level=logging.INFO,
    force=True,
)

logger = logging.getLogger(__name__)
logger.addFilter(TaskIdFilter())

root_logger = logging.getLogger()
for handler in root_logger.handlers:
    handler.addFilter(TaskIdFilter())


def _json_safe(value):
    """
    Converte valores potencialmente não serializáveis em tipos básicos
    para evitar falhas de serialização ao enviar alertas.
    """
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    if isinstance(value, dict):
        return {k: _json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe(v) for v in value]
    try:
        return str(value)
    except Exception:
        return repr(value)

# =================================================================================================
# ENVIO AUTOMÁTICO DE LOGS ERROR PARA WHATSAPP
# =================================================================================================
import requests
from urllib.parse import urlencode
from datetime import datetime

ALERTS_URL = os.getenv("ALERTS_URL", "https://monitoring.leaper.com.br:3033/webhook/alerts")
ALERTS_AUTH = os.getenv("ALERTS_AUTH", "")
ALERTS_RECIPIENT = os.getenv("ALERTS_RECIPIENT", "")

# Monta URL final com recipient como querystring
_query = urlencode({"recipient": ALERTS_RECIPIENT}) if ALERTS_RECIPIENT else ""
WHATSAPP_WEBHOOK_URL = f"{ALERTS_URL}?{_query}" if _query else ALERTS_URL

def send_whatsapp_alert(payload: dict):
    """
    Envia sempre JSON (dict).
    """
    if not WHATSAPP_WEBHOOK_URL or not ALERTS_AUTH:
        return

    headers = {
        "Authorization": ALERTS_AUTH,
        "Content-Type": "application/json",
    }
    try:
        resp = requests.post(
            WHATSAPP_WEBHOOK_URL, headers=headers, json=payload, timeout=5
        )
        resp.raise_for_status()
    except Exception:
        # não reloga para evitar loop/recursão de logger
        pass

class WhatsAppAlertHandler(logging.Handler):
    """
    Handler que envia logs de nível >= ERROR para o WhatsApp em formato JSON.
    """
    def __init__(self, level=logging.ERROR):
        super().__init__(level=level)

    def emit(self, record: logging.LogRecord) -> None:
        if record.levelno >= logging.ERROR:
            try:
                metadata = getattr(record, "metadata", {})
                metadata = _json_safe(metadata)
                payload = {
                    "timestamp": datetime.utcnow().isoformat(),
                    "ip": getattr(record, "ip", ""),
                    "host": getattr(record, "host", ""),
                    "action": getattr(record, "action", ""),
                    "level": record.levelname,
                    "message": _json_safe(record.getMessage()),
                    "metadata": metadata,
                }
                send_whatsapp_alert(payload)
            except Exception:
                self.handleError(record)

# Evita duplicar handler em reloads/imports múltiplos
_has_wa = any(isinstance(h, WhatsAppAlertHandler) for h in root_logger.handlers)
if not _has_wa:
    wa_handler = WhatsAppAlertHandler(level=logging.ERROR)
    root_logger.addHandler(wa_handler)
