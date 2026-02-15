import os
import sys
import json
import requests
from datetime import datetime


def notifica_erro(**extra_fields):
    ALERTS_URL = os.environ.get("ALERTS_URL")
    ALERTS_AUTH = os.environ.get("ALERTS_AUTH")
    ALERTS_RECIPIENT = os.environ.get("ALERTS_RECIPIENT")

    dt_hr_erro = datetime.now()

    # Monta a mensagem de erro para o alerta
    msg_erro = f"⚠️ {dt_hr_erro}\n"
    for campo, valor in extra_fields.items():
        msg_erro += f"{campo}: {valor}\n"

    # Garante que ALERTS_URL não termine com "/"
    if ALERTS_URL.endswith("/"):
        ALERTS_URL = ALERTS_URL[:-1]

    # Monta endpoint com parâmetro de destinatário
    api_endpoint_completo = f"{ALERTS_URL}?recipient={ALERTS_RECIPIENT}"
    
    headers = {"Authorization": ALERTS_AUTH, "Content-Type": "application/json"}
    payload = {"message": msg_erro}

    try:
        payload_json = json.dumps(payload)
        # Envia a requisição de forma síncrona usando a biblioteca requests
        response = requests.post(api_endpoint_completo, data=payload_json, headers=headers, timeout=30)
        if response.status_code == 200:
            return True
        else:
            return False
    except requests.RequestException as e:
        return False
