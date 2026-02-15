# ============================================================================================================================================
# HEALTHCHECK DIÁRIO - RELATÓRIO DE LEADS SEM MENSAGENS POR COMPANY
# ============================================================================================================================================
import os, sys, asyncio
from pathlib import Path
from datetime import datetime, timezone

from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

_root = Path(__file__).resolve().parent.parent
for _sub in ("logging", "db"):
    sys.path.append(str(_root / _sub))
from log_config import logger, log_ip, log_host, log_action, log_metadata, send_whatsapp_alert
from db_connections_async import conecta_leaper_db_core


# ========================================================================================================================================================================
# CONFIGURAÇÕES DE AMBIENTE
# ========================================================================================================================================================================
SERVER_IP = os.getenv("SERVER_IP")
SERVER_HOST = os.getenv("SERVER_HOST")


# ========================================================================================================================================================================
# CONSULTA LEADS SEM MENSAGENS POR COMPANY (DATA DE HOJE)
# ========================================================================================================================================================================
async def consulta_leads_sem_mensagens(db_core):
    """
    Busca na lead_status_transition os registros do dia onde pre_status IS NULL e sugested_status_ai IS NULL
    (leads sem mensagens). Agrupa por company_id e conta leads distintos.
    """
    try:
        query = """
            SELECT
                lst.company_id,
                c.name AS company_name,
                COUNT(DISTINCT lst.lead_id) AS qtd_leads
            FROM lead_status_transition lst
            LEFT JOIN company c ON c.id = lst.company_id
            WHERE lst.execution_date::date = CURRENT_DATE
              AND lst.pre_status IS NULL
              AND lst.sugested_status_ai IS NULL
            GROUP BY lst.company_id, c.name
            ORDER BY qtd_leads DESC
        """
        async with db_core.connection() as conn:
            rows = await asyncio.wait_for(
                conn.fetch_all(query=query),
                timeout=30
            )
        return [dict(row._mapping) for row in rows]
    except Exception as e:
        logger.error(f"Erro ao consultar leads sem mensagens: {e}")
        return []


# ========================================================================================================================================================================
# CONSULTA TOTAIS DO DIA
# ========================================================================================================================================================================
async def consulta_totais_dia(db_core):
    """
    Busca totais gerais do dia: total de execuções, total de leads distintos, total sem mensagens.
    """
    try:
        query = """
            SELECT
                COUNT(*) AS total_execucoes,
                COUNT(DISTINCT lead_id) AS total_leads,
                COUNT(DISTINCT CASE WHEN pre_status IS NULL AND sugested_status_ai IS NULL THEN lead_id END) AS total_leads_sem_mensagens
            FROM lead_status_transition
            WHERE execution_date::date = CURRENT_DATE
        """
        async with db_core.connection() as conn:
            row = await asyncio.wait_for(
                conn.fetch_one(query=query),
                timeout=30
            )
        return dict(row._mapping) if row else {}
    except Exception as e:
        logger.error(f"Erro ao consultar totais do dia: {e}")
        return {}


# ========================================================================================================================================================================
# GERA RELATÓRIO
# ========================================================================================================================================================================
async def gera_relatorio(db_core):
    """Gera e loga o relatório diário de healthcheck e envia via WhatsApp."""

    totais = await consulta_totais_dia(db_core)
    leads_sem_msg = await consulta_leads_sem_mensagens(db_core)

    data_hoje = datetime.now().strftime("%Y-%m-%d")

    # Monta mensagem única para envio
    linhas = [f"\n*REPORT MENSAGENS/LEAD - {data_hoje}*", ""]

    if totais:
        linhas.append(f"Total de execuções: {totais.get('total_execucoes', 0)}")
        linhas.append(f"Leads distintos processados: {totais.get('total_leads', 0)}")
        linhas.append(f"Leads sem mensagens: {totais.get('total_leads_sem_mensagens', 0)}")
    else:
        linhas.append("Não foi possível obter totais do dia.")

    linhas.append("")
    linhas.append("*Por company - leads sem mensagens:*")

    if not leads_sem_msg:
        linhas.append("Nenhum lead sem mensagens hoje.")
    else:
        for row in leads_sem_msg:
            company_name = row.get("company_name") or "N/A"
            qtd = row.get("qtd_leads", 0)
            linhas.append(f"• {company_name}: {qtd} lead(s)")

        total = sum(r.get("qtd_leads", 0) for r in leads_sem_msg)
        linhas.append(f"\n*TOTAL: {total} lead(s) em {len(leads_sem_msg)} company(s)*")

    mensagem = "\n".join(linhas)

    # Loga no logger
    logger.info(mensagem)

    # Envia via WhatsApp (mesmo mecanismo do log_config)
    send_whatsapp_alert({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ip": SERVER_IP or "",
        "host": SERVER_HOST or "",
        "action": "healthcheck_diario",
        "level": "INFO",
        "message": mensagem,
    })


# ========================================================================================================================================================================
# MAIN
# ========================================================================================================================================================================
async def main():
    log_ip.set(SERVER_IP or "")
    log_host.set(SERVER_HOST or "")
    log_action.set("healthcheck_diario")
    logger.info("Executando healthcheck diário...")

    db_core = await conecta_leaper_db_core(min_size=1, max_size=2)

    try:
        await gera_relatorio(db_core)
    finally:
        await db_core.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
