# ========================================================================================================================================================================
# ========================================================================================================================================================================
# WEBHOOK (MAIN)
# ========================================================================================================================================================================
# ========================================================================================================================================================================

# ========================================================================================================================================================================
# IMPORTS
# ========================================================================================================================================================================
import uvicorn, asyncio, os, sys, json, aiohttp
from fastapi import FastAPI, Request, HTTPException, Depends, Query
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager
from pathlib import Path

_app = Path(__file__).resolve().parent
for _sub in ("logging", "db"):
    sys.path.append(str(_app / _sub))
from log_config import logger, log_ip, log_host, log_action, log_metadata
from db_connections_async import conecta_leaper_db_core


# ========================================================================================================================================================================
# VARIÁVEIS DE AMBIENTE
# ========================================================================================================================================================================
SERVER_IP = os.environ.get("SERVER_IP")
SERVER_HOST = os.getenv("SERVER_HOST")
SEMAPHORE_HTTP = os.environ.get("SEMAPHORE_HTTP")


# ========================================================================================================================================================================
# CONFIGURAÇÕES DE LOG
# ========================================================================================================================================================================
log_ip.set(SERVER_IP)
log_host.set(SERVER_HOST)
log_metadata.set({})

    
# ========================================================================================================================================================================
# CONFIGURAÇÕES DE CONEXÃO COM O MONGODB
# ========================================================================================================================================================================
@asynccontextmanager
async def lifespan(app: FastAPI):

    log_action.set("webhook_ai_main")
    
    logger.info("Iniciando cliente Postgres...")
    
    app.conn = None
    app.http_session = None
    app.http_semaphore = asyncio.Semaphore(int(SEMAPHORE_HTTP))
    
    try:
        
        # CONECTA DB LEAPER -------------------------------------------------------------------------------------
        app.conn = await conecta_leaper_db_core(min_size=1, max_size=1)
        if app.conn:
            logger.info("Cliente Postgres iniciado com sucesso")
        else:
            logger.error("Cliente Postgres retornou vazio")
        
        # INICIA SESSÃO HTTP --------------------------------------------------------------------------------------
        app.http_session = aiohttp.ClientSession()
        if (app.http_session is not None) and (app.http_semaphore is not None):
            logger.info("Sessão http iniciada.")
        else:
            logger.error("Tentou mas não conseguiu iniciar sessão http")
        
        yield
        
    except Exception as e:
        logger.error(f"Erro ao iniciar: {e}")
        app.conn = None
        yield
    
    finally:
        if app.conn:
            await app.conn.disconnect()
            logger.info("Conexão com o DB encerrada com sucesso")

        if app.http_session:
            await app.http_session.close()
            logger.info("Sessão http encerrada com sucesso")
            
            
            
# ========================================================================================================================================================================
# INICIA APP
# ========================================================================================================================================================================
app = FastAPI(lifespan=lifespan)
app.router.redirect_slashes = False


# ========================================================================================================================================================================
# MIDDLEWARE DE CONTEXTO DE LOG
# ========================================================================================================================================================================
@app.middleware("http")
async def set_log_context(request: Request, call_next):
    # Remove as barras iniciais/finais e divide o caminho em partes
    path_parts = [segment for segment in request.url.path.strip("/").split("/") if segment]
    context_name = "_".join(path_parts).lower() if path_parts else "webhook_desconhecido"
    
    log_action.set(context_name)
    return await call_next(request)
    

# ========================================================================================================================================================================
# ROTAS
# ========================================================================================================================================================================
from zapi import router as zapi_router
from meta import router as meta_router

app.include_router(zapi_router, prefix="/webhook/zapi")
app.include_router(meta_router, prefix="/webhook/meta")
