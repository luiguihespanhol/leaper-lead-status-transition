# ========================================================================================================================================================================
# ========================================================================================================================================================================
# CONEXÕES COM OS BANCOS DE DADOS
# ========================================================================================================================================================================
# ========================================================================================================================================================================

# ========================================================================================================================================================================
# IMPORTS
# ========================================================================================================================================================================
import os, logging, json
from pathlib import Path 
from dotenv import load_dotenv
from databases import Database


# ========================================================================================================================================================================
# CONFIGURAÇÕES DE AMBIENTE
# ========================================================================================================================================================================
load_dotenv(Path(__file__).resolve().parent.parent / '.env')

LEAPER_DB_HOST = os.getenv("LEAPER_DB_HOST")
LEAPER_DB_USER = os.getenv("LEAPER_DB_USER")
LEAPER_DB_PASSWORD = os.getenv("LEAPER_DB_PASSWORD")
LEAPER_DB_PORT = os.getenv("LEAPER_DB_PORT")
LEAPER_DB_SSL_MODE = os.getenv("LEAPER_DB_SSL_MODE")




# ========================================================================================================================================================================
# CONECTA DB CORE
# ========================================================================================================================================================================
async def conecta_leaper_db_core(min_size, max_size):
    logging.getLogger("databases").setLevel(logging.WARNING)
    logging.getLogger("asyncpg").setLevel(logging.WARNING)
    LEAPER_CORE_URL = (
    f"postgresql+asyncpg://{LEAPER_DB_USER}:{LEAPER_DB_PASSWORD}"
    f"@{LEAPER_DB_HOST}:{LEAPER_DB_PORT}/ms_core_prod_pool"
    f"?sslmode={LEAPER_DB_SSL_MODE}"
    )
    conn = Database(LEAPER_CORE_URL, min_size=min_size, max_size=max_size, timeout=60, command_timeout=120)
    await conn.connect()
    return conn

# ========================================================================================================================================================================
# CONECTA DB GATEWAY
# ========================================================================================================================================================================
async def conecta_leaper_db_gateway(min_size, max_size):
    logging.getLogger("databases").setLevel(logging.WARNING)
    logging.getLogger("asyncpg").setLevel(logging.WARNING)
    LEAPER_GATEWAY_URL = (
    f"postgresql+asyncpg://{LEAPER_DB_USER}:{LEAPER_DB_PASSWORD}"
    f"@{LEAPER_DB_HOST}:{LEAPER_DB_PORT}/ms_gateway_prod_pool"
    f"?sslmode={LEAPER_DB_SSL_MODE}"
    )
    conn = Database(LEAPER_GATEWAY_URL, min_size=min_size, max_size=max_size, timeout=60, command_timeout=120)
    await conn.connect()
    return conn
    
# ========================================================================================================================================================================
# CONECTA DB EVOLUTION
# ========================================================================================================================================================================
async def conecta_leaper_db_evo(evo_db_host, db_name, min_size, max_size):

    logging.getLogger("databases").setLevel(logging.WARNING)
    logging.getLogger("asyncpg").setLevel(logging.WARNING)

    def _load_evo_creds_dict():
        """Lê evo_db_creds.json (dict por host) no mesmo diretório deste arquivo."""
        here = os.path.dirname(__file__)
        path = os.path.join(here, "evo_db_creds.json")
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
        

    # BUSCAR NO ARQUIVO COM DE-PARA DO ev_db_host para user e password
    creds = _load_evo_creds_dict()
    if evo_db_host not in creds:
        raise KeyError(f"Host '{evo_db_host}' não encontrado em evo_db_creds.json")

    evo_db_host_full = f"{evo_db_host}.db.ondigitalocean.com"
    leaper_evo_db_port = 25060
    leaper_evo_db_ssl_mode = "require"
    leaper_evo_db_user = creds[evo_db_host]["user"]
    leaper_evo_db_password = creds[evo_db_host]["password"]
    
    leaper_evo_db_url = (
    f"postgresql+asyncpg://{leaper_evo_db_user}:{leaper_evo_db_password}"
    f"@{evo_db_host_full}:{leaper_evo_db_port}/{db_name}"
    f"?sslmode={leaper_evo_db_ssl_mode}"
    )
    conn = Database(leaper_evo_db_url, min_size=min_size, max_size=max_size, timeout=60, command_timeout=120)
    await conn.connect()
    return conn
    


