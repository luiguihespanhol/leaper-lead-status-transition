# ========================================================================================================================================================================
# ========================================================================================================================================================================
# FUNÇÃO PARA OBTER O ID DO WORKER A PARTIR DO NOME DO CONTAINER, QUE VEM A PARTIR DO ID
# ========================================================================================================================================================================
# ========================================================================================================================================================================

import docker, re

def obter_infos_container(container_id):
    client = docker.from_env()
    container = client.containers.get(container_id)
    nome_container_completo = container.name

    # Ajustar regex para lidar com hífens e sublinhados corretamente
    match = re.match(r'^.*?-(.*?)-\d+$', nome_container_completo)
    
    if match:
        nome_container = match.group(1)  # Extrai o nome correto
        replica_container = re.search(r'(\d+)$', nome_container_completo).group(1)
        return nome_container, replica_container
    else:
        return None, None