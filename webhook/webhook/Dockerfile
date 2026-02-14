# ==========================================================
# USA IMAGEM OFICIAL DO PYTHON
# ==========================================================
FROM python:3.13-slim

# ==========================================================
# DEFINE DIRETÓRIO /APP COMO WORKDIR
# ==========================================================
WORKDIR /app

# ==========================================================
# COPIA CONTEÚDO DO HOST PARA O CONTAINER
# ==========================================================
COPY . /app
COPY requirements.txt .

# ==========================================================
# INSTALA DEPENDÊNCIAS DO PY
# ==========================================================
RUN pip install --no-cache-dir -r requirements.txt

# ==========================================================
# ENTRYPOINT PARA DEFINIR LOG COM VARIÁVEIS
# ==========================================================
RUN chmod +x /app/entrypoint_uvicorn.py
ENTRYPOINT ["/app/entrypoint_uvicorn.py"]

# ==========================================================
# COMANDO DO CONTAINER
# ==========================================================
#CMD ["tail", "-f", "/dev/null"]
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "80"]

