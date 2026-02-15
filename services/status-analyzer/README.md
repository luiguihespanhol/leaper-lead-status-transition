# Alterações no Status Analyzer - Lead Status Transition

Este documento descreve as alterações realizadas no `status_analyzer.py` para separar a lógica de análise/agendamento da lógica de envio de mensagens.

## Resumo das Alterações

O serviço `status_analyzer` agora **apenas agenda** as mensagens de confirmação, sem enviá-las diretamente. O envio efetivo é feito pelo novo serviço `send_messages`.

---

## 1. Alteração no Campo de Data

### Antes
- Gravava em `message_sent_date` no momento do envio da mensagem

### Depois
- Grava em `message_schedule_date` no momento do agendamento
- O campo `message_sent_date` é preenchido pelo serviço `send_messages` após envio bem-sucedido

### Arquivos Alterados
- `status_analyzer.py`:
  - Função `insere_registro_ai_tracking`: parâmetro e campo alterados de `message_sent_date` para `message_schedule_date`
  - Query INSERT alterada para usar `message_schedule_date`

---

## 2. Novo Campo: message_status

### Descrição
Novo campo para controlar o status de envio da mensagem.

### Valores Possíveis
- `pending`: mensagem agendada, aguardando envio
- `sent`: mensagem enviada com sucesso
- `ignored`: mensagem ignorada (havia outra mais recente para o mesmo lead, que estava pending)
- `n/a`: análise por keyword sem agendamento de mensagem
- `NULL`: registro sem agendamento de mensagem (análise normal)

### Lógica no status_analyzer
```python
# Define message_status baseado no contexto
if message_schedule_date:
    message_status = 'pending'
elif metadata.get("executor") == "keyword":
    message_status = 'n/a'
else:
    message_status = None
```

### Query INSERT atualizada
```sql
INSERT INTO lead_status_transition (
    ...
    message_schedule_date,
    message_status,
    ...
)
VALUES (
    ...
    :message_schedule_date,
    :message_status,
    ...
)
```

---

## 3. Remoção da Lógica de Envio de Mensagens

### Antes
- Função `enviar_confirmacao_alteracao_status` montava o payload e enviava via WhatsApp

### Depois
- Função renomeada para `agenda_confirmacao_alteracao_status`
- Apenas grava o registro no banco com `message_schedule_date` e `message_status = 'pending'`
- Não monta payload nem envia mensagem
- Removido parâmetro `session` (não precisa mais de HTTP)

### Código Removido
- Import: `from envio_mensagens import envia_mensagem_com_botao_whatsapp`
- Função: `monta_payload_whatsapp` (movida para `send_messages.py`)
- Chamada: `envia_mensagem_com_botao_whatsapp(session, payload)`

---

## 4. Alteração na Nomenclatura de Retornos

### Antes
- Retornava `"confirmation_sent"` e contava `"enviadas"`

### Depois
- Retorna `"confirmation_scheduled"` e conta `"agendadas"`

### Variáveis Alteradas (replace all)
- `enviadas` → `agendadas`
- `enviados` → `agendados`
- `enviados_total` → `agendados_total`
- `"confirmation_sent"` → `"confirmation_scheduled"`
- `"Enviadas:"` → `"Agendados:"` (nos logs)

---

## 5. Novo Serviço: send_messages

Criado em `/lead_status_transition/send_messages/` com a seguinte estrutura:

```
send_messages/
├── docker-compose.yml
└── send_messages/
    ├── Dockerfile
    ├── requirements.txt
    └── send_messages.py
```

### Configurações
- **Intervalo de execução**: 5 minutos (300 segundos)
- **Janela Meta API**: 23h50min (para ter margem antes de fechar 24h)
- **Concorrência**: 4 mensagens simultâneas por company

### Fluxo do send_messages

```
1. Consulta companies ativas com mensagens pendentes (JOIN único)
   │
   ▼
2. Para cada company:
   │
   ├─ Verifica janela de 24h (service_ai_last_response_at)
   │   │
   │   ├─ "nova" (null, vazio, "null", "None") → envia msg_abertura
   │   ├─ "aberta" (< 23h50min) → pode enviar mensagens individuais
   │   └─ "fechada" (>= 23h50min) → envia msg_abertura
   │
   ▼
3. Se janela "nova" ou "fechada":
   │
   ├─ Verifica service_ai_last_opening_msg_sent_at (já enviou hoje?)
   │   ├─ SIM → pula (não envia de novo)
   │   └─ NÃO → Envia msg_abertura (template: pending_approval_summary)
   │              - Botão único com payload: {"company_id": "", "action": "open_24h_window"}
   │              - Após sucesso: grava service_ai_last_opening_msg_sent_at = NOW() no metadata da company
   │
   ▼
4. Se janela "aberta":
   │
   ├─ Consulta mensagens pendentes (apenas mais recente por lead)
   │
   ├─ Para cada mensagem:
   │   ├─ Monta payload WhatsApp (template: lead_status_transition_confirmation)
   │   ├─ Envia via API Meta
   │   └─ Se sucesso:
   │       ├─ Linha enviada: message_status = 'sent', message_sent_date = NOW()
   │       └─ Demais linhas pending do lead: message_status = 'ignored'
   │
   ▼
5. Aguarda 5 minutos e repete
```

### Funções Principais

| Função | Descrição |
|--------|-----------|
| `consulta_companies_com_pendentes` | Query única com JOIN para buscar companies ativas com pendentes |
| `consulta_mensagens_pendentes_por_company` | Retorna apenas a mais recente por lead (ROW_NUMBER) |
| `atualiza_mensagem_enviada` | Marca como 'sent' a enviada e 'ignored' as demais (que estavam pending) |
| `monta_payload_whatsapp` | Monta payload de mensagem individual de confirmação |
| `monta_payload_msg_abertura` | Monta payload de abertura de janela (template: pending_approval_summary) |
| `verifica_janela_aberta` | Retorna "nova", "aberta" ou "fechada" baseado em service_ai_last_response_at |
| `atualiza_opening_msg_sent_at` | Grava `service_ai_last_opening_msg_sent_at` no metadata da company após envio da msg_abertura |

---

## 6. Fluxo Atualizado Completo

```
[status_analyzer] (a cada 5 min)
      │
      ▼
  Analisa lead com AI
      │
      ▼
  Precisa confirmação?
      │
      ├─ NÃO → auto_update ou keep_same_status
      │         - Se keyword: message_status = 'n/a'
      │         - Senão: message_status = NULL
      │
      └─ SIM → Grava registro com:
               - message_schedule_date = NOW()
               - message_status = 'pending'
                      │
                      ▼
              [send_messages] (a cada 5 min)
                      │
                      ▼
              Consulta companies com pendentes
                      │
                      ▼
              Verifica janela 24h (service_ai_last_response_at)
                      │
                      ├─ "nova" ou "fechada" → Envia msg_abertura
                      │                        (aguarda resposta do webhook)
                      │
                      └─ "aberta" → Para cada lead (mais recente pending):
                                        │
                                        ▼
                                  Monta payload confirmação
                                        │
                                        ▼
                                  Envia WhatsApp
                                        │
                                        ▼
                                  Atualiza status:
                                  - Enviada: 'sent'
                                  - Demais pending do lead: 'ignored'
```

---

## 7. Campos da Tabela lead_status_transition

| Campo | Preenchido por | Descrição |
|-------|---------------|-----------|
| `message_schedule_date` | status_analyzer | Data/hora do agendamento |
| `message_status` | status_analyzer → send_messages | Status: pending/sent/ignored/n/a/NULL |
| `message_sent_date` | send_messages | Data/hora do envio efetivo |

### Detalhes do message_status

| Valor | Definido por | Quando |
|-------|--------------|--------|
| `pending` | status_analyzer | Quando há agendamento de mensagem |
| `sent` | send_messages | Após envio bem-sucedido da mensagem |
| `ignored` | send_messages | Outras mensagens pending do mesmo lead após envio |
| `n/a` | status_analyzer | Análise por keyword sem agendamento |
| `NULL` | status_analyzer | Análise normal sem agendamento |

---

## 8. Campo lead_lid no metadata_tracking

### Descrição
O campo `lead_lid` foi adicionado ao `metadata_tracking` em todos os 3 pontos de fluxo do status_analyzer. Contém o `lid` (identificador legível) do lead.

### Extração
```python
lid = lead_info.get("lid", "")
```

### Onde é gravado (nos 3 fluxos)
1. **Fluxo AI** (`processa_ai`): `metadata_tracking["lead_lid"] = lid`
2. **Fluxo Keywords** (`processa_keywords`): `metadata_tracking["lead_lid"] = lid`
3. **Lead sem mensagens**: `metadata_tracking["lead_lid"] = lid`

### Uso no send_messages
O `lead_lid` é extraído do metadata para identificar o lead no body da mensagem quando não há nome nem telefone:
```python
lead_lid = metadata.get("lead_lid", "")
# Fallback: O lead ({lead_lid}) recebeu uma sugestão...
```

---

## 9. Checklist para Aplicar no lead_status_ai

### Alterações no INSERT
- [ ] Adicionar campo `message_status` na query INSERT
- [ ] Implementar lógica: `pending` se agendamento, `n/a` se keyword, `None` senão
- [ ] Alterar parâmetro `message_sent_date` → `message_schedule_date`

### Alterações na Função de Agendamento
- [ ] Renomear função de envio para agendamento
- [ ] Remover import `envia_mensagem_com_botao_whatsapp`
- [ ] Remover função `monta_payload_whatsapp`
- [ ] Remover chamada de envio WhatsApp

### Alterações nos Retornos
- [ ] Alterar retornos de `confirmation_sent` → `confirmation_scheduled`
- [ ] Alterar variáveis `enviadas` → `agendadas`
- [ ] Atualizar logs de "enviando" → "agendando"

### Campo lead_lid no metadata_tracking
- [ ] Extrair `lid` do lead_info: `lid = lead_info.get("lid", "")`
- [ ] Adicionar `"lead_lid": lid` no metadata_tracking de todos os fluxos (AI, keywords, lead sem mensagens)

### Formato da mensagem de confirmação (interactive buttons)
- [ ] Mensagem de confirmação agora é interactive buttons (NÃO template)
- [ ] Botão 1: KEEP|inserted_id (status atual), Botão 2: CHANGE|inserted_id (sugerido), Botão 3: REVERSED|inserted_id (apenas END_WON/END_LOST)
- [ ] Body com identificação do lead: nome > telefone > lid (fallback)
- [ ] STATUS_EMOJIS no título dos botões, em MAIÚSCULA, truncado em 20 chars
- [ ] Sem header

### Serviço send_messages
- [ ] Criar serviço send_messages correspondente (ou reutilizar o existente)
- [ ] Configurar para processar mensagens do lead_status_ai

---

## 10. Webhook - Alterações

### Arquivo: `/lead_status_transition/webhook/webhook/meta.py`

O webhook agora suporta 3 cenários de clique de botão:

#### Cenário 1: Interactive buttons (novo formato - msgs de confirmação)
- `type = "interactive"`, `interactive.type = "button_reply"`
- `button_reply.id` = `"ACTION|inserted_id"` (ex: `"KEEP|123"`, `"CHANGE|456"`, `"REVERSED|789"`)
- Parseia o `id` com `split("|")` para extrair action e inserted_id

#### Cenário 2: Template buttons (formato antigo - compatibilidade)
- `type = "button"`, `button.payload` = JSON com `action` e `inserted_id`
- Mantido para botões já enviados antes da mudança

#### Cenário 3: Msg abertura (template - open_24h_window)
- `type = "button"`, `button.payload` = JSON com `{"company_id": "...", "action": "open_24h_window"}`
- Atualiza `service_ai_last_response_at = NOW()` no metadata da company
- Não faz mais nada (não há lead/tracking envolvido)

### Atualização de service_ai_last_response_at
Em **todo clique de botão** (confirmação ou abertura), atualiza `service_ai_last_response_at = NOW()` no metadata da company para renovar a janela de 24h.

### Nova função: `atualiza_service_ai_last_response_at(conn, company_id)`
```sql
UPDATE company
SET metadata = jsonb_set(
    COALESCE(metadata, '{}'::jsonb),
    '{service_ai_last_response_at}',
    to_jsonb(to_char(NOW(), 'YYYY-MM-DD"T"HH24:MI:SS.US'))
),
updated_at = NOW()
WHERE id = :company_id
```

---

## 11. TODOs Pendentes

1. **Migração de dados**: Script para atualizar registros existentes com `message_status`

---

## 12. Observações Importantes

1. A query que consulta leads disponíveis ainda usa `message_sent_date` para calcular o intervalo de reprocessamento - isso está correto pois precisa saber se a mensagem foi de fato enviada.

2. A constante `STATUS_EMOJIS` foi mantida no `status_analyzer.py` mas não é mais usada nele. Pode ser removida se desejado.

3. O novo serviço `send_messages` é independente e pode ser escalado separadamente.

4. A lógica de "mais recente por lead" usa `ROW_NUMBER() OVER (PARTITION BY lead_id ORDER BY message_schedule_date DESC)` para garantir que apenas uma mensagem seja enviada por lead.

5. A marcação como `ignored` só afeta mensagens que estavam com `message_status = 'pending'`.

6. A verificação de janela (`verifica_janela_aberta`) trata como "nova" quando `service_ai_last_response_at` é: `None`, string vazia `""`, `"null"`, ou `"None"`.

---

## 13. Payloads WhatsApp

### Payload msg_abertura (janela nova/fechada)
```python
template_name = "pending_approval_summary"

# Body com 1 parâmetro (qtd de leads pendentes) + 1 botão
payload_open_window = json.dumps({
    "company_id": str(company_id),
    "action": "open_24h_window"
})

components = [
    {
        "type": "body",
        "parameters": [
            {"type": "text", "parameter_name": "leads", "text": qtd_leads_formatado}  # {{leads}} no template
        ]
    },
    {
        "type": "button",
        "sub_type": "quick_reply",
        "index": 0,
        "parameters": [{"type": "payload", "payload": payload_open_window}]
    }
]
```

### Payload confirmação individual (janela aberta) - Interactive Buttons
```python
# NÃO é template - é mensagem interativa com botões
# Botão 1: status atual (KEEP)
# Botão 2: status sugerido (CHANGE)
# Botão 3: reversed do sugerido (REVERSED) - apenas quando END_WON/END_LOST

# ID dos botões: ACTION|inserted_id (ex: "KEEP|123", "CHANGE|123", "REVERSED|123")
# Títulos: EMOJI + STATUS NAME em MAIÚSCULA (truncado em 20 chars)

# Body text:
# - Com nome: *Nome* (telefone) recebeu uma sugestão de alteração de status, de *emoji STATUS* para *emoji STATUS*.
# - Sem nome, com tel: O lead (telefone) recebeu...
# - Sem nome, sem tel, com lid: O lead (lid) recebeu...

# Sem header

STATUS_EMOJIS = {
    "LEAD_START": "⏳",
    "CONTATO_INICIADO": "▶️",
    "QUALIFICACAO": "📝",
    "NEGOCIACAO": "💱",
    "PROPOSTA_ENVIADA": "➡️",
    "END_WON": "✅",
    "END_LOST": "❌"
}

buttons = [
    {"type": "reply", "reply": {"id": f"KEEP|{inserted_id}", "title": f"{pre_emoji} {pre_status_name.upper()}"[:20]}},
    {"type": "reply", "reply": {"id": f"CHANGE|{inserted_id}", "title": f"{ai_emoji} {ai_status_name.upper()}"[:20]}}
]

# 3o botão somente para END_WON/END_LOST (reversed: END_WON↔END_LOST)
if ai_suggestion_status_code in ("END_WON", "END_LOST"):
    buttons.append({"type": "reply", "reply": {"id": f"REVERSED|{inserted_id}", "title": f"{reversed_emoji} {reversed_name.upper()}"[:20]}})

payload = {
    "messaging_product": "whatsapp",
    "recipient_type": "individual",
    "to": str(tel_resp_company),
    "type": "interactive",
    "interactive": {
        "type": "button",
        "body": {"text": body_text},
        "action": {"buttons": buttons}
    }
}
```
