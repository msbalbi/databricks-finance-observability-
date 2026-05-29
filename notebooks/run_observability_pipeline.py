import requests
import json
import time
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from datetime import datetime

# -------------------------------------------------------------------------
# 1. CONFIGURAÇÕES INICIAIS
# -------------------------------------------------------------------------
TEAMS_WEBHOOK_URL = "https://your-teams-webhook-url-here"
TABELA_AUDITORIA_ERROS = "hive_metastore.finance_governance.log_observabilidade_erros"

# Parâmetros de contexto extraídos nativamente do Job
job_name = dbutils.widgets.get("job_name")
run_id = dbutils.widgets.get("run_id")
workspace_url = "https://<sua-instancia-databricks>.cloud.databricks.com"

spark = SparkSession.builder.getOrCreate()

# Início do cronômetro para medir a performance técnica do Spark (Substituindo APM)
tempo_inicio = time.time()

# -------------------------------------------------------------------------
# 2. FUNÇÃO CENTRAL: GRAVAÇÃO HISTÓRICA NA TABELA DELTA
# -------------------------------------------------------------------------
def registrar_na_base_delta(status_execucao, tipo_incidente, mensagem_detalhada, duracao_segundos):
    """
    Centraliza o histórico técnico e de qualidade na tabela Delta.
    Substitui a necessidade de gráficos do Datadog para análise de performance.
    """
    try:
        dados_log = [{
            "timestamp_evento": datetime.now(),
            "job_name": job_name,
            "run_id": run_id,
            "squad": "Finanças",
            "status_execucao": status_execucao,      # SUCCESS ou FAILED
            "tipo_incidente": tipo_incidente,        # N/A, Schema Drift, Volumetria, Contrato, Infraestrutura
            "mensagem": mensagem_detalhada,
            "duracao_segundos": float(duracao_segundos), # Tempo de execução capturado
            "link_databricks": f"{workspace_url}/#job/jobs/runs/view?runId={run_id}"
        }]
        
        df_log = spark.createDataFrame(dados_log)
        
        # Salva na tabela Delta de governança (Append para acumular o histórico)
        df_log.write \
            .format("delta") \
            .mode("append") \
            .option("mergeSchema", "true") \
            .saveAsTable(TABELA_AUDITORIA_ERROS)
            
    except Exception as e:
        # Fallback caso a própria gravação Delta falhe (evita loop de erro)
        print(f"Erro crítico ao gravar na tabela Delta de auditoria: {str(e)}")

# -------------------------------------------------------------------------
# 3. FUNÇÃO: DISPARO DE ALERTAS EXCLUSIVOS PARA O TEAMS
# -------------------------------------------------------------------------
def enviar_alerta_teams(titulo, mensagem, tipo_alerta="Attention", categoria="Erro"):
    """Envia um card visual detalhado direto para o canal da Squad de Engenharia"""
    fatos_base = [
        {"title": "Squad:", "value": "Finanças"},
        {"title": "Categoria do Incidente:", "value": categoria},
        {"title": "Linhagem de Dados:", "value": "Mapeada via Unity Catalog 🌐"},
        {"title": "Run ID:", "value": run_id}
    ]

    teams_card = {
        "type": "message",
        "attachments": [{
            "contentType": "application/vnd.microsoft.card.adaptive",
            "content": {
                "type": "AdaptiveCard",
                "body": [
                    {"type": "TextBlock", "size": "Large", "weight": "Bolder", "text": titulo, "color": tipo_alerta},
                    {"type": "TextBlock", "text": mensagem, "wrap": True},
                    {"type": "FactSet", "facts": fatos_base}
                ],
                "actions": [{
                    "type": "Action.OpenUrl",
                    "title": "Abrir Execução no Databricks",
                    "url": f"{workspace_url}/#job/jobs/runs/view?runId={run_id}"
                }],
                "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                "version": "1.2"
            }
        }]
    }
    
    requests.post(TEAMS_WEBHOOK_URL, data=json.dumps(teams_card), headers={"Content-Type": "application/json"})

# -------------------------------------------------------------------------
# 4. EXECUÇÃO DO PIPELINE COM AS VALIDAÇÕES DOS 5 PILARES
# -------------------------------------------------------------------------
try:
    # [PILAR 1 - LINHAGEM] Nativamente controlada pelo Unity Catalog no momento da leitura abaixo
    df_atual = spark.read.table("hive_metastore.silver.transacoes_financeiras")

    # [PILAR 3 - SCHEMA DRIFT] Validação ativa de estrutura de colunas
    schema_esperado = {"id_transacao": "string", "valor_transacao": "double"}
    for coluna, tipo in schema_esperado.items():
        tipo_atual = dict(df_atual.dtypes).get(coluna)
        if tipo_atual != tipo:
            msg = f"Schema Drift! A coluna '{coluna}' era esperada como {tipo}, mas retornou {tipo_atual}."
            
            duracao = time.time() - tempo_inicio
            registrar_na_base_delta("FAILED", "Schema Drift", msg, duracao)
            enviar_alerta_teams("⚠️ ALTERAÇÃO DE ESQUEMA (SCHEMA DRIFT)", msg, "Warning", "Schema Drift")
            raise Exception(msg)

    # [PILAR 2 - VOLUMETRIA] Validação ativa de comportamento do volume de dados
    contagem_atual = df_atual.count()
    if contagem_atual < 1000:  # Exemplo de threshold mínimo aceitável
        msg = f"Volume de dados anomalamente baixo detectado. Apenas {contagem_atual} registros processados hoje."
        
        duracao = time.time() - tempo_inicio
        registrar_na_base_delta("FAILED", "Volumetria", msg, duracao)
        enviar_alerta_teams("📊 ANOMALIA DE VOLUMETRIA", msg, "Warning", "Volumetria")
        raise ValueError(msg)

    # [PILAR 5 - CONTRATOS DE DADOS] Validação de regras vitais de negócio financeiro
    valor_total = df_atual.select(F.sum("valor_transacao")).collect()[0][0] or 0
    if valor_total <= 0:
        msg = f"Contrato de dados violado. O valor financeiro consolidado resultou zerado ou negativo (R$ {valor_total})."
        
        duracao = time.time() - tempo_inicio
        registrar_na_base_delta("FAILED", "Data Contract", msg, duracao)
        enviar_alerta_teams("❌ QUEBRA DE CONTRATO DE DADOS", msg, "Attention", "Data Contract")
        raise ValueError(msg)

    # --- PERSISTÊNCIA FINAL NA CAMADA GOLD ---
    # Se passou em tudo, salva na Gold e computa o log de SUCESSO com o tempo total gasto
    # df_atual.write.mode("overwrite").saveAsTable("hive_metastore.gold.fato_transacoes")
    
    duracao_total = time.time() - tempo_inicio
    registrar_na_base_delta("SUCCESS", "N/A", "Pipeline executado com sucesso e critérios de qualidade atendidos.", duracao_total)

except Exception as e:
    # Captura de falhas de infraestrutura / Sintaxe Spark que fogem das validações acima
    if not any(x in str(e) for x in ["Schema Drift", "Volume de dados", "Contrato de dados"]):
        duracao_falha = time.time() - tempo_inicio
        msg_falha = f"O Job falhou por um erro técnico crítico de execução/infraestrutura: {str(e)}"
        
        registrar_na_base_delta("FAILED", "Erro de Infraestrutura", msg_falha, duracao_falha)
        enviar_alerta_teams("🚨 FALHA CRÍTICA NO PIPELINE", msg_falha, "Attention", "Erro de Infraestrutura / Spark")
        raise e
