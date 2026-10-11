Execute o `docker-compose.yaml` do repositório SocialGuard para iniciar o RabbitMQ, o SocialGuard e os demais serviços auxiliares. Inicie o OpenMock depois que a API SocialGuard estiver disponível em `http://localhost:8084`.

## Consumidor de lotes

O OpenMock consome mensagens de texto simples contendo o `batchId` como UUID (por exemplo, `b9e1aab3-6ff1-401d-8eee-ee19ab04b468`) da fila durável `socialguard.batch.processing`. O UUID recebido é usado nas chamadas à API, sem conversão para identificador numérico. Para cada lote, consulta a API SocialGuard, reivindica o processamento e envia todas as classificações para `PUT /api/v1/batches/{id}/results`. Os resultados são persistidos pelo SocialGuard; o OpenMock não grava uma cópia local.

O classificador simulado escolhe `OK`, `SPAM`, `VIOLENT` ou `SEXUAL` para cada comentário, gera entre 100 e 500 tokens por comentário e escolhe `gpt-4` ou `gpt-3.5-turbo`. Falhas transitórias são tentadas até três vezes; depois, a mensagem é encaminhada à fila `socialguard.batch.failed`.

As configurações podem ser sobrescritas por variáveis de ambiente:

| Variável | Padrão |
| --- | --- |
| `RABBITMQ_HOST` / `RABBITMQ_PORT` | `localhost` / `5672` |
| `RABBITMQ_USERNAME` / `RABBITMQ_PASSWORD` | `aluno` / `aluno` |
| `BATCH_EXCHANGE` / `BATCH_QUEUE` | `socialguard.batch` / `socialguard.batch.processing` |
| `BATCH_ROUTING_KEY` | `batch.process` |
| `BATCH_DLX` / `BATCH_DLQ` | `socialguard.batch.dlx` / `socialguard.batch.failed` |
| `BATCH_DLX_ROUTING_KEY` | `batch.failed` |
| `BATCH_MAX_ATTEMPTS` | `3` |
| `BATCH_RETRY_INITIAL_INTERVAL` / `BATCH_RETRY_MAX_INTERVAL` | `1s` / `5s` |
| `BATCH_RETRY_MULTIPLIER` | `2.0` |
| `SOCIALGUARD_BASE_URL` | `http://localhost:8084` |
| `SOCIALGUARD_CONNECT_TIMEOUT` / `SOCIALGUARD_READ_TIMEOUT` | `2s` / `10s` |
| `INFERENCE_MINIMUM_TOKENS` / `INFERENCE_MAXIMUM_TOKENS` | `100` / `500` |
| `INFERENCE_MODELS` | `gpt-4,gpt-3.5-turbo` |

As métricas permanecem disponíveis em `/actuator/prometheus`; a exportação OTLP de métricas foi desativada, pois o endpoint local configurado respondia `404`. A exportação de traces OTLP continua configurada separadamente.