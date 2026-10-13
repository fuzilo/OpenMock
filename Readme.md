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
| `INFERENCE_SIMULATED_FAILURE_RATE` / `INFERENCE_SIMULATED_LATENCY` | `0.0` / `0s` |
| `INFERENCE_CB_FAILURE_RATE_THRESHOLD` | `50` |
| `INFERENCE_CB_SLIDING_WINDOW_SIZE` / `INFERENCE_CB_MINIMUM_NUMBER_OF_CALLS` | `10` / `5` |
| `INFERENCE_CB_WAIT_DURATION_IN_OPEN_STATE` | `30s` |
| `INFERENCE_CB_PERMITTED_CALLS_IN_HALF_OPEN_STATE` | `2` |
| `INFERENCE_CB_SLOW_CALL_DURATION_THRESHOLD` / `INFERENCE_CB_SLOW_CALL_RATE_THRESHOLD` | `30s` / `50` |

## Circuit breaker da inferência

A inferência é chamada pela interface `InferenceClient`, hoje implementada pelo simulador, e passa pelo circuit breaker `openai` (Resilience4j). Falhas de `InferenceUnavailableException` e chamadas lentas contam para abrir o circuito; `PermanentBatchProcessingException` não conta.

Com o circuito aberto, o consumidor devolve a mensagem à fila antes de consultar ou reivindicar o lote, e o listener é pausado até o circuito passar para meio-aberto. Se a inferência falhar depois da reivindicação, a falha é informada ao SocialGuard por `POST /api/v1/batches/{id}/failure`.

Para ver o circuito abrir sem um provedor real, defina `INFERENCE_SIMULATED_FAILURE_RATE` (de `0.0` a `1.0`) e, se quiser, `INFERENCE_SIMULATED_LATENCY`. O estado fica em `/actuator/prometheus`, nas métricas `resilience4j_circuitbreaker_*` com `name="openai"`.

As métricas permanecem disponíveis em `/actuator/prometheus`; a exportação OTLP de métricas foi desativada, pois o endpoint local configurado respondia `404`. A exportação de traces OTLP continua configurada separadamente.