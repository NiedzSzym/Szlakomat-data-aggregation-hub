# Architektura — Scatter-Gather Data Aggregation

## Wzorzec Scatter-Gather

System implementuje wzorzec **Scatter-Gather** (fan-out / fan-in):

```
Client
  │
  │ POST /dispatch { operation, targets[], parameters }
  ▼
Orchestrator API
  ├── Redis: inicjalizuje stan zadania (expected=N, received=0, status=Processing)
  ├── RabbitMQ publish → task.{operation}.{target_1}
  ├── RabbitMQ publish → task.{operation}.{target_2}
  └── RabbitMQ publish → task.{operation}.{target_N}
        │
        │  Topic Exchange: orchestrator.tasks
        │
        ├─► Worker A (binding: task.pricing.#)
        ├─► Worker B (binding: task.events.#)
        └─► Worker C (binding: task.*.attraction_wawel)
              │
              │  reply_to: orchestrator.results
              ▼
        ResultsConsumerService (BackgroundService)
              │
              ▼
        RedisScrapingResultAggregator
              ├── RPUSH task:{id}:results <json>
              ├── HINCRBY task:{id} received 1  (atomowe)
              └── (jeśli received = expected) → finalizacja → status Completed

Client
  │ GET /{taskId}  (polling)
  ▼
Orchestrator API → Redis → AggregatedResult
```

## Routing

Routing key ma format `task.{operation}.{target}`, np.:

| Operacja | Target | Routing key |
|----------|--------|-------------|
| pricing | attraction_wawel | `task.pricing.attraction_wawel` |
| events | attraction_wieliczka | `task.events.attraction_wieliczka` |

Worker deklaruje własny BindingKey pattern przy starcie. Orchestrator nie zna workerów — zna tylko konwencję klucza. Nowy provider podpina się bez jakiejkolwiek zmiany w kodzie orchestratora.

## Provider Registry

Workers rejestrują się w Redis przy połączeniu i odnawiają TTL heartbeatem co 30 s (TTL = 90 s). Po wyłączeniu workera jego wpis wygasa automatycznie.

```
Redis:
  provider:{id}     → JSON (ProviderRegistration), TTL = 90s
  providers:index   → Set wszystkich zarejestrowanych IDs
```

`GET /api/providers` odpytuje registry i zwraca listę aktywnych providerów z ich operacjami i obsługiwanymi targetami. To jedyny mechanizm discovery — orchestrator i klienci API mogą dynamicznie poznawać dostępne możliwości systemu.

## Stan zadania w Redis

Każde zadanie ma dwa klucze z TTL 30 minut:

```
task:{id}          → Hash { expected, received, status }
task:{id}:results  → List [ json, json, … ]
```

`HINCRBY` (atomowa inkrementacja) eliminuje race condition przy równoległych odpowiedziach wielu workerów.

## Clean Architecture

```
Domain          ← enums, czyste typy domenowe, zero zależności
Application     ← interfejsy (IMessagePublisher, IProviderRegistry…), DTOs
                   zależy tylko od Domain
Messaging       ← RabbitMQ: publisher (Singleton + lazy connection),
                   ResultsConsumerService (BackgroundService z retry loop)
                   zależy od Application
Storage         ← Redis: TaskStateStore, ScrapingResultAggregator, ProviderRegistry
                   zależy od Application
Api             ← ASP.NET Core: kontrolery, DI composition root
                   zależy od Application + rejestruje Messaging i Storage
Workers         ← niezależne procesy, zależą od Application + Storage
```

Warstwa `Application` nie zależy od infrastruktury — `IMessagePublisher` i `IProviderRegistry` to interfejsy, których implementacje mogą być podmieniane bez zmiany logiki biznesowej.

## Odporność

**BackgroundService retry loop** — zarówno `ResultsConsumerService` jak i każdy worker opakowują `ExecuteAsync` w pętlę `while`, która łapie `BrokerUnreachableException` i `AlreadyClosedException`. Serwis przeżywa chwilowy brak brokera bez zatrzymania hosta.

**At-least-once delivery** — manual ACK po udanej agregacji. NACK z `requeue=false` dla wiadomości których nie da się zdekodować (dead-letter). NACK z `requeue=true` dla przejściowych błędów przetwarzania.

**TTL jako zabezpieczenie** — klucze Redis wygasają niezależnie od tego czy zadanie zostało ukończone, co zapobiega wyciekom pamięci przy nieodpowiadających workerach.
