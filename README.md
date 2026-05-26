# Szlakomat — Data Aggregation Hub

Mikroserwisowy orkiestrator danych oparty na wzorcu **Scatter-Gather**. Przyjmuje zapytanie o dane dla wielu atrakcji, rozsyła zadania do niezależnych data providerów przez RabbitMQ i agreguje odpowiedzi w Redis.

## Stack

| Warstwa | Technologia |
|---------|-------------|
| API / Orchestrator | ASP.NET Core 8, Swagger |
| Transport | RabbitMQ 3.13 (Topic Exchange) |
| Stan / Agregacja | Redis 7 (Hash + List) |
| Data providers (.NET) | .NET 8 Worker Service |
| Data providers (Python) | Python 3.11+, pika |

## Struktura projektów

```
src/
  TourDataOrchestrator.Domain          # Enums (OrchestratorTaskStatus)
  TourDataOrchestrator.Application     # Interfejsy (IMessagePublisher, IProviderRegistry…)
                                       # DTOs (WorkerTaskMessage, ProviderRegistration…)
  TourDataOrchestrator.Messaging       # RabbitMqMessagePublisher, ResultsConsumerService
  TourDataOrchestrator.Storage         # RedisTaskStateStore, RedisScrapingResultAggregator
                                       # RedisProviderRegistry
  TourDataOrchestrator.Api             # OrchestratorController, ProvidersController
  TourDataOrchestrator.MockWorker      # Przykładowy .NET worker (obsługuje task.#)
workers/
  python_mock_worker/
    pricing_worker.py                  # Python worker — operacja: pricing
    events_worker.py                   # Python worker — operacja: events
    data/
      attractions.pricing.json
      attractions.events.json
```

## Wymagania

- Docker (RabbitMQ + Redis)
- .NET 8 SDK
- Python 3.11+ z `pika` (`pip install pika`)

## Uruchomienie

**1. Infrastruktura**
```bash
docker compose up -d
```
RabbitMQ Management UI: `http://localhost:15672` (guest/guest)

**2. Orchestrator API**
```bash
dotnet run --project src/TourDataOrchestrator.Api
```
Swagger: `http://localhost:5000/swagger`

**3. Data providers (uruchom co najmniej jeden)**

.NET MockWorker:
```bash
dotnet run --project src/TourDataOrchestrator.MockWorker
```

Python workers:
```bash
cd workers/python_mock_worker
python pricing_worker.py   # terminal 1
python events_worker.py    # terminal 2
```

## API

### POST `/api/orchestrator/dispatch`
Rozsyła zadanie do workerów. Zwraca `task_id` i HTTP 202.

```json
{
  "operation": "pricing",
  "targets": ["attraction_wawel", "attraction_wieliczka"],
  "parameters": {
    "date_from": "2026-06-10",
    "pax": { "adults": 2, "children": 1 }
  }
}
```

`operation` odpowiada segmentowi routing key providera (`pricing`, `events` itd.).

### GET `/api/orchestrator/{taskId}`
Zwraca zagregowany wynik. Pollinguj do czasu gdy `status != "Processing"`.

| HTTP | Status | Znaczenie |
|------|--------|-----------|
| 202 | Processing | Czeka na odpowiedzi workerów |
| 200 | Completed | Wszystkie workery odpowiedziały poprawnie |
| 200 | CompletedPartially | Część workerów zgłosiła błąd |
| 404 | — | Nieznany `task_id` lub wygasł TTL (30 min) |

### GET `/api/providers`
Zwraca listę aktywnych data providerów zarejestrowanych w Redis.

```json
[
  {
    "provider_id": "mock-worker",
    "operation": "*",
    "binding_key": "task.#",
    "supported_targets": ["attraction_wawel", "attraction_wieliczka"],
    "registered_at": "2026-05-26T10:00:00Z"
  }
]
```

Provider jest widoczny dopóki jego TTL w Redis nie wygaśnie (heartbeat co 30 s, TTL = 90 s).

## Dodawanie nowego data providera (.NET)

1. Utwórz projekt `Microsoft.NET.Sdk.Worker`, dodaj referencje do `Application` i `Storage`.
2. Zaimplementuj `BackgroundService` z logiką konsumpcji kolejki.
3. Przy starcie zarejestruj się przez `IProviderRegistry.RegisterAsync(...)` i uruchom heartbeat.
4. Zadeklaruj kolejkę z `BindingKey = "task.{twoja_operacja}.#"`.
5. Odbieraj `WorkerTaskMessage`, zwracaj `WorkerResultMessage` na `reply_to`.

```csharp
await _registry.RegisterAsync(new ProviderRegistration(
    ProviderId:       "moj-worker",
    Operation:        "moja_operacja",
    BindingKey:       "task.moja_operacja.#",
    SupportedTargets: ["cel_a", "cel_b"],
    Description:      "Opis providera"
), ttl: TimeSpan.FromSeconds(90), stoppingToken);
```

Orchestrator nie wymaga żadnych zmian — nowy provider jest automatycznie widoczny w `GET /api/providers`.

## Python workers

Dwa niezależne workery demonstrują podejście polyglot — ten sam kontrakt AMQP/JSON, inna technologia:

| Worker | Kolejka | Operacja | Zwracane dane |
|--------|---------|----------|---------------|
| `pricing_worker.py` | `worker.python.pricing` | `pricing` | Typy biletów, szacowany koszt |
| `events_worker.py` | `worker.python.events` | `events` | Harmonogram, dostępność slotów |

Dane statyczne w `data/attractions.pricing.json` i `data/attractions.events.json`. Konfiguracja przez zmienne środowiskowe (patrz `.env.example`).

## Konfiguracja

Wszystkie wartości domyślne zakładają lokalne środowisko Docker. Sekcje w `appsettings.json`:

```json
"RabbitMq": { "Host": "localhost", "Port": 5672, "..." : "..." },
"Redis":    { "ConnectionString": "localhost:6379", "TaskStateTtl": "00:30:00" }
```

Szczegóły architektury i wzorców projektowych: [ARCHITECTURE.md](ARCHITECTURE.md)
