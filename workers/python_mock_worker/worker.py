#!/usr/bin/env python3
"""
Python mock worker dla wzorca Scatter-Gather.

Demonstruje polyglot microservices — kontrakt komunikacyjny to czyste JSON/AMQP,
technologia implementacji providera jest bez znaczenia dla Orkiestratora.

Przepływ:
  1. Bind kolejki do Exchange 'orchestrator.tasks' z wybranym Routing Key
  2. Odbierz WorkerTaskMessage od Orkiestratora
  3. Przetwórz (wybierz dane z SAMPLE_DATA lub generuj dynamicznie)
  4. Opublikuj WorkerResultMessage na kolejkę z BasicProperties.reply_to
"""

import json
import logging
import os
import time
from copy import deepcopy
from datetime import datetime, date, timedelta, timezone
from typing import Any

import pika
import pika.exceptions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("python-mock-worker")


# ---------------------------------------------------------------------------
# Konfiguracja — każda wartość nadpisywalna przez zmienną środowiskową
# ---------------------------------------------------------------------------
RABBITMQ_HOST  = os.getenv("RABBITMQ_HOST",  "localhost")
RABBITMQ_PORT  = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER  = os.getenv("RABBITMQ_USER",  "guest")
RABBITMQ_PASS  = os.getenv("RABBITMQ_PASS",  "guest")
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")
TASK_EXCHANGE  = os.getenv("TASK_EXCHANGE",  "orchestrator.tasks")
WORKER_QUEUE   = os.getenv("WORKER_QUEUE",   "worker.python.mock")
BINDING_KEY    = os.getenv("BINDING_KEY",    "task.#")
PREFETCH_COUNT = int(os.getenv("PREFETCH_COUNT", "5"))


# ---------------------------------------------------------------------------
# Sample data — baza wiedzy mock workera.
# W produkcyjnym workerze ta sekcja jest zastąpiona wywołaniem zewnętrznego
# API lub logiką Web Scrapingu.
# ---------------------------------------------------------------------------
_SAMPLE_CATALOG: dict[str, dict[str, Any]] = {
    "attraction_wawel": {
        "name": "Zamek Królewski na Wawelu",
        "category": "zamek/muzeum",
        "address": "Wawel 5, 31-001 Kraków",
        "price_adult_pln": 30,
        "price_child_pln": 0,
        "currency": "PLN",
        "duration_minutes": 90,
        "booking_required": True,
        "opening_hours": {"pon": "zamknięte", "wt-nd": "09:30–17:00"},
        "available_slots": ["09:30", "10:30", "11:30", "13:00", "14:00", "15:00"],
        "capacity_per_slot": 25,
        "description": "Historyczna rezydencja królów polskich na wzgórzu wawelskim.",
        "url": "https://wawel.krakow.pl",
        "tags": ["UNESCO", "historia", "sztuka"],
    },
    "attraction_wieliczka": {
        "name": "Kopalnia Soli Wieliczka",
        "category": "kopalnia/dziedzictwo UNESCO",
        "address": "Daniłowicza 10, 32-020 Wieliczka",
        "price_adult_pln": 119,
        "price_child_pln": 89,
        "currency": "PLN",
        "duration_minutes": 120,
        "booking_required": True,
        "opening_hours": {"all_week": "08:00–17:00"},
        "available_slots": ["08:00", "09:00", "10:00", "11:00", "12:00", "13:00"],
        "capacity_per_slot": 35,
        "description": "Zabytkowa kopalnia soli wpisana na Listę Światowego Dziedzictwa UNESCO.",
        "url": "https://www.wieliczka-saltmine.com",
        "tags": ["UNESCO", "podziemia", "rodzinne"],
    },
    "attraction_auschwitz": {
        "name": "Muzeum Auschwitz-Birkenau",
        "category": "muzeum/miejsce pamięci",
        "address": "Więźniów Oświęcimia 20, 32-603 Oświęcim",
        "price_adult_pln": 0,
        "price_child_pln": 0,
        "currency": "PLN",
        "duration_minutes": 180,
        "booking_required": True,
        "opening_hours": {"all_week": "08:00–19:00"},
        "available_slots": ["08:00", "09:30", "11:00", "12:30", "14:00"],
        "capacity_per_slot": 20,
        "description": "Teren byłego obozu koncentracyjnego. Wstęp bezpłatny, wymagana rezerwacja.",
        "url": "https://www.auschwitz.org",
        "tags": ["UNESCO", "historia", "memorial"],
    },
    "transport_mpk": {
        "name": "MPK Kraków",
        "category": "komunikacja_miejska",
        "price_single_pln": 6.00,
        "price_day_pass_pln": 15.00,
        "price_48h_pass_pln": 24.00,
        "currency": "PLN",
        "lines_tram": ["1", "4", "8", "13", "18", "22", "52"],
        "lines_bus": ["100", "102", "144", "192", "501"],
        "real_time_url": "https://rozklady.mpk.krakow.pl",
        "app": "Jakdojade",
        "description": "Komunikacja miejska w Krakowie — tramwaje i autobusy.",
    },
}


def _compute_availability(base_slots: list[str], date_from: str | None, pax: dict | None) -> list[dict]:
    """
    Symuluje sprawdzanie dostępności: dla każdego slotu zwraca liczbę wolnych miejsc.
    W produkcji: zapytanie do API systemu rezerwacji lub scraping kalendarza.
    """
    adults  = (pax or {}).get("adults", 1)
    children = (pax or {}).get("children", 0)
    total_pax = adults + children

    slots_out = []
    for slot in base_slots:
        # Deterministyczny "pseudo-losowy" stan oparty na slocie — powtarzalny dla tych
        # samych danych wejściowych, co ułatwia testowanie.
        seed = sum(ord(c) for c in (slot + (date_from or "")))
        free_seats = max(0, 30 - (seed % 20))
        slots_out.append({
            "time": slot,
            "free_seats": free_seats,
            "bookable": free_seats >= total_pax,
        })
    return slots_out


def build_payload(target: str, intent: str, parameters: dict | None) -> dict:
    """
    Buduje payload odpowiedzi na podstawie katalogu i parametrów żądania.
    Zwraca dane właściwe dla danego target i intent (pricing / availability / oba).
    """
    parameters = parameters or {}
    date_from  = parameters.get("date_from")
    pax        = parameters.get("pax")

    base = deepcopy(_SAMPLE_CATALOG.get(target))

    if base is None:
        # Brak dedykowanych danych — generyczny fallback
        base = {
            "name": f"Nieznany zasób: {target}",
            "price_adult_pln": 25,
            "price_child_pln": 10,
            "available_slots": ["10:00", "14:00"],
            "description": "Brak danych w katalogu mock workera dla tego zasobu.",
        }

    intent_lower = intent.lower()
    result: dict[str, Any] = {
        "source": "python_mock_worker",
        "target": target,
        "name": base["name"],
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }

    # --- Dane cenowe (pricing / full) ---
    if "pricing" in intent_lower or "full" in intent_lower or "availabilityandpricing" in intent_lower:
        result["pricing"] = {
            "price_adult_pln":  base.get("price_adult_pln"),
            "price_child_pln":  base.get("price_child_pln"),
            "currency":          base.get("currency", "PLN"),
            "booking_required":  base.get("booking_required", False),
            "duration_minutes":  base.get("duration_minutes"),
        }

    # --- Dane dostępności (availability / full) ---
    if "availability" in intent_lower or "full" in intent_lower or "availabilityandpricing" in intent_lower:
        slots = base.get("available_slots", [])
        result["availability"] = {
            "date": date_from,
            "slots": _compute_availability(slots, date_from, pax),
            "opening_hours": base.get("opening_hours"),
            "capacity_per_slot": base.get("capacity_per_slot"),
        }

    # --- Metadane wspólne ---
    result["meta"] = {
        "address":     base.get("address"),
        "category":    base.get("category"),
        "tags":        base.get("tags", []),
        "url":         base.get("url"),
        "description": base.get("description"),
    }

    return result


# ---------------------------------------------------------------------------
# Konsument AMQP
# ---------------------------------------------------------------------------

def on_task_received(
    channel: pika.channel.Channel,
    method: pika.spec.Basic.Deliver,
    properties: pika.spec.BasicProperties,
    body: bytes,
) -> None:
    task = None
    try:
        task = json.loads(body)
        task_id    = task["task_id"]
        target     = task["target"]
        intent     = task["intent"]
        parameters = task.get("parameters")

        reply_to = properties.reply_to or task.get("reply_to", "orchestrator.results")

        logger.info("Otrzymano zadanie %s | Target: %s | Intent: %s", task_id, target, intent)

        payload = build_payload(target, intent, parameters)

        result = {
            "task_id": task_id,
            "target":  target,
            "success": True,
            "payload": payload,
            "error":   None,
        }

        channel.basic_publish(
            exchange="",
            routing_key=reply_to,
            properties=pika.BasicProperties(
                content_type="application/json",
                correlation_id=task_id,
            ),
            body=json.dumps(result, ensure_ascii=False),
        )

        channel.basic_ack(delivery_tag=method.delivery_tag)
        logger.info("Odpowiedź wysłana dla zadania %s → '%s'", task_id, reply_to)

    except json.JSONDecodeError as exc:
        logger.error("Błąd deserializacji JSON: %s", exc)
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except KeyError as exc:
        logger.error("Brakujące pole w WorkerTaskMessage: %s", exc)
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=False)
    except Exception as exc:
        task_id_log = (task or {}).get("task_id", "?")
        logger.exception("Nieoczekiwany błąd dla zadania %s: %s", task_id_log, exc)
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def connect_and_consume() -> None:
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters  = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VHOST,
        credentials=credentials,
        heartbeat=60,
        blocked_connection_timeout=300,
    )

    connection = pika.BlockingConnection(parameters)
    channel    = connection.channel()

    channel.exchange_declare(exchange=TASK_EXCHANGE, exchange_type="topic", durable=True)
    channel.queue_declare(queue=WORKER_QUEUE, durable=True)
    channel.queue_bind(queue=WORKER_QUEUE, exchange=TASK_EXCHANGE, routing_key=BINDING_KEY)
    channel.basic_qos(prefetch_count=PREFETCH_COUNT)
    channel.basic_consume(queue=WORKER_QUEUE, on_message_callback=on_task_received)

    logger.info(
        "Python MockWorker gotowy. Kolejka: '%s', Binding: '%s' → Exchange: '%s'",
        WORKER_QUEUE, BINDING_KEY, TASK_EXCHANGE,
    )
    channel.start_consuming()


def main() -> None:
    retry_delay = 5
    while True:
        try:
            connect_and_consume()
        except pika.exceptions.AMQPConnectionError as exc:
            logger.warning("Utracono połączenie: %s. Ponowna próba za %ds…", exc, retry_delay)
            time.sleep(retry_delay)
        except KeyboardInterrupt:
            logger.info("Zatrzymanie workera (SIGINT).")
            break


if __name__ == "__main__":
    main()
