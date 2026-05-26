#!/usr/bin/env python3
"""
Python Events Worker — wzorzec Scatter-Gather.
Operacja: 'events' | Binding: task.events.#
Zwraca: harmonogram wydarzeń z symulowaną dostępnością slotów dla podanej daty i pax.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone

import pika
import pika.exceptions

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("python-events-worker")

RABBITMQ_HOST  = os.getenv("RABBITMQ_HOST",  "localhost")
RABBITMQ_PORT  = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER  = os.getenv("RABBITMQ_USER",  "guest")
RABBITMQ_PASS  = os.getenv("RABBITMQ_PASS",  "guest")
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")
TASK_EXCHANGE  = os.getenv("TASK_EXCHANGE",  "orchestrator.tasks")
WORKER_QUEUE   = os.getenv("WORKER_QUEUE",   "worker.python.events")
BINDING_KEY    = os.getenv("BINDING_KEY",    "task.events.#")
PREFETCH_COUNT = int(os.getenv("PREFETCH_COUNT", "5"))

_DATA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "attractions.events.json")


def _load_catalog() -> dict:
    with open(_DATA_FILE, encoding="utf-8") as f:
        raw = json.load(f)
    return {a["attraction_id"]: a for a in raw["attractions"]}


_CATALOG: dict = _load_catalog()


def _slot_availability(slots: list[str], date_from: str | None, pax: dict | None, capacity: int) -> list[dict]:
    """
    Deterministyczna symulacja dostępności — seed oparty na slocie i dacie,
    dzięki czemu te same parametry zawsze zwracają te same wyniki (powtarzalność dla testów).
    """
    total_pax = (pax or {}).get("adults", 1) + (pax or {}).get("children", 0)
    result = []
    for slot in slots:
        seed       = sum(ord(c) for c in (slot + (date_from or "")))
        free_seats = max(0, capacity - (seed % (capacity + 1)))
        result.append({
            "time":       slot,
            "free_seats": free_seats,
            "bookable":   free_seats >= total_pax,
        })
    return result


def build_payload(target: str, parameters: dict | None) -> dict:
    parameters = parameters or {}
    date_from  = parameters.get("date_from")
    pax        = parameters.get("pax")
    attraction = _CATALOG.get(target)

    if attraction is None:
        return {
            "source":           "python_events_worker",
            "attraction_id":    target,
            "attraction_name":  f"Nieznana atrakcja: {target}",
            "error":            "Brak danych w katalogu events dla tego targetu.",
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        }

    events_out = []
    for event in attraction["events"]:
        events_out.append({
            "event_id":          event["event_id"],
            "name":              event["name"],
            "duration_minutes":  event["duration_minutes"],
            "capacity_per_slot": event["capacity_per_slot"],
            "slots":             _slot_availability(
                event["slots"], date_from, pax, event["capacity_per_slot"]
            ),
        })

    return {
        "source":           "python_events_worker",
        "attraction_id":    attraction["attraction_id"],
        "attraction_name":  attraction["attraction_name"],
        "query_date":       date_from,
        "events":           events_out,
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def on_task_received(channel, method, properties, body: bytes) -> None:
    task = None
    try:
        task       = json.loads(body)
        task_id    = task["task_id"]
        target     = task["target"]
        operation  = task["operation"]
        parameters = task.get("parameters")
        reply_to   = properties.reply_to or task.get("reply_to", "orchestrator.results")

        logger.info("Otrzymano zadanie %s | Target: %s | Operacja: %s", task_id, target, operation)

        payload = build_payload(target, parameters)

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
        logger.exception("Nieoczekiwany błąd dla zadania %s: %s", (task or {}).get("task_id", "?"), exc)
        channel.basic_nack(delivery_tag=method.delivery_tag, requeue=True)


def connect_and_consume() -> None:
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    params = pika.ConnectionParameters(
        host=RABBITMQ_HOST, port=RABBITMQ_PORT,
        virtual_host=RABBITMQ_VHOST, credentials=credentials,
        heartbeat=60, blocked_connection_timeout=300,
    )

    connection = pika.BlockingConnection(params)
    channel    = connection.channel()

    channel.exchange_declare(exchange=TASK_EXCHANGE, exchange_type="topic", durable=True)
    channel.queue_declare(queue=WORKER_QUEUE, durable=True)
    channel.queue_bind(queue=WORKER_QUEUE, exchange=TASK_EXCHANGE, routing_key=BINDING_KEY)
    channel.basic_qos(prefetch_count=PREFETCH_COUNT)
    channel.basic_consume(queue=WORKER_QUEUE, on_message_callback=on_task_received)

    logger.info(
        "EventsWorker gotowy. Kolejka: '%s', Binding: '%s' → Exchange: '%s'",
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
