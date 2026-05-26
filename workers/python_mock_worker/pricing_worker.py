#!/usr/bin/env python3
"""
Python Pricing Worker — wzorzec Scatter-Gather.
Operacja: 'pricing' | Binding: task.pricing.#
Zwraca: typy biletów + szacowany koszt wizyty dla podanego pax.
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
logger = logging.getLogger("python-pricing-worker")

RABBITMQ_HOST  = os.getenv("RABBITMQ_HOST",  "localhost")
RABBITMQ_PORT  = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER  = os.getenv("RABBITMQ_USER",  "guest")
RABBITMQ_PASS  = os.getenv("RABBITMQ_PASS",  "guest")
RABBITMQ_VHOST = os.getenv("RABBITMQ_VHOST", "/")
TASK_EXCHANGE  = os.getenv("TASK_EXCHANGE",  "orchestrator.tasks")
WORKER_QUEUE   = os.getenv("WORKER_QUEUE",   "worker.python.pricing")
BINDING_KEY    = os.getenv("BINDING_KEY",    "task.pricing.#")
PREFETCH_COUNT = int(os.getenv("PREFETCH_COUNT", "5"))

_DATA_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data", "attractions.pricing.json")


def _load_catalog() -> dict:
    with open(_DATA_FILE, encoding="utf-8") as f:
        raw = json.load(f)
    return {a["attraction_id"]: a for a in raw["attractions"]}


_CATALOG: dict = _load_catalog()


def _estimate_total(ticket_types: list[dict], pax: dict | None) -> dict:
    pax      = pax or {}
    adults   = pax.get("adults", 1)
    children = pax.get("children", 0)

    adult_price = next((t["price"] for t in ticket_types if t["type"] == "adult"), 0.0)
    child_price = next((t["price"] for t in ticket_types if t["type"] == "child"), 0.0)

    return {
        "adults":   adults,
        "children": children,
        "total":    round(adult_price * adults + child_price * children, 2),
        "currency": "PLN",
    }


def build_payload(target: str, parameters: dict | None) -> dict:
    parameters = parameters or {}
    attraction = _CATALOG.get(target)

    if attraction is None:
        return {
            "source":           "python_pricing_worker",
            "attraction_id":    target,
            "attraction_name":  f"Nieznana atrakcja: {target}",
            "error":            "Brak danych w katalogu pricing dla tego targetu.",
            "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        }

    ticket_types = attraction["ticket_types"]

    return {
        "source":           "python_pricing_worker",
        "attraction_id":    attraction["attraction_id"],
        "attraction_name":  attraction["attraction_name"],
        "currency":         attraction["currency"],
        "ticket_types":     ticket_types,
        "estimated_total":  _estimate_total(ticket_types, parameters.get("pax")),
        "notes":            attraction.get("notes"),
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
        "PricingWorker gotowy. Kolejka: '%s', Binding: '%s' → Exchange: '%s'",
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
