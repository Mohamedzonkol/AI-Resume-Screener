import asyncio
import json
import time
import pika
from pika.exceptions import AMQPConnectionError
from app.core.config import settings
from app.services.resume_parser import handle_resume_parse
from app.services.rank_refresh import handle_rank_refresh


def start_consumers():
    url = settings.RABBITMQ_URL
    if not url:
        print("[AI Worker] ERROR: RABBITMQ_URL not set. Consumer thread exiting.", flush=True)
        return

    if not url.endswith("/"):
        url += "/"

    params = pika.URLParameters(url)
    delay = 1

    # ✅ One event loop per worker thread
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    while True:
        try:
            print(f"[AI Worker] Connecting to RabbitMQ at {url} …", flush=True)
            connection = pika.BlockingConnection(params)
            channel = connection.channel()

            channel.queue_declare(queue="resume.parse", durable=True)
            channel.queue_declare(queue="rank.refresh", durable=True)
            channel.basic_qos(prefetch_count=1)

            def safe_json(body):
                try:
                    return json.loads(body)
                except json.JSONDecodeError:
                    return None

            def on_resume_parse(ch, method, properties, body):
                msg = safe_json(body)
                if msg is None:
                    ch.basic_nack(method.delivery_tag, requeue=False)
                    return

                try:
                    loop.run_until_complete(handle_resume_parse(msg))
                    ch.basic_ack(method.delivery_tag)
                except Exception as e:
                    print(f"[AI Worker] resume.parse failed: {e}", flush=True)
                    ch.basic_nack(method.delivery_tag, requeue=True)

            def on_rank_refresh(ch, method, properties, body):
                msg = safe_json(body)
                if msg is None:
                    ch.basic_nack(method.delivery_tag, requeue=False)
                    return

                try:
                    loop.run_until_complete(handle_rank_refresh(msg))
                    ch.basic_ack(method.delivery_tag)
                except Exception as e:
                    print(f"[AI Worker] rank.refresh failed: {e}", flush=True)
                    ch.basic_nack(method.delivery_tag, requeue=True)

            channel.basic_consume("resume.parse", on_resume_parse)
            channel.basic_consume("rank.refresh", on_rank_refresh)

            print("[AI Worker] Connected to RabbitMQ, consuming…", flush=True)
            delay = 1
            channel.start_consuming()

        except AMQPConnectionError:
            print(f"[AI Worker] RabbitMQ not ready, retrying in {delay}s…", flush=True)
            time.sleep(delay)
            delay = min(delay * 2, 30)
