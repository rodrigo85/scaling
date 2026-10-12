#!/usr/bin/env python3
"""
Consumer worker: polls SQS, aggregates the raw Parquet batch with DuckDB, writes it to
the refined bucket, publishes a completion event and exposes Prometheus metrics.
"""

import json
import logging
import os
import re
import signal
import time
from datetime import datetime, timezone

import duckdb
from prometheus_client import Counter, Gauge, Histogram, start_http_server

from common import ACCESS_KEY, ENDPOINT, PROCESSED_QUEUE_URL, RAW_BUCKET, REFINED_BUCKET, SECRET_KEY, SQS_QUEUE_URL, client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("consumer")

METRICS_PORT = int(os.getenv("METRICS_PORT", "8000"))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))
WAIT_TIME_SECONDS = int(os.getenv("WAIT_TIME_SECONDS", "1"))
# Message bodies come from the queue: only accept keys shaped like the generator's output.
RAW_KEY_RE = re.compile(r"^raw/orders/batch_\d{5}\.parquet$")

MESSAGES_PROCESSED = Counter("consumer_messages_processed_total", "Total de mensagens SQS processadas")
RECORDS_PROCESSED = Counter("consumer_records_processed_total", "Total de registros processados")
EVENTS_EMITTED = Counter("consumer_events_emitted_total", "Total de eventos de saida publicados no SQS downstream")
PROCESSING_DURATION = Histogram("consumer_processing_seconds", "Tempo de processamento por lote de mensagens")
ACTIVE_WORKER = Gauge("consumer_active_workers", "Indica se o worker está ativo")

running = True


def stop(signum, frame):
    # Finish the current batch, then leave the loop (Kubernetes scale-in sends SIGTERM).
    global running
    logger.info("Sinal de parada recebido. Encerrando após o lote atual...")
    running = False


def setup_duckdb():
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    endpoint = ENDPOINT.replace("http://", "").replace("https://", "")
    con.execute(f"""
        SET s3_endpoint = '{endpoint}';
        SET s3_url_style = 'path';
        SET s3_use_ssl = false;
        SET s3_access_key_id = '{ACCESS_KEY}';
        SET s3_secret_access_key = '{SECRET_KEY}';
    """)
    return con


def process_message(con, sqs, body):
    data = json.loads(body)
    raw_key = data["s3_key"]
    if not RAW_KEY_RE.match(raw_key):
        raise ValueError(f"s3_key inválida: {raw_key!r}")
    batch_id = int(data.get("batch_id", 0))

    now = datetime.now(timezone.utc)
    year, month, day = now.strftime("%Y"), now.strftime("%m"), now.strftime("%d")
    input_path = f"s3://{RAW_BUCKET}/{raw_key}"
    output_key = f"refined/orders/year={year}/month={month}/day={day}/orders_batch_{batch_id:05d}_{int(time.time() * 1000)}.parquet"
    output_path = f"s3://{REFINED_BUCKET}/{output_key}"

    logger.info(f"[ENTRADA] Lote #{batch_id} recebido | Origem: {input_path}")
    # COPY returns the number of rows written (one per order).
    records = con.execute(f"""
        COPY (
            SELECT
                o_orderkey,
                o_custkey,
                o_orderstatus,
                o_totalprice,
                o_orderdate,
                count(*) as item_count,
                sum(l_extendedprice) as gross_value,
                sum(l_extendedprice * (1 - l_discount)) as net_value,
                current_timestamp as processed_at
            FROM read_parquet('{input_path}')
            GROUP BY 1, 2, 3, 4, 5
        ) TO '{output_path}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """).fetchone()[0]

    if PROCESSED_QUEUE_URL:
        try:
            sqs.send_message(QueueUrl=PROCESSED_QUEUE_URL, MessageBody=json.dumps({
                "event_type": "DATASET_REFINED_READY",
                "table": "orders",
                "bucket": REFINED_BUCKET,
                "s3_key": output_key,
                "s3_uri": output_path,
                "batch_id": batch_id,
                "records": records,
                "year": year,
                "month": month,
                "day": day,
                "completed_at": now.isoformat(),
            }))
            EVENTS_EMITTED.inc()
        except Exception as e:
            logger.warning(f"Aviso: falha ao emitir evento downstream para {output_key}: {e}")

    logger.info(f"[SAIDA]   Lote #{batch_id} refinado gravado | Destino: {output_path} ({records} pedidos)")
    return records


def run_consumer():
    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    logger.info("Iniciando consumer worker...")
    start_http_server(METRICS_PORT)
    ACTIVE_WORKER.set(1)
    sqs = client("sqs")
    con = setup_duckdb()
    logger.info(f"Escutando fila: {SQS_QUEUE_URL}")

    while running:
        try:
            messages = sqs.receive_message(
                QueueUrl=SQS_QUEUE_URL,
                MaxNumberOfMessages=BATCH_SIZE,
                WaitTimeSeconds=WAIT_TIME_SECONDS,
            ).get("Messages", [])
            if not messages:
                continue

            start_t = time.time()
            done = []
            for msg in messages:
                try:
                    RECORDS_PROCESSED.inc(process_message(con, sqs, msg["Body"]))
                    MESSAGES_PROCESSED.inc()
                    done.append({"Id": msg["MessageId"], "ReceiptHandle": msg["ReceiptHandle"]})
                except Exception as e:
                    # Not deleted: SQS redelivers it and, after 3 attempts, moves it to the DLQ.
                    logger.error(f"Erro ao processar mensagem {msg['MessageId']}: {e}")

            if done:
                sqs.delete_message_batch(QueueUrl=SQS_QUEUE_URL, Entries=done)

            duration = time.time() - start_t
            PROCESSING_DURATION.observe(duration)
            logger.info(f"Processadas {len(done)} mensagens em {duration:.2f}s")
        except Exception as e:
            logger.error(f"Erro no loop do consumer: {e}")
            time.sleep(2)

    ACTIVE_WORKER.set(0)
    logger.info("Worker encerrado.")


if __name__ == "__main__":
    run_consumer()
