#!/usr/bin/env python3
"""
Load producer: pushes claim-check messages (pointers to raw Parquet files) into SQS,
either at a fixed rate, an exact count, or following the simulated 24h profile.
"""

import argparse
import json
import logging
import time
import uuid
from concurrent.futures import ThreadPoolExecutor

from common import HOURLY_RATES, RAW_BUCKET, SQS_QUEUE_URL, client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("producer")

TOTAL_RAW_BATCHES = 150  # files written by generator.py at sf=0.05
SQS_BATCH = 10           # SendMessageBatch maximum


def make_entries(count, start_id):
    entries = []
    for i in range(count):
        batch_num = (start_id + i) % TOTAL_RAW_BATCHES
        entries.append({
            "Id": f"msg_{i}",
            "MessageBody": json.dumps({
                "event_id": str(uuid.uuid4()),
                "timestamp": time.time(),
                "batch_id": batch_num,
                "bucket": RAW_BUCKET,
                "s3_key": f"raw/orders/batch_{batch_num:05d}.parquet",
            }),
        })
    return entries


def send(sqs, entries):
    try:
        sqs.send_message_batch(QueueUrl=SQS_QUEUE_URL, Entries=entries)
    except Exception as e:
        logger.error(f"Erro ao enviar lote SQS: {e}")


def run_rate(rate, duration=None, limit=None):
    """Send `rate` msg/s until `duration` seconds pass or `limit` messages are sent."""
    logger.info(f"Gerando tráfego a {rate} msg/s (duração={duration}s, limite={limit} msgs)...")
    sqs = client("sqs")
    threads = 16 if rate >= 1000 else 8
    start = time.time()
    sent = 0

    with ThreadPoolExecutor(max_workers=threads) as pool:
        while (duration is None or time.time() - start < duration) and (limit is None or sent < limit):
            cycle = time.time()
            target = rate if limit is None else min(rate, limit - sent)
            futures = []
            for offset in range(0, target, SQS_BATCH):
                n = min(SQS_BATCH, target - offset)
                futures.append(pool.submit(send, sqs, make_entries(n, sent)))
                sent += n
            for f in futures:
                f.result()
            if limit is not None and sent >= limit:
                break
            time.sleep(max(0.0, 1.0 - (time.time() - cycle)))

    total = max(0.001, time.time() - start)
    logger.info(f"Concluído: {sent} mensagens em {total:.1f}s (taxa efetiva: {sent / total:.1f} msg/s)")


def run_day(seconds_per_hour):
    logger.info(f"Simulando 24h ({seconds_per_hour}s por hora = {seconds_per_hour * 24 / 60:.1f} min)...")
    for hour, rate in enumerate(HOURLY_RATES):
        logger.info(f"=== HORA SIMULADA: {hour:02d}:00 | TAXA ALVO: {rate} msg/s ===")
        run_rate(rate, duration=seconds_per_hour)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Load Producer para Ingestion Autoscaling")
    parser.add_argument("--mode", choices=["test", "full", "hour", "rate", "count"], default="test",
                        help="test (100 msg/s), count (N msgs), full (dia 24h), hour (hora específica), rate (taxa fixa)")
    parser.add_argument("--messages", type=int, default=1000, help="Quantidade de mensagens no modo count")
    parser.add_argument("--rate", type=int, default=250, help="Taxa em msg/s para modo rate ou count")
    parser.add_argument("--duration", type=int, default=10, help="Duração em segundos para modo rate, hour ou test")
    parser.add_argument("--hour", type=int, default=19, help="Hora (0 a 23) para modo hour")
    parser.add_argument("--seconds-per-hour", type=int, default=60, help="Segundos por hora no modo full")
    args = parser.parse_args()

    if args.mode == "count":
        run_rate(args.rate, limit=args.messages)
    elif args.mode == "test":
        run_rate(100, duration=args.duration)
    elif args.mode == "full":
        run_day(args.seconds_per_hour)
    elif args.mode == "hour":
        run_rate(HOURLY_RATES[args.hour], duration=args.duration)
    elif args.mode == "rate":
        run_rate(args.rate, duration=args.duration)
