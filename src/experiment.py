#!/usr/bin/env python3
"""
Experiment runner: configures scenario A (fixo, 2 pods), B (KEDA + Karpenter) or
C (grande, 40 pods), plays the simulated 24h load and samples metrics every second to CSV.
"""

import argparse
import csv
import subprocess
import threading
import time
from pathlib import Path

from common import HOURLY_RATES, SQS_QUEUE_URL, client
from producer import run_rate

ROOT = Path(__file__).resolve().parent.parent
KEDA_MANIFEST = str(ROOT / "k8s" / "keda-scaledobject.yaml")
FIELDS = ["timestamp", "simulated_hour", "target_rate_msgs_sec", "sqs_backlog", "sqs_inflight", "consumer_pods", "active_nodes"]
FIXED_REPLICAS = {"fixo": 2, "grande": 40}


def kubectl(*args):
    return subprocess.check_output(["kubectl", *args], stderr=subprocess.DEVNULL, text=True)


def sample(sqs):
    try:
        attrs = sqs.get_queue_attributes(QueueUrl=SQS_QUEUE_URL, AttributeNames=[
            "ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible",
        ])["Attributes"]
        backlog = int(attrs["ApproximateNumberOfMessages"])
        inflight = int(attrs["ApproximateNumberOfMessagesNotVisible"])
    except Exception:
        backlog = inflight = 0
    try:
        pods = sum("Running" in line for line in kubectl("get", "pods", "-l", "app=worker", "--no-headers").splitlines())
        # " Ready" with the space, so "NotReady" is not counted.
        nodes = sum(" Ready" in line for line in kubectl("get", "nodes", "--no-headers").splitlines())
    except Exception:
        pods = nodes = 0
    return backlog, inflight, pods, nodes


def configure_scenario(scenario, sqs):
    print(f"\n=== CONFIGURANDO CENÁRIO: {scenario.upper()} ===")
    if scenario == "keda":
        subprocess.run(["kubectl", "apply", "-f", KEDA_MANIFEST], check=True)
        print("[SETUP] Cenário B: KEDA ativo (2 a 40 pods) + Karpenter para nós.")
    else:
        replicas = FIXED_REPLICAS[scenario]
        subprocess.run(["kubectl", "delete", "-f", KEDA_MANIFEST, "--ignore-not-found=true"], check=False)
        subprocess.run(["kubectl", "scale", "deployment", "worker", f"--replicas={replicas}"], check=True)
        print(f"[SETUP] KEDA desativado, frota fixa em {replicas} pods.")

    try:
        sqs.purge_queue(QueueUrl=SQS_QUEUE_URL)
        print("[SETUP] Fila SQS purgada.")
    except Exception as e:
        print(f"[SETUP] Aviso ao purgar fila: {e}")
    time.sleep(3)


def run_experiment(scenario, seconds_per_hour, output_csv):
    sqs = client("sqs")
    configure_scenario(scenario, sqs)
    print(f"\n[EXECUÇÃO] {24 * seconds_per_hour}s ({seconds_per_hour}s por hora simulada)")

    rows = []
    stop = threading.Event()

    def collector():
        start = time.time()
        while not stop.is_set():
            elapsed = time.time() - start
            hour = min(23, int(elapsed / seconds_per_hour))
            backlog, inflight, pods, nodes = sample(sqs)
            rows.append(dict(zip(FIELDS, [round(elapsed, 1), hour, HOURLY_RATES[hour], backlog, inflight, pods, nodes])))
            stop.wait(1.0)

    t = threading.Thread(target=collector, daemon=True)
    t.start()

    for hour, rate in enumerate(HOURLY_RATES):
        print(f"\n---> [Hora {hour:02d}:00] Taxa: {rate} msg/s | Backlog atual: {rows[-1]['sqs_backlog'] if rows else 0}")
        run_rate(rate, duration=seconds_per_hour)

    print("\n[EXECUÇÃO] Injeção concluída. Coletando mais 10s de drenagem...")
    time.sleep(10)
    stop.set()
    t.join(timeout=3)

    output_csv.parent.mkdir(parents=True, exist_ok=True)
    with output_csv.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(rows)

    n = max(1, len(rows))
    print(f"\n=== RESUMO: CENÁRIO {scenario.upper()} ===")
    print(f"  CSV:                    {output_csv}")
    print(f"  Pico de backlog:        {max((r['sqs_backlog'] for r in rows), default=0)} mensagens")
    print(f"  Pico de pods:           {max((r['consumer_pods'] for r in rows), default=0)}")
    print(f"  Pico de nós:            {max((r['active_nodes'] for r in rows), default=0)}")
    print(f"  Média de pods no dia:   {sum(r['consumer_pods'] for r in rows) / n:.1f}")
    print(f"  Média de nós no dia:    {sum(r['active_nodes'] for r in rows) / n:.1f}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Executor de Experimentos de Autoscaling")
    parser.add_argument("--scenario", choices=["fixo", "keda", "grande"], required=True)
    parser.add_argument("--seconds-per-hour", type=int, default=10, help="Segundos por hora simulada (10 = 4 min por rodada)")
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()
    run_experiment(args.scenario, args.seconds_per_hour, args.output or ROOT / "results" / f"scenario_{args.scenario}.csv")
