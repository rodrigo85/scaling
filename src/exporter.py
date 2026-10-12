#!/usr/bin/env python3
"""
Prometheus exporter for the Grafana dashboard: SQS queue depth, oldest-message age,
in/out rates, and Kubernetes consumer pods and nodes (real k3d vs Karpenter KWOK).
"""

import json
import logging
import os
import ssl
import time
import urllib.request

from prometheus_client import Gauge, start_http_server

from common import PROCESSED_QUEUE_URL, SQS_QUEUE_URL, client

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("exporter")

K8S_API_URL = os.getenv("K8S_API_URL", "https://k3d-scaling-cluster-server-0:6443")
TOKEN_FILE = os.getenv("TOKEN_FILE", "/app/secrets/k8s-token.txt")
EXPORTER_PORT = int(os.getenv("EXPORTER_PORT", "9190"))
SCRAPE_INTERVAL = int(os.getenv("SCRAPE_INTERVAL", "2"))
POD_SELECTOR = "app%3Dworker"

QUEUE_VISIBLE = Gauge("sqs_approximate_number_of_messages", "Mensagens visíveis (backlog)", ["queue"])
QUEUE_NOT_VISIBLE = Gauge("sqs_approximate_number_of_messages_not_visible", "Mensagens em processamento", ["queue"])
QUEUE_OLDEST_MESSAGE_AGE = Gauge("sqs_oldest_message_age_seconds", "Espera da mensagem mais antiga da fila de entrada")
INBOUND_RATE = Gauge("pipeline_inbound_rate_per_sec", "Taxa de entrada (msg/s)")
OUTBOUND_RATE = Gauge("pipeline_outbound_rate_per_sec", "Taxa de saída/processamento (msg/s)")
CONSUMER_PODS = Gauge("k8s_active_consumer_pods", "Pods consumers Running ou Pending")
CLUSTER_NODES = Gauge("k8s_active_cluster_nodes", "Nós no cluster (reais + Karpenter)")
PODS_PER_NODE = Gauge("k8s_pods_per_node", "Pods workers por nó", ["node", "node_type"])
NODE_INFO = Gauge("k8s_node_info", "Nós do cluster (reais vs Karpenter KWOK)", ["node", "node_type", "cpu_capacity", "status"])


def queue_depth(sqs, url):
    attrs = sqs.get_queue_attributes(
        QueueUrl=url,
        AttributeNames=["ApproximateNumberOfMessages", "ApproximateNumberOfMessagesNotVisible"],
    )["Attributes"]
    visible = int(attrs.get("ApproximateNumberOfMessages", 0))
    not_visible = int(attrs.get("ApproximateNumberOfMessagesNotVisible", 0))
    name = url.rsplit("/", 1)[-1]
    QUEUE_VISIBLE.labels(queue=name).set(visible)
    QUEUE_NOT_VISIBLE.labels(queue=name).set(not_visible)
    return visible, not_visible


def oldest_message_age(sqs):
    # Peek with VisibilityTimeout=0 so the message stays available to consumers.
    msgs = sqs.receive_message(
        QueueUrl=SQS_QUEUE_URL, AttributeNames=["SentTimestamp"], VisibilityTimeout=0, MaxNumberOfMessages=1,
    ).get("Messages", [])
    if not msgs:
        return 0.0
    sent = float(msgs[0]["Attributes"]["SentTimestamp"]) / 1000.0
    return round(max(0.0, time.time() - sent), 2)


def query_k8s(ctx, token, path):
    req = urllib.request.Request(f"{K8S_API_URL}{path}", headers={"Authorization": f"Bearer {token}"})
    return json.loads(urllib.request.urlopen(req, context=ctx, timeout=3).read())


def node_label(name):
    if "k3d-" not in name:
        return f"karpenter-{name.rsplit('-', 1)[-1]}", "Karpenter-KWOK"
    if "agent" in name:
        return name.replace("k3d-scaling-cluster-", ""), "Fisico-k3d"
    return "server-control-plane", "Fisico-k3d"


def collect_k8s(ctx, token):
    pods = query_k8s(ctx, token, f"/api/v1/namespaces/default/pods?labelSelector={POD_SELECTOR}")["items"]
    nodes = query_k8s(ctx, token, "/api/v1/nodes")["items"]
    CONSUMER_PODS.set(sum(1 for p in pods if p["status"].get("phase") in ("Running", "Pending")))
    CLUSTER_NODES.set(len(nodes))

    # Drop series of nodes Karpenter already removed.
    NODE_INFO.clear()
    PODS_PER_NODE.clear()
    per_node = {n["metadata"]["name"]: 0 for n in nodes}
    for p in pods:
        node = p["spec"].get("nodeName")
        if node in per_node:
            per_node[node] += 1

    for n in nodes:
        name = n["metadata"]["name"]
        short, n_type = node_label(name)
        ready = any(c["type"] == "Ready" and c["status"] == "True" for c in n["status"].get("conditions", []))
        cpu = n["status"].get("capacity", {}).get("cpu", "N/A")
        NODE_INFO.labels(node=short, node_type=n_type, cpu_capacity=cpu, status="Ready" if ready else "NotReady").set(1)
        PODS_PER_NODE.labels(node=short, node_type=n_type).set(per_node[name])


def main():
    logger.info(f"Exporter na porta {EXPORTER_PORT}...")
    start_http_server(EXPORTER_PORT)
    sqs = client("sqs")
    # k3d API uses a self-signed cert; the exporter only reads from the local lab cluster.
    ssl_ctx = ssl._create_unverified_context()

    last_time = time.time()
    last_in = last_out = 0
    in_rate = out_rate = 0.0

    while True:
        now = time.time()
        dt = max(0.5, now - last_time)
        try:
            visible, not_visible = queue_depth(sqs, SQS_QUEUE_URL)
            QUEUE_OLDEST_MESSAGE_AGE.set(oldest_message_age(sqs) if visible else 0.0)

            # The output queue is never consumed, so its depth = total processed so far.
            out_total = sum(queue_depth(sqs, PROCESSED_QUEUE_URL))
            in_total = visible + not_visible + out_total

            # Hold the last non-zero rate and decay it, so the panel doesn't flicker to 0.
            raw_in = (in_total - last_in) / dt if last_in else 0.0
            raw_out = (out_total - last_out) / dt if last_out else 0.0
            in_rate = raw_in if raw_in > 0 else in_rate * 0.7
            out_rate = raw_out if raw_out > 0 else out_rate * 0.7
            INBOUND_RATE.set(round(in_rate, 2))
            OUTBOUND_RATE.set(round(out_rate, 2))
            last_in, last_out, last_time = in_total, out_total, now
        except Exception as e:
            logger.error(f"Erro ao coletar SQS: {e}")

        if os.path.exists(TOKEN_FILE):
            try:
                with open(TOKEN_FILE) as f:
                    collect_k8s(ssl_ctx, f.read().strip())
            except Exception as e:
                logger.error(f"Erro ao consultar API do Kubernetes: {e}")

        time.sleep(SCRAPE_INTERVAL)


if __name__ == "__main__":
    main()
