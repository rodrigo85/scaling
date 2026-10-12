VPS_HOST ?= vpsmouts
VPS_DIR ?= /opt/scaling
KARPENTER_REPO ?= /tmp/karpenter-repo
RUN = ssh $(VPS_HOST)
PY = cd $(VPS_DIR)/src && python3

MSGS ?= 1000
RATE ?= 250
DURATION ?= 10
SEC_PER_HOUR ?= 5

.PHONY: help sync up down status tunnel tf-init tf-apply tf-destroy gen-data \
	test-producer test-consumer test-pipeline \
	k8s-cluster k8s-keda k8s-karpenter k8s-build k8s-deploy k8s-token k8s-status \
	inject-rate inject-msgs test-autoscaling test-node-autoscaling watch-scaling karpenter-logs logs-consumer \
	dia-fixo dia-keda dia-grande fetch-results

help: ## Lista os comandos
	@grep -E '^[a-z0-9-]+:.*## ' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  make %-22s %s\n", $$1, $$2}'

# --- Infra base (LocalStack, exporter, Prometheus, Grafana) ---

sync: ## Copia o código para a VPS
	rsync -avz --exclude '.git' --exclude '.claude' --exclude '__pycache__' --exclude '.terraform' \
		--exclude 'secrets' --exclude 'results' ./ $(VPS_HOST):$(VPS_DIR)/

up: sync ## Sobe LocalStack, exporter, Prometheus e Grafana
	$(RUN) "cd $(VPS_DIR)/docker && docker compose up -d --build"
	$(RUN) "until curl -s http://127.0.0.1:4566/_localstack/health | grep -qE '\"s3\": \"(running|available)\"'; do sleep 2; done; echo 'LocalStack pronto'"

down: ## Para os containers
	$(RUN) "cd $(VPS_DIR)/docker && docker compose down"

status: ## Containers e recursos livres da VPS
	$(RUN) "docker ps --filter 'name=scaling-' --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'; free -h; df -h /"

tunnel: ## Túnel SSH: Grafana em localhost:3001, LocalStack em localhost:4566
	ssh -N -L 3001:127.0.0.1:3001 -L 4566:127.0.0.1:4566 $(VPS_HOST)

tf-init: sync ## terraform init
	$(RUN) "cd $(VPS_DIR)/terraform && terraform init"

tf-apply: sync ## Cria buckets e filas no LocalStack
	$(RUN) "cd $(VPS_DIR)/terraform && terraform apply -auto-approve"

tf-destroy: ## Destrói buckets e filas
	$(RUN) "cd $(VPS_DIR)/terraform && terraform destroy -auto-approve"

gen-data: sync ## Gera os lotes TPC-H no bucket raw
	$(RUN) "$(PY) generator.py 0.05"

# --- Teste do pipeline sem Kubernetes ---

test-producer: sync ## Envia 100 msg/s por 5s
	$(RUN) "$(PY) producer.py --mode test --duration 5"

test-consumer: ## Roda o consumer por 15s num container (imagem gerada no make up)
	$(RUN) "docker run --rm --network scaling-lab_default -e AWS_ENDPOINT_URL=http://localstack:4566 \
		scaling-app:latest timeout -s TERM 15 python consumer.py || true"

test-pipeline: test-producer test-consumer ## Teste ponta a ponta (producer + consumer)

# --- Kubernetes (k3d + KEDA + Karpenter KWOK) ---

k8s-cluster: ## Cria o cluster k3d na rede do LocalStack
	$(RUN) "k3d cluster list | grep -q 'scaling-cluster' || k3d cluster create scaling-cluster --network scaling-lab_default --servers 1 --agents 2 --wait"

k8s-keda: ## Instala o KEDA
	$(RUN) "helm repo add kedacore https://kedacore.github.io/charts && helm repo update && helm upgrade --install keda kedacore/keda --namespace keda --create-namespace --wait"

k8s-karpenter: sync ## Instala Karpenter KWOK (imagem karpenter-kwok:latest já importada) e o NodePool
	$(RUN) "helm upgrade --install karpenter $(KARPENTER_REPO)/kwok/charts --namespace karpenter --create-namespace \
		--set controller.image.repository=karpenter-kwok --set controller.image.tag=latest --set settings.preferencePolicy=Ignore --wait \
		&& kubectl apply -f $(VPS_DIR)/k8s/karpenter-nodepool.yaml"

k8s-build: sync ## Builda a imagem e importa no k3d
	$(RUN) "docker build -t scaling-app:latest -f $(VPS_DIR)/docker/Dockerfile $(VPS_DIR) && k3d image import scaling-app:latest -c scaling-cluster"

k8s-deploy: k8s-build ## Aplica consumer + KEDA
	$(RUN) "kubectl apply -f $(VPS_DIR)/k8s/consumer-deployment.yaml -f $(VPS_DIR)/k8s/keda-scaledobject.yaml && kubectl rollout restart deployment worker"

k8s-token: sync ## Gera token read-only do exporter em secrets/k8s-token.txt
	$(RUN) "kubectl apply -f $(VPS_DIR)/k8s/exporter-rbac.yaml && mkdir -p $(VPS_DIR)/secrets \
		&& kubectl create token scaling-exporter --duration=8760h > $(VPS_DIR)/secrets/k8s-token.txt && chmod 644 $(VPS_DIR)/secrets/k8s-token.txt"

k8s-status: ## Nós, pods e HPA
	$(RUN) "kubectl get nodes -o wide; kubectl get pods -o wide; kubectl get scaledobject,hpa"

# --- Carga e observação ---

inject-rate: ## RATE msg/s por DURATION segundos
	$(RUN) "$(PY) producer.py --mode rate --rate $(RATE) --duration $(DURATION)"

inject-msgs: ## MSGS mensagens a RATE msg/s
	$(RUN) "$(PY) producer.py --mode count --messages $(MSGS) --rate $(RATE)"

test-autoscaling: sync ## Injeta MSGS e acompanha os pods por 18s
	$(RUN) "$(PY) producer.py --mode count --messages $(MSGS) --rate $(RATE)"
	$(RUN) "for i in \$$(seq 1 6); do sleep 3; echo \"--- Amostra \$$i ---\"; kubectl get pods -l app=worker --no-headers | wc -l | sed 's/^/Total pods: /'; kubectl get hpa keda-hpa-consumer-scaler --no-headers 2>/dev/null; done"

test-node-autoscaling: sync ## Injeta 2.500 msgs (500 msg/s por 5s) e acompanha os nós
	$(RUN) "$(PY) producer.py --mode rate --rate 500 --duration 5"
	$(RUN) "for i in \$$(seq 1 8); do sleep 3; echo \"--- Amostra \$$i ---\"; kubectl get nodes --no-headers; kubectl get nodeclaims --no-headers 2>/dev/null || true; done"

watch-scaling: ## Pods, HPA e nós em tempo real
	$(RUN) -t "watch -n 1 'kubectl get pods -l app=worker -o wide; echo; kubectl get hpa keda-hpa-consumer-scaler; echo; kubectl get nodes'"

karpenter-logs: ## Logs do Karpenter
	$(RUN) "kubectl logs -n karpenter -l app.kubernetes.io/name=karpenter -f"

logs-consumer: ## Logs de entrada/saída dos workers
	$(RUN) "kubectl logs -l app=worker -f --tail=20 --prefix=true --max-log-requests=40"

# --- Experimento ---

dia-fixo: sync ## Cenário A: frota fixa de 2 pods
	$(RUN) "$(PY) experiment.py --scenario fixo --seconds-per-hour $(SEC_PER_HOUR)"

dia-keda: sync ## Cenário B: KEDA 2-40 pods + Karpenter
	$(RUN) "$(PY) experiment.py --scenario keda --seconds-per-hour $(SEC_PER_HOUR)"

dia-grande: sync ## Cenário C: frota fixa de 40 pods
	$(RUN) "$(PY) experiment.py --scenario grande --seconds-per-hour $(SEC_PER_HOUR)"

fetch-results: ## Baixa results/*.csv da VPS
	mkdir -p ./results
	rsync -avz $(VPS_HOST):$(VPS_DIR)/results/ ./results/
