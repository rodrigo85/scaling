# Plano — Simulação de autoscaling de ingestão (protótipo LinkedIn)

## Objetivo

Demonstrar, com custo zero, uma arquitetura de ingestão em nuvem (AWS ou Azure) que
reage sozinha a um pico enorme de carga em um horário do dia:

- a fila enche, os consumers escalam (pods) e, quando falta capacidade, **novas máquinas
  (nós) são alocadas** — com logs reais do autoscaler;
- o backlog drena, a carga cai e a infra volta ao tamanho mínimo (scale-down / scale-to-zero);
- tudo descrito em Terraform, pronto para subir na nuvem real trocando só o endpoint.

Entregável final: vídeo/prints + gráfico comparativo + post.

## Decisões Consolidadas

1. **Nuvem simulada:** AWS (LocalStack Community + Karpenter com provider KWOK).
2. **Onde roda:** VPS `vpsmouts` (`/opt/scaling`, 8 vCPUs, 22 GiB de RAM livres).
3. **Custo:** **R$ 0,00 estrito** (sem nuvem paga; Fase 7 descartada; cálculo de economia via Infracost).
4. **Segurança:** Todas as portas de serviços e dashboards (Grafana, LocalStack, Prometheus) amarradas exclusivamente em `127.0.0.1`, acessadas via túnel SSH.

## Restrições

- **Somente dado sintético** (TPC-H via extensão `tpch` do DuckDB). Nunca usar dado nem
  bucket de cliente (`cdh-*`) em material público.
- Sem AWS/Azure real na fase de desenvolvimento (nem free tier).
- O post deve deixar claro o que é real e o que é simulado (ver "Limites").

## Arquitetura (versão AWS)

| Peça | Papel | Equivalente na nuvem |
|---|---|---|
| LocalStack | S3 (`raw`, `refined`) e SQS (fila de ingestão) | S3, SQS |
| Terraform + `tflocal` | cria buckets, fila e notificações | mesmo código na AWS real |
| k3d | cluster Kubernetes local (2–3 nós reais) | EKS |
| KEDA | escala consumers pelo tamanho da fila SQS | KEDA no EKS |
| Karpenter + provider KWOK | aloca/remove nós simulados quando há pods `Pending` | Karpenter no EKS |
| Producer (Python) | gera carga com perfil diário e pico | fontes reais |
| Consumer (Python/DuckDB) | lê arquivo do `raw`, grava Parquet/Delta no `refined` | job de ingestão |
| Prometheus + Grafana | métricas e dashboard | CloudWatch/Managed Grafana |
| Infracost | estima custo mensal do Terraform na AWS real | — |

Versão Azure: Azurite (Blob/Queue) ou emulador Service Bus, KEDA com scaler Azure Queue /
Service Bus, AKS Node Auto Provisioning (Karpenter) no lugar do Karpenter AWS.

### Modelo híbrido de nós (importante)

Nós KWOK são falsos: os pods agendados neles aparecem `Running`, mas não executam código.

- **Nós k3d reais** rodam os consumers de verdade e processam dados (baseline + parte do pico).
- **Nós KWOK** recebem o excedente do pico e mostram a decisão real do Karpenter de alocar
  máquina (`found provisionable pod(s)`, `computed new nodeclaim(s)`, `launched nodeclaim`,
  `registered nodeclaim`, depois `disrupting nodeclaim(s)` no scale-down).

Alternativa considerada: script próprio que observa pods `Pending` e roda `k3d node create`
(nó real, pod real), mas o log seria do script e não de um autoscaler de mercado.

## Perfil de carga — 1 dia comprimido em 24 minutos

1 hora simulada = `SEC_PER_HOUR` segundos (padrão 5s no Makefile). Taxa em mensagens/s
por hora (`HOURLY_RATES` em `src/common.py`, fonte única):

- base noturna: 80 msg/s;
- manhã/almoço: 200–280 msg/s;
- **pico de fechamento: 18h = 450, 19h = 500, 20h = 350 msg/s** (~6x a base), calibrado
  para ficar um pouco acima da capacidade da frota e formar backlog que drena logo depois.

Cada mensagem aponta para um arquivo no `raw` (gerado a partir do TPC-H).

## Experimento — mesmo dia, três cenários

| Cenário | Configuração | Resultado esperado |
|---|---|---|
| A | frota fixa pequena (2 pods) | backlog explode; dado das 18h só sai muito depois |
| B | KEDA 2→40 pods + Karpenter | backlog drena em minutos; nós sobem e descem |
| C | frota fixa grande (40 pods 24h) | aguenta o pico, mas paga capacidade ociosa o dia todo |

## Métricas (um painel Grafana)

- mensagens/s entrando × saindo;
- backlog da fila;
- pods ativos e nós ativos (reais + KWOK);
- latência ponta a ponta (horário do evento → gravação no `refined`);
- tempo para escalar e tempo para drenar o backlog;
- pod-minutos e nó-minutos de B × C → "mesmo SLA com N% menos compute", convertido em
  dólar via Infracost.

## Fases

1. **Base:** `docker-compose` com LocalStack, Prometheus e Grafana; Terraform criando
   buckets e fila; gerador TPC-H populando o `raw`.
2. **Pipeline:** producer com perfil de carga e consumer gravando no `refined`; validar
   ponta a ponta com 1 pod.
3. **Escala de pods:** cluster k3d + KEDA `ScaledObject` lendo a fila SQS.
4. **Escala de nós:** Karpenter com provider KWOK, `NodePool` com tipos de instância
   simulados e consolidação ligada.
5. **Experimento:** `make dia-fixo`, `make dia-keda`, `make dia-grande`; cada rodada salva
   métricas em CSV.
6. **Resultado:** gráfico comparativo, Infracost, gravação de tela (Grafana +
   `kubectl get nodes -w` + `kubectl logs -f` do Karpenter lado a lado no pico).
7. **Estimativa de Custo:** Infracost comparando os 3 cenários simulados (A, B e C),
   comprovando a economia sem necessidade de gastar em nuvem real.

Interface alvo: `make up`, `make load`, `make dia-<cenario>`, `make down`.

## Limites (declarar no post)

- Emuladores validam arquitetura, IaC e comportamento de autoscaling, não latência nem
  throughput reais da nuvem. Divulgar comportamento e proporções, não números absolutos.
- Escala de nós é simulada (KWOK): a decisão do Karpenter é real, as máquinas não.
- Throughput absoluto reflete a máquina local/VPS.

## Pré-checagens antes de começar

- Recursos livres onde vai rodar (`nproc`, `free -h`, `df -h`).
- Docker funcionando; k3d, kubectl, helm, terraform, tflocal instalados.
- Licença/plano atual do LocalStack (versões recentes podem exigir conta gratuita).
- Preços atuais de EKS/AKS e Spot, se a rodada real (fase 7) for feita.
