#!/usr/bin/env bash

echo "[1] kill background jobs"
jobs -p | xargs -r kill

echo "[2] kill existing kubectl port-forward processes"
pkill -f "kubectl.*port-forward"

# 혹시 남아있는거 대비 (강제)
sleep 1
pkill -9 -f "kubectl.*port-forward"

echo "[3] wait for ports cleanup"
sleep 2

echo "[4] start port-forward"

# Prometheus
kubectl -n observability port-forward \
  --address 0.0.0.0 \
  svc/knative-kube-prometheus-st-prometheus 9090:9090 \
  > /dev/null 2>&1 &

# Jaeger
kubectl -n observability port-forward \
  --address 0.0.0.0 \
  svc/jaeger 16686:16686 \
  > /dev/null 2>&1 &

echo "[done] port-forward restarted cleanly"
