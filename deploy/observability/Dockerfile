# syntax=docker/dockerfile:1

FROM prom/prometheus:v3.5.0@sha256:63805ebb8d2b3920190daf1cb14a60871b16fd38bed42b857a3182bc621f4996

COPY deploy/observability/v1/prometheus.yml /etc/prometheus/prometheus.yml
COPY deploy/observability/v1/prometheus-alerts.yaml /etc/prometheus/rules/prometheus-alerts.yaml
