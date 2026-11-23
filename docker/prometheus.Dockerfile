FROM prom/prometheus:v3.5.1@sha256:38c3b05c3bc744ff1b0b7b4eb82196026442845e62a1e2073795565da506d7a2
COPY --chmod=644 monitoring/prometheus.yml monitoring/cdp-alerts.yml /etc/prometheus/
