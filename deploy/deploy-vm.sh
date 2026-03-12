#!/usr/bin/env bash
# deploy-vm.sh — install VictoriaMetrics on EC2 (run once)
#
# Usage (from your Mac):
#   EC2_HOST=ubuntu@<ip> ./deploy/deploy-vm.sh
#
# After this, the executor's metrics push (localhost:8428) will just work.
# Point Grafana Cloud remote_write or a local Grafana to http://<ec2-ip>:8428

set -euo pipefail

EC2_HOST="${EC2_HOST:-ubuntu@YOUR_EC2_IP}"
VM_VERSION="v1.115.0"          # https://github.com/VictoriaMetrics/VictoriaMetrics/releases
ARCH="linux-amd64"

[[ "$EC2_HOST" == *"YOUR_EC2_IP"* ]] && { echo "Set EC2_HOST"; exit 1; }

echo "[vm] Installing VictoriaMetrics $VM_VERSION on $EC2_HOST …"

ssh "$EC2_HOST" bash <<REMOTE
set -euo pipefail

VM_URL="https://github.com/VictoriaMetrics/VictoriaMetrics/releases/download/${VM_VERSION}/victoria-metrics-${ARCH}-${VM_VERSION}.tar.gz"

echo "[vm] Downloading …"
wget -q -O /tmp/vm.tar.gz "\$VM_URL"
tar -xzf /tmp/vm.tar.gz -C /tmp/
mv /tmp/victoria-metrics-prod /home/ubuntu/victoria-metrics-prod
chmod +x /home/ubuntu/victoria-metrics-prod

mkdir -p /home/ubuntu/vm-data

echo "[vm] Installing systemd unit …"
sudo cp /home/ubuntu/shadow-bot/deploy/victoria-metrics.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable victoria-metrics
sudo systemctl restart victoria-metrics

sleep 2
sudo systemctl status victoria-metrics --no-pager | head -15

echo ""
echo "[vm] Done. VictoriaMetrics listening on :8428"
echo "     Push URL:  http://localhost:8428/api/v1/import/prometheus"
echo "     Query UI:  http://\$(curl -s ifconfig.me):8428/vmui"
REMOTE

echo ""
echo "To open the metrics UI, add port 8428 to your EC2 security group (your IP only)."
echo "Or use an SSH tunnel:  ssh -L 8428:localhost:8428 $EC2_HOST"
