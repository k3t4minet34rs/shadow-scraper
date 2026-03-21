hi
poetry run python -m shadow.main
ssh -L 8428:localhost:8428 admin@<your-ec2-ip> -N

poetry run shadow-scraper
poetry run shadow-executor

~/.local/bin/poetry run weather-cycle --dry-run --db data/weather.db

~/.local/bin/poetry run weather-cycle --budget 200 --db data/weather.db

# 1. Copy service + timer files
sudo cp deploy/weather-cycle.service /etc/systemd/system/
sudo cp deploy/weather-cycle.timer   /etc/systemd/system/

# 2. Generate .env.service (strips "export " prefix from .env)
grep -v '^#' .env | sed 's/^export //' > .env.service

# 3. Reload and enable
sudo systemctl daemon-reload
sudo systemctl enable --now weather-cycle.timer

# 4. Verify timer is scheduled
systemctl list-timers weather-cycle.timer

# 5. Test a manual run right now
sudo systemctl start weather-cycle.service
journalctl -u weather-cycle -f

