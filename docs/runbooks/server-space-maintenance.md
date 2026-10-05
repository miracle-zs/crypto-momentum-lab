# Server space maintenance

`cml-housekeeping.timer` bounds host-only operational artefacts once per day.
It never deletes PostgreSQL data or `/var/lib/crypto-momentum-lab/table-archive`.

The defaults retain seven days of crash-log archives, the current plus two
newest application images, build cache used in the last seven days, and 300 MB
of systemd journal history.  Every running container image is retained even if
it is older than the configured rollback set.

Install or refresh the units after pulling a release:

```bash
install -D -m 0755 deploy/ops/cml_housekeeping.sh \
  /opt/crypto-momentum-lab/deploy/ops/cml_housekeeping.sh
install -D -m 0644 deploy/ops/cml-housekeeping.service \
  /etc/systemd/system/cml-housekeeping.service
install -D -m 0644 deploy/ops/cml-housekeeping.timer \
  /etc/systemd/system/cml-housekeeping.timer
systemctl daemon-reload
systemctl enable --now cml-housekeeping.timer
```

To run it on demand and inspect its next run:

```bash
systemctl start cml-housekeeping.service
systemctl list-timers cml-housekeeping.timer
```

Override retention without editing tracked files through the service manager,
for example `CML_CRASH_LOG_RETENTION_DAYS=14`.  Keep the image-retention count
at three or above unless rapid rollback is intentionally unavailable.
