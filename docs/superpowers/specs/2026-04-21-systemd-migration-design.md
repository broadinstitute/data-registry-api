# Systemd Migration Design

**Date:** 2026-04-21  
**Status:** Approved

## Overview

Replace the current screen-based process management with systemd services for both the `dev` and `prd` environments of `data-registry-api`. The GH Actions deploy pipeline is unchanged; only the fabric `restart` task and server process ownership change.

## Current State

| | dev | prd |
|---|---|---|
| Port | 8000 | 443 |
| Checkout dir | `/home/ec2-user/data-registry-api-qa` | `/home/ec2-user/data-registry-api-prd` |
| Screen session | `dr-api-dev` | `dr-api-prd` |
| Start command | `python3 -m dataregistry.main -e .env serve --port {port}` | same |
| Process user | `ec2-user` | `ec2-user` |

Port 443 on prd is enabled via `setcap CAP_NET_BIND_SERVICE` on the python3 binary, which carries over unchanged.

## Target State

- Two systemd service files (`dr-api-dev.service`, `dr-api-prd.service`) committed to `deploy/`
- `fab restart` calls `sudo systemctl restart <service>` instead of killing/creating screen sessions
- New `fab setup` task installs and enables a service on the server (one-time per env)
- GH Actions workflow is unchanged

## Service Files

### `deploy/dr-api-dev.service`

```ini
[Unit]
Description=Data Registry API (dev)
After=network.target

[Service]
User=ec2-user
WorkingDirectory=/home/ec2-user/data-registry-api-qa
ExecStart=/usr/bin/python3 -m dataregistry.main -e .env serve --port 8000
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### `deploy/dr-api-prd.service`

```ini
[Unit]
Description=Data Registry API (prd)
After=network.target

[Service]
User=ec2-user
WorkingDirectory=/home/ec2-user/data-registry-api-prd
ExecStart=/usr/bin/python3 -m dataregistry.main -e .env serve --port 443
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
```

## Fabfile Changes

### `restart` task (updated)

Replaces screen kill/create with a single systemctl call:

```python
@task
def restart(c, env):
    service = "dr-api-dev" if env == 'dev' else "dr-api-prd"
    c.run(f"sudo systemctl restart {service}")
```

### `setup` task (new)

Copies the service file from the repo, installs it, and enables + starts the service. Run once per environment on initial server setup:

```python
@task
def setup(c, env):
    """
    Install and enable the systemd service for the specified environment.
    Run once to set up the service on the server.
    """
    service = "dr-api-dev" if env == 'dev' else "dr-api-prd"
    c.put(f"deploy/{service}.service", f"/tmp/{service}.service")
    c.run(f"sudo mv /tmp/{service}.service /etc/systemd/system/{service}.service")
    c.run("sudo systemctl daemon-reload")
    c.run(f"sudo systemctl enable {service}")
    c.run(f"sudo systemctl start {service}")
```

`ec2-user` has passwordless sudo via the `wheel` group (standard on Amazon Linux EC2), so no sudoers changes are needed.

## GH Actions

No changes required. The pipeline calls `fab deploy`, which calls `restart`. Once `restart` is updated, deploys automatically use `systemctl`.

## Cutover Sequence

Perform for `dev` first, verify, then repeat for `prd`:

1. Deploy updated code (includes new service files and updated fabfile):
   ```bash
   fab deploy --env=dev -H ec2-user@<host>
   ```
2. Install the systemd service (one-time):
   ```bash
   fab setup --env=dev -H ec2-user@<host>
   ```
3. Kill the orphaned screen session manually:
   ```bash
   screen -ls | grep dr-api-dev
   screen -S <id>.dr-api-dev -X quit
   ```

After step 2, systemd owns the process. Subsequent `fab deploy` calls will use `systemctl restart`.

## Out of Scope

- Python 3.12 upgrade (follow-on task)
- Virtual environment setup (follow-on task, bundled with Python 3.12)
- Switching from fabric to `appleboy/ssh-action`
