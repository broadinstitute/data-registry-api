# Python 3.12 + Venv Migration Design

**Date:** 2026-04-22
**Status:** Approved

## Overview

Migrate `data-registry-api` from Python 3.9 (bare pip install) to Python 3.12 with per-environment virtual environments. Python 3.12.x is already available on the server at `/usr/bin/python3.12` (installed via `sudo dnf install python3.12` on Amazon Linux 2023).

## Current State

- Python 3.9.20 (system), no venvs
- `fab migrate` runs bare `python3 -m pip install -r requirements.txt`
- Service files use `ExecStart=/usr/bin/python3 ...`
- CI tests against Python 3.9

## Target State

- Python 3.12 venvs at `{checkout_dir}/venv` for each environment
- `fab setup` creates the venv, pip installs, installs service file, restarts service
- `fab migrate` uses venv pip and python for all commands
- Service files `ExecStart` points at venv python
- CI tests against Python 3.12

## Fabfile Changes

### New helper

```python
def get_venv_dir(env):
    return get_checkout_directory(env) + "/venv"
```

### `setup` task (updated)

Venv creation and pip install added before the service file upload:

```python
@task
def setup(c, env):
    """
    Install and enable the systemd service for the specified environment.
    Run once to set up the service on the server, or again to update the
    service file. Creates the Python 3.12 venv and installs dependencies.

    Parameters:
        c: The connection context.
        env: The environment (e.g., 'dev', 'prd').
    """
    checkout_dir = get_checkout_directory(env)
    venv_dir = get_venv_dir(env)
    service = "dr-api-dev" if env == 'dev' else "dr-api-prd"

    # Create venv and install dependencies
    c.run(f"/usr/bin/python3.12 -m venv --clear {venv_dir}")
    with c.cd(checkout_dir):
        c.run(f"{venv_dir}/bin/pip install -r requirements.txt")

    # Install service file
    deploy_dir = os.path.dirname(os.path.abspath(__file__))
    c.put(os.path.join(deploy_dir, f"{service}.service"), f"/tmp/{service}.service")
    c.run(f"sudo mv /tmp/{service}.service /etc/systemd/system/{service}.service")
    c.run("sudo systemctl daemon-reload")
    c.run(f"sudo systemctl enable {service}")
    c.run(f"sudo systemctl restart {service}")
```

### `migrate` task (updated)

Switches from bare `python3` to venv binaries:

```python
@task
def migrate(c, env):
    """
    Run db migrations.
    """
    directory = get_checkout_directory(env)
    venv_dir = get_venv_dir(env)
    with c.cd(directory):
        db = "dataregistry_qa" if env == 'dev' else "dataregistry"
        c.run(f"{venv_dir}/bin/pip install -r requirements.txt")
        c.run(f"export DATA_REGISTRY_DB_NAME={db}; {venv_dir}/bin/python -m alembic upgrade head")
```

## Service File Changes

`ExecStart` updated to use venv python in both service files.

### `deploy/dr-api-dev.service`

```ini
[Unit]
Description=Data Registry API (dev)
After=network.target

[Service]
User=ec2-user
WorkingDirectory=/home/ec2-user/data-registry-api-qa
ExecStart=/home/ec2-user/data-registry-api-qa/venv/bin/python -m dataregistry.main -e .env serve --port 8000
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
ExecStart=/home/ec2-user/data-registry-api-prd/venv/bin/python -m dataregistry.main -e .env serve --port 443
Restart=on-failure
RestartSec=5
NoNewPrivileges=true
AmbientCapabilities=CAP_NET_BIND_SERVICE
CapabilityBoundingSet=CAP_NET_BIND_SERVICE

[Install]
WantedBy=multi-user.target
```

Note: `AmbientCapabilities` replaces the old `setcap CAP_NET_BIND_SERVICE` on `/usr/bin/python3`. This attaches the capability grant to the service unit rather than the binary, so it survives venv recreation. `NoNewPrivileges=true` prevents child processes from gaining additional capabilities.

## CI Change

In `.github/workflows/continuous-integration.yml`:

```yaml
python-version: ["3.12"]
```

The deploy action (`.github/actions/deploy/action.yml`) uses `python-version: '3.8'` only to run fabric — unchanged.

## Cutover Sequence

Run for `dev` first, verify, then repeat for `prd`:

```bash
cd deploy
fab setup --env=dev -H ec2-user@<DEPLOY_HOST>
```

Verify on the server:

```bash
sudo systemctl status dr-api-dev
/home/ec2-user/data-registry-api-qa/venv/bin/python --version
# expected: Python 3.12.x
```

## Out of Scope

- Removing pyenv (leave as-is, it is not causing harm)
- Upgrading other services on the same host
