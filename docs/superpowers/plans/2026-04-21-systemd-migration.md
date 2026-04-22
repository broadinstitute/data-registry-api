# Systemd Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace screen-based process management with systemd services for the `dev` and `prd` environments of `data-registry-api`.

**Architecture:** Add two systemd service files to `deploy/`, update `fab restart` to call `systemctl restart` instead of managing screen sessions, and add a one-time `fab setup` task that installs and enables the service on the server. GH Actions is unchanged.

**Tech Stack:** Python/Fabric 2, systemd, Amazon Linux EC2 (`ec2-user`, passwordless sudo via `wheel` group)

**Spec:** `docs/superpowers/specs/2026-04-21-systemd-migration-design.md`

---

### Task 1: Add systemd service files

**Files:**
- Create: `deploy/dr-api-dev.service`
- Create: `deploy/dr-api-prd.service`

- [ ] **Step 1: Create `deploy/dr-api-dev.service`**

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

- [ ] **Step 2: Create `deploy/dr-api-prd.service`**

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

- [ ] **Step 3: Verify files exist and look correct**

```bash
cat deploy/dr-api-dev.service
cat deploy/dr-api-prd.service
```

Expected: Both files display with the correct `WorkingDirectory` and `ExecStart` port.

- [ ] **Step 4: Commit**

```bash
git add deploy/dr-api-dev.service deploy/dr-api-prd.service
git commit -m "feat(deploy): add systemd service files for dev and prd"
```

---

### Task 2: Update fabfile

**Files:**
- Modify: `deploy/fabfile.py`

Current `restart` task (lines 40–58) kills screen sessions, runs `pip install`, and starts a new screen. Replace it entirely. Add new `setup` task after `restart`.

- [ ] **Step 1: Replace the `restart` task**

In `deploy/fabfile.py`, replace the entire `restart` function with:

```python
@task
def restart(c, env):
    """
    Restart the server by restarting the systemd service.

    Parameters:
        c: The connection context.
        env: The environment (e.g., 'dev', 'prd').
    """
    service = "dr-api-dev" if env == 'dev' else "dr-api-prd"
    c.run(f"sudo systemctl restart {service}")
```

- [ ] **Step 2: Add the `setup` task**

Add this function after `restart` in `deploy/fabfile.py`:

```python
@task
def setup(c, env):
    """
    Install and enable the systemd service for the specified environment.
    Run once to set up the service on the server. Subsequent deploys use
    the restart task.

    Parameters:
        c: The connection context.
        env: The environment (e.g., 'dev', 'prd').
    """
    service = "dr-api-dev" if env == 'dev' else "dr-api-prd"
    c.put(f"deploy/{service}.service", f"/tmp/{service}.service")
    c.run(f"sudo mv /tmp/{service}.service /etc/systemd/system/{service}.service")
    c.run("sudo systemctl daemon-reload")
    c.run(f"sudo systemctl enable {service}")
    c.run(f"sudo systemctl start {service}")
```

- [ ] **Step 3: Verify fabric sees both tasks**

```bash
cd deploy && fab --list
```

Expected output includes `restart` and `setup` in the task list.

- [ ] **Step 4: Commit**

```bash
git add deploy/fabfile.py
git commit -m "feat(deploy): migrate restart to systemd, add setup task"
```

---

### Task 3: Cutover dev

Perform the one-time migration of the `dev` environment from screen to systemd. Do this from the repo root on your local machine (fabric SSHes in).

- [ ] **Step 1: Deploy updated code to dev**

Pull the latest code and run migrations without triggering a restart (the service doesn't exist yet). We call `update-source` and `migrate` individually rather than `fab deploy` to avoid the `restart` step until after `setup` runs.

```bash
cd deploy
fab update-source --env=dev -H ec2-user@<DEPLOY_HOST>
fab migrate --env=dev -H ec2-user@<DEPLOY_HOST>
```

Replace `<DEPLOY_HOST>` with the value of the `DEPLOY_HOST` secret (or ask a teammate).

- [ ] **Step 2: Install the systemd service**

```bash
fab setup --env=dev -H ec2-user@<DEPLOY_HOST>
```

Expected: No errors. The service is now running under systemd.

- [ ] **Step 3: Verify the service is running**

SSH into the server and run:

```bash
sudo systemctl status dr-api-dev
```

Expected: `Active: active (running)` with the correct `ExecStart` command shown.

- [ ] **Step 4: Verify the endpoint responds**

From the server or locally (if port 8000 is accessible):

```bash
curl -s -o /dev/null -w "%{http_code}" http://localhost:8000/
```

Expected: Any non-5xx response (200, 404, 401 are all fine — just not a connection refused).

- [ ] **Step 5: Kill the old screen session**

SSH into the server and run:

```bash
screen -ls | grep dr-api-dev
```

If a session appears, kill it:

```bash
screen -S <id>.dr-api-dev -X quit
```

Replace `<id>` with the numeric prefix shown in `screen -ls` output. If no session appears, skip this step.

- [ ] **Step 6: Verify service still running after screen kill**

```bash
sudo systemctl status dr-api-dev
```

Expected: Still `active (running)`. systemd owns the process now.

---

### Task 4: Cutover prd

Identical to Task 3 but for the `prd` environment. Only proceed after Task 3 is verified healthy.

- [ ] **Step 1: Deploy updated code to prd**

```bash
cd deploy
fab update-source --env=prd -H ec2-user@<DEPLOY_HOST>
fab migrate --env=prd -H ec2-user@<DEPLOY_HOST>
```

- [ ] **Step 2: Install the systemd service**

```bash
fab setup --env=prd -H ec2-user@<DEPLOY_HOST>
```

Expected: No errors.

- [ ] **Step 3: Verify the service is running**

SSH into the server and run:

```bash
sudo systemctl status dr-api-prd
```

Expected: `Active: active (running)`.

- [ ] **Step 4: Verify the endpoint responds**

```bash
curl -s -o /dev/null -w "%{http_code}" https://localhost/
```

Expected: Any non-5xx response.

- [ ] **Step 5: Kill the old screen session**

```bash
screen -ls | grep dr-api-prd
```

If a session appears:

```bash
screen -S <id>.dr-api-prd -X quit
```

- [ ] **Step 6: Verify service still running after screen kill**

```bash
sudo systemctl status dr-api-prd
```

Expected: Still `active (running)`.

---

### Task 5: Verify GH Actions deploy still works

Trigger a deploy through the normal pipeline and confirm it uses systemd restart end-to-end.

- [ ] **Step 1: Push a trivial commit to `main`**

```bash
git checkout main
git pull
# make a no-op change, e.g., add a blank line to README then remove it
git commit --allow-empty -m "chore: verify systemd deploy pipeline"
git push origin main
```

- [ ] **Step 2: Watch the GH Actions run**

Go to the Actions tab on GitHub. Confirm `deploy-qa` job completes without errors.

- [ ] **Step 3: Verify dev service restarted via systemd**

SSH into the server:

```bash
sudo journalctl -u dr-api-dev -n 20
```

Expected: Log entries showing the service was stopped and started during the deploy.
