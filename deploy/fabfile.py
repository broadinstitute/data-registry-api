import os
from fabric import task


@task
def deploy(c, env, commit=None):
    """
    Deploy the application to the specified environment.

    Parameters:
        c: The connection context.
        env: The environment (e.g., 'dev', 'prd').
        commit: The commit to deploy. If not specified, the latest commit on main.
    """
    c.forward_agent = True
    update_source(c, env, commit)
    migrate(c, env)
    restart(c, env)


@task
def update_source(c, env, commit=None):
    """
    Update the environment's source code to either the latest in main or a specified commit.
    """
    directory = get_checkout_directory(env)
    with c.cd(directory):
        if commit:
            c.run("git fetch --all --tags")
            is_tag = c.run(f"git tag -l {commit}", warn=True).ok

            if is_tag:
                c.run(f"git checkout {commit}")
            else:
                c.run(f"git checkout {commit} && git pull")
        else:
            c.run("git checkout main && git pull")


@task
def restart(c, env):
    """
    Restart the server by restarting the systemd service.

    Note: Does not install dependencies. Run 'fab migrate' first when deploying new code.

    Parameters:
        c: The connection context.
        env: The environment (e.g., 'dev', 'prd').
    """
    service = "dr-api-dev" if env == 'dev' else "dr-api-prd"
    c.run(f"sudo systemctl restart {service}")


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
    deploy_dir = os.path.dirname(os.path.abspath(__file__))
    c.put(os.path.join(deploy_dir, f"{service}.service"), f"/tmp/{service}.service")
    c.run(f"sudo mv /tmp/{service}.service /etc/systemd/system/{service}.service")
    c.run("sudo systemctl daemon-reload")
    c.run(f"sudo systemctl enable {service}")
    c.run(f"sudo systemctl start {service}")


def get_checkout_directory(env):
    return "/home/ec2-user/data-registry-api-qa" if env == 'dev' else "/home/ec2-user/data-registry-api-prd"


@task
def migrate(c, env):
    """
    Run db migrations.
    """
    directory = get_checkout_directory(env)
    with c.cd(directory):
        db = "dataregistry_qa" if env == 'dev' else "dataregistry"
        c.run("python3 -m pip install -r requirements.txt")
        c.run(f"export DATA_REGISTRY_DB_NAME={db}; python3 -m alembic upgrade head")
