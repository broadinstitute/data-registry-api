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
            c.run(f"git fetch && git checkout {commit}")
        else:
            c.run("git checkout main && git pull")


@task
def restart(c, env):
    """
    Restart the server by terminating existing screen sessions and starting a new session.

    Parameters:
        c: The connection context.
        env: The environment (e.g., 'dev', 'prd').
    """
    directory = get_checkout_directory(env)
    screen_session = "dr-api-dev" if env == 'dev' else "dr-api-prd"
    port = 8000 if env == 'dev' else 443

    with c.cd(directory):
        # terminate running screen sessions
        c.run(
            f"screen -ls | grep -o '[0-9]*\.{screen_session}' | while read -r line; do screen -S \"${{line}}\" -X quit; done")
        c.run("python3.8 -m pip install -r requirements.txt")
        c.run(
            f"screen -dmS {screen_session} bash -c 'python3.8 -m dataregistry.main -e .env serve --port {port}'")


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
        c.run("python3.8 -m pip install -r requirements.txt")
        c.run(f"export DATA_REGISTRY_DB_NAME={db}; python3.8 -m alembic upgrade head")
