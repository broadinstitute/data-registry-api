from fabric import task


@task
def deploy(c, env, commit):
    if env == 'prod':
        c.run("echo 'Deploying to prod!'")
        c.run("cd ~/data-registry-api-prd")
    else:
        c.run("echo 'Deploying to dev!'")
        c.run("cd ~/data-registry-api-qa")
    if commit:
        c.run(f"git checkout {commit}")
    else:
        c.run("git pull")


@task
def restart(c, env):
    """
    Restart the server by terminating existing screen sessions and starting a new session.

    Parameters:
        c: The connection context.
        env: The environment (e.g., 'dev', 'prd').
    """
    directory = "/home/ec2-user/data-registry-api-qa" if env == 'dev' else "/home/ec2-user/data-registry-api-prd"
    screen_session = "dr-api-dev" if env == 'dev' else "dr-api-prd"
    port = 8000 if env == 'dev' else 443

    with c.cd(directory):
        # terminate running screen sessions
        c.run(f"screen -ls | grep -o '[0-9]*\.{screen_session}' | while read -r line; do screen -S \"${{line}}\" -X quit; done")
        c.run(
            f"screen -dmS {screen_session} bash -c 'python3.8 -m dataregistry.main -e .env serve --port {port}'")
