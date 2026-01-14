import os

import click
import uvicorn
from dotenv import load_dotenv


@click.group()
@click.option('--env-file', '-e', type=str)
@click.pass_context
def cli(ctx, env_file):
    if env_file:
        load_dotenv(env_file)


@click.command(name='serve')
@click.option('--port', '-p', type=int, default=5000)
def cli_serve(port):
    defaultpy37 = "DEFAULT:!aNULL:!eNULL:!MD5:!3DES:!DES:!RC4:!IDEA:!SEED:!aDSS:!SRP:!PSK"
    os.environ['UVICORN_LIMIT_MAX_REQUEST_SIZE'] = str(3 * 1024 * 1024 * 1024)
    uvicorn.run(
        'dataregistry.server:app',
        host='0.0.0.0',
        port=port,
        log_level='info',
        ssl_certfile='/home/ec2-user/ssl/fullchain.pem' if os.getenv('USE_HTTPS') == 'true' else None,
        ssl_keyfile='/home/ec2-user/ssl/key.pem' if os.getenv('USE_HTTPS') == 'true' else None,
        ssl_ciphers=defaultpy37 if os.getenv('USE_HTTPS') == 'true' else None
    )


cli.add_command(cli_serve)


def main():
    cli()


if __name__ == '__main__':
    main()
