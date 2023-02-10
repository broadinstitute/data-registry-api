import click
import uvicorn


@click.group()
@click.option('--env-file', '-e', type=str)
@click.pass_context
def cli(ctx, env_file):
    if env_file:
        print("env files not yet supported")


@click.command(name='serve')
@click.option('--port', '-p', type=int, default=5000)
def cli_serve(port):
    uvicorn.run(
        'dataregistry.server:app',
        host='0.0.0.0',
        port=port,
        log_level='info',
    )


cli.add_command(cli_serve)


def main():
    cli()


if __name__ == '__main__':
    main()