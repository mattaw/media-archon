# SPDX-FileCopyrightText: 2022-present Matthew Swabey <matthew@swabey.org>
#
# SPDX-License-Identifier: AGPL-3.0-or-later

from pathlib import Path

import click

from .walker import WalkerFactory

CONFIG_FILE = "media-archon.toml"


def show_help_and_exit() -> None:
    ctx = click.get_current_context()
    click.echo(ctx.get_help())
    exit(-1)


@click.command()
@click.option(
    "-c",
    "--config",
    type=click.Path(readable=False),
    show_default=True,
    default=Path(CONFIG_FILE),
    help="Specify a config file.",
)
@click.version_option()
def main(config: Path) -> None:
    try:
        factory = WalkerFactory.from_toml(config)
    except FileNotFoundError:
        click.echo(f"Could not find configuration file {CONFIG_FILE}.\n")
        show_help_and_exit()
    except PermissionError:
        click.echo(f"Could not read configuration file {CONFIG_FILE}.\n")
        show_help_and_exit()

    factory.build_and_run()

    return
