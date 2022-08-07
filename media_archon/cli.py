# SPDX-FileCopyrightText: 2022-present Matthew Swabey <matthew@swabey.org>
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import logging
from pathlib import Path
from typing import Optional

import click

from .walker import CONFIG_FILE_NAME, Walker

logging.basicConfig(
    format="%(asctime)s %(threadName)-10s %(levelname)-7s %(message)s",
    level=logging.INFO,
)


def show_help_and_exit() -> None:
    ctx = click.get_current_context()
    click.echo(ctx.get_help())
    exit(-1)


@click.command()
@click.option(
    "-c",
    "--config",
    type=click.Path(exists=False),
    help="Specify a config file.",
)
@click.argument(
    "src", type=click.Path(exists=True, file_okay=False, dir_okay=True, readable=True)
)
@click.version_option()
def main(config: Optional[Path], src: Path) -> None:
    try:
        ranger = Walker.from_toml(src_dir=src, config_path=config)
    except FileNotFoundError:
        click.echo(f"Could not find configuration file {CONFIG_FILE_NAME}.\n")
        show_help_and_exit()
    except PermissionError:
        click.echo(f"Could not read configuration file {CONFIG_FILE_NAME}.\n")
        show_help_and_exit()

    ranger.build_and_run()
    click.echo("Done.")
    return
