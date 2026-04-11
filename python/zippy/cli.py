"""
Python CLI entrypoint for zippy management commands.
"""

from __future__ import annotations

import click

from .cli_master import master_group
from .cli_stream import stream_group


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
def main() -> None:
    """
    zippy management CLI.
    """


main.add_command(master_group)
main.add_command(stream_group)


if __name__ == "__main__":
    main()
