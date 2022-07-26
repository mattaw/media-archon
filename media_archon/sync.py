# SPDX-FileCopyrightText: 2022-present Matthew Swabey <matthew@swabey.org>
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import logging
from concurrent.futures import Future, ThreadPoolExecutor
from logging import getLogger
from pathlib import Path
from queue import Queue
from typing import Callable, Iterable, Set

from attrs import define, evolve

logging.basicConfig(
    format="%(asctime)s - %(threadName)s - %(message)s", level=logging.DEBUG
)
logger = getLogger(__name__)

# Note - in python if we a=b where b is anything other than str, int + a few more types
#  a=b just copies a reference. Ruthlessly exploit this to pass around the same
#  thread pools and the queue etc.
# There is a light ThreadPoolExecutor to manage the overlapping IO and directory
# exploration, and any copying that is needed. Thinking cpu_count() * 100?
# Heavy threadpool should be cpu_count * 2


class SyncWalkerException(Exception):
    pass


@define
class SyncWalker:
    src: Path
    tgt: Path
    pool: ThreadPoolExecutor
    results: Queue[Future]
    convert: Callable[[Iterable[Path]], None]

    def _start(self):
        self.results.put(self.pool.submit(SyncWalker._walk_thread, self))

    @staticmethod
    def start(sync_walker: "SyncWalker") -> None:
        sync_walker._start()

    def _walk(self) -> None:
        # Run this in a thread using staticmethod and passing a new instance
        # Sanity check ourselves
        if not self.src.exists():
            raise SyncWalkerException(
                f"Source directory {self.src.absolute()} does not exist."
            )
        if not self.src.is_dir():
            raise SyncWalkerException(
                f"Source {self.src.absolute()} is not a directory."
            )

        # Create target dir if it doesn't exist
        try:
            self.tgt.mkdir(exist_ok=True)
        except FileExistsError as e:
            raise SyncWalkerException() from e

        files: Set[Path] = set()
        for item in self.src.iterdir():
            # Handle the different things we care about here
            logger.debug("item %s", str(item))
            if item.is_dir():
                # Submit a new worker to work on subdir
                new_syncwalker = evolve(
                    self, src=self.src / item.name, tgt=self.tgt / item.name
                )
                new_syncwalker._start()

            if item.is_file():
                files.add(item)

        if files:
            # We found files, but we don't care what they are
            self.convert(files, self.tgt)

    @staticmethod
    def _walk_thread(sync_walker: "SyncWalker") -> None:
        sync_walker._walk()
