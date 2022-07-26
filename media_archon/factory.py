# SPDX-FileCopyrightText: 2022-present Matthew Swabey <matthew@swabey.org>
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import os
from concurrent.futures import ThreadPoolExecutor, wait
from logging import getLogger
from pathlib import Path
from queue import Queue
from typing import TextIO

from attrs import define

from .sync import SyncWalker

logger = getLogger(__name__)


def convert(foo, bar):
    pass


@define
class Factory:
    @classmethod
    def from_toml(cls, file: TextIO) -> None:

        light_pool = ThreadPoolExecutor(
            max_workers=os.cpu_count() * 10 + 10,
            thread_name_prefix="light"
            # max_workers=2
        )
        heavy_pool = ThreadPoolExecutor(
            max_workers=os.cpu_count() * 2, thread_name_prefix="heavy"
        )
        results = Queue(maxsize=os.cpu_count() * 20)

        initial_walker = SyncWalker(
            src=Path("."),
            tgt=Path("."),
            pool=light_pool,
            results=results,
            convert=convert,
        )

        SyncWalker.start(initial_walker)

        # Drain the results queue waiting for them to be done
        #  Note shutdown wait=True doesn't work as we haven't submitted
        #  all the jobs before calling shutdown!
        ts = set()
        ts.add(results.get())
        while ts:
            logger.debug("waiting %d", len(ts))
            wait(ts)
            ts = set()
            while not results.empty():
                ts.add(results.get())

        heavy_pool.shutdown(wait=True, cancel_futures=False)
        light_pool.shutdown(wait=True, cancel_futures=False)

        file.close()
