# SPDX-FileCopyrightText: 2022-present Matthew Swabey <matthew@swabey.org>
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import logging
import os
import shutil
from concurrent.futures import FIRST_EXCEPTION, Future, ThreadPoolExecutor, wait
from logging import getLogger
from pathlib import Path
from queue import Queue
from typing import Dict, Set, Union, List

import tomli
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
    light_pool: ThreadPoolExecutor
    heavy_pool: ThreadPoolExecutor  # Smaller number of threads for conversion
    results: Queue[Future]
    converter_cfg: Dict[
        str, Union[str, int, List[str], Dict[str, Union[str, int, List[str]]]]
    ]
    copier_cfg: Dict[
        str, Union[str, int, List[str], Dict[str, Union[str, int, List[str]]]]
    ]

    def __del__(self):
        self.light_pool.shutdown(wait=True)
        self.heavy_pool.shutdown(wait=True)

    def _walk(self) -> None:
        # Sanity check src
        logger.debug("Walking '%s'", str(self.src))
        if not self.src.exists():
            raise SyncWalkerException(f"Source directory {self.src} does not exist.")
        if not self.src.is_dir():
            raise SyncWalkerException(
                f"Source {self.src.absolute()} is not a directory."
            )

        # Create target dir (deleting files unexpectedly present)
        if not self.tgt.exists():
            logger.debug("  Creating dir %s", str(self.tgt))
            self.tgt.mkdir(exist_ok=True)
        elif not self.tgt.is_dir():
            logger.debug("  Replacing with dir %s", str(self.tgt))
            self.tgt.unlink()
            self.tgt.mkdir()

        files = set(self.src.iterdir())
        config_update_path = self.src / str(self.converter_cfg["config_update"])
        if config_update_path in files:
            logger.debug("  Detected updated config %s", str(config_update_path))
            files.remove(config_update_path)  # Do not process it
            # TODO update conversion settings for new walker
            syncwalker = self
        else:
            syncwalker = self

        for item in files:
            # Handle the different things we care about here

            if item.is_dir():
                # Submit a new worker to work on subdir
                new_syncwalker = evolve(
                    syncwalker, src=self.src / item.name, tgt=self.tgt / item.name
                )
                new_syncwalker.start()

            elif item.is_file():
                if item.suffix in set(self.copier_cfg["inputs"]):
                    tgt = self.tgt / item.name
                    self._copy(item, tgt)

    def start(self):
        self.results.put(self.light_pool.submit(SyncWalker._walk_thread, self))

    @staticmethod
    def _walk_thread(sync_walker: "SyncWalker") -> None:
        sync_walker._walk()

    @staticmethod
    def start_thread(sync_walker: "SyncWalker") -> None:
        sync_walker.start()

    def _copy(self, src: Path, tgt: Path):
        self.results.put(self.light_pool.submit(self._copy_thread, src=src, tgt=tgt))

    @staticmethod
    def _copy_thread(src: Path, tgt: Path):
        logger.debug("Copy called %s -> %s", src, tgt)
        exists = tgt.exists()
        is_file = tgt.is_file()
        if exists and not is_file:
            logger.debug("    Unlinking non-file '%s'", str(tgt))
            shutil.rmtree(tgt, ignore_errors=True)
        elif is_file:
            src_mtime = src.stat().st_mtime_ns
            tgt_mtime = tgt.stat().st_mtime_ns
            if src_mtime >= tgt_mtime:
                logger.debug("    Copying newer '%s' to '%s'", str(src), str(tgt))
                shutil.copy(src, tgt)
        else:
            logger.debug("    Copying '%s' to '%s'", str(src), str(tgt))
            shutil.copy(src, tgt)


class WalkerFactoryException(Exception):
    pass


@define
class WalkerFactory:
    """This class builds the walker and gets it running."""

    walker: SyncWalker

    @classmethod
    def from_toml(cls, config_path: Path) -> "WalkerFactory":
        """Read the config file and build the walker from the config

        Args:
            file: Path to the TOML config file.

        Raises:
            FileNotFoundError: Config file not found at the config_path.
            PermissionError: File at config_path is not readable.
            TOMLDecodeError: Config file is not valid TOML
        """
        with open(config_path, "rb") as f:  # tomli requires "rb"
            try:
                toml_dict = tomli.load(f)
            except tomli.TOMLDecodeError as e:
                raise WalkerFactoryException(
                    f"Config '{config_path}' does not contain valid TOML."
                ) from e

        logger.debug("Config: %s", str(toml_dict))

        num_avail_cpus = len(os.sched_getaffinity(0))
        if not num_avail_cpus or num_avail_cpus <= 1:
            logger.warn(
                "Failed to determine number of available CPUs, defaulting to 1."
            )
            num_avail_cpus = 1

        light_pool_workers = toml_dict.setdefault("light_threads", None)
        if not light_pool_workers:
            light_pool_workers = num_avail_cpus * 10
            logger.debug("Light pool threads defaulting to %d", light_pool_workers)

        heavy_pool_workers = toml_dict.setdefault("heavy_threads", None)
        if not heavy_pool_workers:
            heavy_pool_workers = num_avail_cpus * 2
            logger.debug("Heavy pool threads defaulting to %d", heavy_pool_workers)

        light_pool = ThreadPoolExecutor(
            max_workers=light_pool_workers, thread_name_prefix="light"
        )
        heavy_pool = ThreadPoolExecutor(
            max_workers=heavy_pool_workers, thread_name_prefix="heavy"
        )
        results: Queue[Future] = Queue(maxsize=num_avail_cpus * 40)

        if not Path(toml_dict["converter"]["exe"]).expanduser().is_file():
            exe_str = toml_dict["converter"]["exe"]
            raise WalkerFactoryException(f"Cannot access converter at {exe_str}")

        walker = SyncWalker(
            copier_cfg=toml_dict["copier"],
            converter_cfg=toml_dict["converter"],
            src=Path(toml_dict["source"]).expanduser(),
            tgt=Path(toml_dict["target"]).expanduser(),
            light_pool=light_pool,
            heavy_pool=heavy_pool,
            results=results,
        )

        return cls(walker=walker)

    def build_and_run(self) -> None:

        self.walker.start()

        # Drain the results queue of Futures and wait for them to be done
        #  Note shutdown wait=True doesn't work as we haven't submitted
        #  all the jobs before calling shutdown!
        results = self.walker.results
        futures: Set[Future] = set()
        futures.add(results.get())
        while futures:
            logger.debug("waiting %d", len(futures))
            wait(futures, return_when=FIRST_EXCEPTION)
            for future in futures:
                if future.exception() is not None:
                    raise Exception(
                        "Exception %s", str(future.exception())
                    ) from future.exception()
            futures = set()
            while not results.empty():
                futures.add(results.get())
