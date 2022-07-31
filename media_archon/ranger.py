# SPDX-FileCopyrightText: 2022-present Matthew Swabey <matthew@swabey.org>
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import logging
import os
import re
import shutil
import subprocess
from collections.abc import Iterable
from concurrent.futures import FIRST_EXCEPTION, Future, ThreadPoolExecutor, wait
from logging import getLogger
from pathlib import Path
from queue import Empty, Queue
from typing import Any, Dict, List, Optional, Set, Union

import tomli
from attrs import define, evolve

logging.basicConfig(
    format="%(asctime)s:%(threadName)-10s - %(message)s", level=logging.DEBUG
)
logger = getLogger(__name__)

# Note - in python if we a=b where b is anything other than str, int + a few more types
#  a=b just copies a reference. Ruthlessly exploit this to pass around the same
#  thread pools and the queue etc.
# There is a light ThreadPoolExecutor to manage the overlapping IO and directory
# exploration, and any copying that is needed. Thinking cpu_count() * 100?
# Heavy threadpool should be cpu_count * 2


def validate_pos_int(var_: Any) -> int:
    """Convert var into an int and validate it is positive"""
    var_ = int(var_)  # Rases ValueError if not able to convert
    if var_ < 1:
        raise ValueError(f"{var_} is not a positive integer.")
    return var_


def validate_is_dir(path_str: Any) -> Path:
    dir_ = Path(path_str).expanduser()
    if not dir_.is_dir():
        raise FileNotFoundError(f"{path_str} is not a directory or does not exist.")
    return dir_


def validate_is_file(path_str: Any) -> Path:
    file_ = Path(path_str).expanduser()
    if not file_.is_file():
        raise FileNotFoundError(f"{path_str} is not a file or does not exist.")
    return file_


def validate_suffixes(suffixes: Iterable[Any]) -> Set[str]:
    pattern = re.compile(r"^\.[\w]+$")
    for suffix in suffixes:
        if not pattern.match(suffix):
            raise ValueError("File suffixes must be of the form .a-z0-9")
    return set(suffixes)


class WalkerException(Exception):
    pass


class WalkerConfException(Exception):
    pass


@define
class Walker:
    src_dir: Path
    tgt_dir: Path
    walker_threadpool: ThreadPoolExecutor
    converter_threadpool: ThreadPoolExecutor  # Smaller number of threads for conversion
    results: Queue[Future]
    copier_input_exts: Set[str]
    conv_input_exts: Set[str]
    converter_exe: Path
    converter_output: str
    converter_cmd: str
    converter_cmd_args: Dict[str, Union[int, str]]
    config_file_name: str

    @classmethod
    def from_toml(cls, config_path: Path) -> "Walker":
        """Read the config file and build the ranger from the config

        Args:
            file: Path to the TOML config file.

        Raises:
            FileNotFoundError: Config file not found at the config_path.
            PermissionError: File at config_path is not readable.
            TOMLDecodeError: Config file is not valid TOML
            WalkerConfException: Config file is not correct
        """
        with open(config_path, "rb") as f:  # tomli requires "rb"
            try:
                toml_dict = tomli.load(f)
            except tomli.TOMLDecodeError as e:
                raise WalkerConfException(
                    f"Config '{config_path}' does not contain valid TOML."
                ) from e

        logger.debug("Config: %s", str(toml_dict))

        # Create light_pool and heavy_pool
        try:
            num_walkers = toml_dict.setdefault("walkers", None)
            num_walkers = (
                validate_pos_int(num_walkers) if num_walkers is not None else None
            )
        except ValueError as e:
            raise WalkerConfException(
                "If 'walkers' is set it must be a postive integer."
            ) from e

        try:
            num_converters = toml_dict.setdefault("converters", None)
            num_converters = (
                validate_pos_int(num_converters) if num_converters is not None else None
            )
        except ValueError as e:
            raise WalkerConfException(
                "If 'converters' is set it must be a postive integer."
            ) from e

        try:
            num_cpus = len(os.sched_getaffinity(0))
        except AttributeError:
            logger.info(
                "Failed to determine number of available CPUs using"
                " os.sched_getaffinity(), assuming 1."
            )
            num_cpus = 1

        num_walkers = num_cpus * 10 if not num_walkers else num_walkers
        num_converters = num_cpus * 2 if not num_converters else num_converters

        walker_threadpool = ThreadPoolExecutor(
            max_workers=num_walkers, thread_name_prefix="light"
        )
        converter_threadpool = ThreadPoolExecutor(
            max_workers=num_converters, thread_name_prefix="heavy"
        )
        logger.info("Walker threadpool created. Size: %d", num_walkers)
        logger.info("Converter threadpool created. Size: %d", num_converters)

        # Create results queue - this can get quite large!
        results: Queue[Future] = Queue()

        # Check/sanitize source and target:
        try:
            src_dir_str = toml_dict["src_dir"]
            src_dir = validate_is_dir(src_dir_str)
        except KeyError as e:
            raise WalkerConfException(
                "src_dir=<dir> must be defined in the config."
            ) from e
        except TypeError as e:
            raise WalkerConfException(
                f"src_dir={src_dir_str} is not a valid directory"
                " path on this operating system."
            ) from e
        except FileNotFoundError:
            raise WalkerConfException(f"{src_dir_str} was not found.")

        try:
            tgt_dir_str = toml_dict["tgt_dir"]
            tgt_dir = validate_is_dir(tgt_dir_str)
        except KeyError as e:
            raise WalkerConfException(
                "tgt_dir=<dir> must be defined in the config."
            ) from e
        except TypeError as e:
            raise WalkerConfException(
                f"v={tgt_dir_str} is not a valid directory"
                " path on this operating system."
            ) from e
        except FileNotFoundError:
            raise WalkerConfException(f"{tgt_dir_str} was not found.")

        # Check copier inputs
        try:
            copier_input_suffixes = validate_suffixes(toml_dict["copier"]["inputs"])
        except KeyError as e:
            raise WalkerConfException(
                "copier.inputs must be defined in the config."
            ) from e
        except ValueError as e:
            raise WalkerConfException(
                'copier.inputs must be a list of file suffixes e.g. ".jpg".'
            ) from e

        # Check converter inputs
        try:
            converter_input_suffixes = validate_suffixes(
                toml_dict["converter"]["inputs"]
            )
        except KeyError as e:
            raise WalkerConfException(
                "converter.inputs must be defined in the config."
            ) from e
        except ValueError as e:
            raise WalkerConfException(
                'converter.inputs must be a list of file suffixes e.g. ".jpg".'
            ) from e

        # Check converter outputs
        try:
            converter_output_suffix = validate_suffixes(
                [toml_dict["converter"]["output"]]
            ).pop()
        except KeyError as e:
            raise WalkerConfException(
                "converter.output must be defined in the config."
            ) from e
        except ValueError as e:
            raise WalkerConfException(
                'converter.output must be a single file suffix e.g. ".opus".'
            ) from e

        # Check for converter program:
        try:
            exe_path_str = toml_dict["converter"]["exe"]
            exe = validate_is_file(exe_path_str)
        except KeyError as e:
            raise WalkerConfException(
                "[converter.exe]=<path to converter> must be defined in the config."
            ) from e
        except TypeError as e:
            raise WalkerConfException(
                f"target={tgt_dir_str} is not a valid filename"
                " on this operating system."
            ) from e
        except FileNotFoundError:
            raise WalkerConfException(f"{tgt_dir_str} was not found.")

        # Check for converter command:
        try:
            converter_cmd = toml_dict["converter"]["cmd"]
        except KeyError as e:
            raise WalkerConfException(
                "[converter.cmd]=<command> must be defined in the config."
            ) from e

        # Check for converter command args:
        try:
            converter_cmd_args = toml_dict["converter"]["cmd_args"]
        except KeyError:
            converter_cmd_args = None

        # Check for converter config_update
        try:
            config_file_name = toml_dict["converter"]["config"]
            Path(config_path)
        except KeyError as e:
            raise WalkerConfException(
                "[converter.config]=<command> must be defined in the config."
            ) from e

        walker = Walker(
            src_dir=src_dir,
            tgt_dir=tgt_dir,
            walker_threadpool=walker_threadpool,
            converter_threadpool=converter_threadpool,
            results=results,
            copier_input_exts=copier_input_suffixes,
            conv_input_exts=converter_input_suffixes,
            converter_exe=exe,
            converter_output=converter_output_suffix,
            converter_cmd=converter_cmd,
            converter_cmd_args=converter_cmd_args,
            config_file_name=config_file_name,
        )
        return walker

    def build_and_run(self) -> None:
        # Drain the results queue of Futures and wait for them to be done
        #  Note shutdown wait=True doesn't work as we haven't submitted
        #  all the jobs before calling shutdown!
        self.start()

        results = self.results
        futures: Set[Future] = set()
        futures.add(results.get())
        while futures:
            logger.debug("Waiting for %d threads.", len(futures))
            wait(futures, return_when=FIRST_EXCEPTION)
            for future in futures:
                if future.exception() is not None:
                    raise Exception(
                        "Exception %s", str(future.exception())
                    ) from future.exception()
            futures = set()
            while True:  # queue.empty() isn't reliable
                try:
                    futures.add(results.get(block=False))
                except Empty:
                    break

    def _walk(self) -> None:
        logger.debug("Walking '%s'", str(self.src_dir))

        # Create target dir deleting file if unexpectedly present
        if not self.tgt_dir.exists():
            logger.debug("  Creating dir %s", str(self.tgt_dir))
            self.tgt_dir.mkdir(exist_ok=True)
        elif not self.tgt_dir.is_dir():
            logger.debug("  Replacing with dir %s", str(self.tgt_dir))
            self.tgt_dir.unlink()
            self.tgt_dir.mkdir()

        # Build set of all objects in the directory exluding .* and config:
        src_objects: Set[Path] = set()
        updated_config_file: Optional[Path] = None
        for src_obj in self.src_dir.iterdir():
            src_obj_name = src_obj.name
            if src_obj_name[:1] == ".":
                logger.debug("  Ignoring %s", str(src_obj))
                continue
            elif src_obj_name == self.config_file_name:
                logger.debug("  Found config file %s", str(src_obj))
                updated_config_file = src_obj
                continue
            else:
                src_objects.add(src_obj)

        # TODO: figure out updated file
        if updated_config_file:
            syncwalker = self
        else:
            syncwalker = self

        # Process every item in the directory
        expected_tgt_names = set()  # Record all expected files in target
        for item in src_objects:
            item_name = item.name
            if item.is_dir():
                # Submit a new worker to work on subdir
                new_syncwalker: "Walker" = evolve(
                    syncwalker,
                    src_dir=syncwalker.src_dir / item_name,
                    tgt_dir=syncwalker.tgt_dir / item_name,
                )
                new_syncwalker.start()
                expected_tgt_names.add(item_name)
            elif item.is_file():
                if item.suffix in self.copier_input_exts:
                    # Potential copy file
                    tgt = self.tgt_dir / item_name
                    self._copy(item, tgt)
                    expected_tgt_names.add(item_name)
                elif item.suffix in self.conv_input_exts:
                    # Potential convert file
                    tgt_name = item.stem + self.converter_output
                    tgt = self.tgt_dir / tgt_name
                    self._convert(item, tgt)
                    expected_tgt_names.add(tgt_name)

        # Unlink / rmdir any extra items from tgt_dir
        for tobj in self.tgt_dir.iterdir():
            tobj_name = tobj.name
            if tobj_name not in expected_tgt_names and tobj_name[:1] != ".":
                self._delete(tobj)

    def start(self):
        self.results.put(self.walker_threadpool.submit(Walker._walk_thread, self))

    @staticmethod
    def _walk_thread(sync_walker: "Walker") -> None:
        sync_walker._walk()

    @staticmethod
    def start_thread(sync_walker: "Walker") -> None:
        sync_walker.start()

    @staticmethod
    def _copy_thread(src: Path, tgt: Path):
        logger.debug("  Considering for copy %s -> %s", src, tgt)
        try:
            src_mtime = src.stat().st_mtime_ns
            tgt_mtime = tgt.stat().st_mtime_ns
            if src_mtime >= tgt_mtime or src.is_dir() != tgt.is_dir():
                logger.debug("    Copying newer %s -> %s", src, tgt)
                if tgt.is_dir():
                    shutil.rmtree(tgt)
                else:
                    tgt.unlink()
                shutil.copy(src, tgt)
        except FileNotFoundError:
            logger.debug("    Copying %s -> %s", src, tgt)
            shutil.copy(src, tgt)

    def _copy(self, src: Path, tgt: Path):
        self.results.put(
            self.walker_threadpool.submit(self._copy_thread, src=src, tgt=tgt)
        )

    @staticmethod
    def _delete_thread(tgt: Path):
        logger.debug("Considering for delete %s", str(tgt))
        if tgt.is_dir():
            logger.debug("    Deleting dir '%s'", str(tgt))
            shutil.rmtree(tgt)
        else:
            logger.debug("    Deleting file '%s'", str(tgt))
            tgt.unlink()

    def _delete(self, tgt: Path):
        self.results.put(self.walker_threadpool.submit(self._delete_thread, tgt=tgt))

    def _actual_convert(self, src: Path, tgt: Path):
        cmd_pre: List[str] = [str(self.converter_exe)] + self.converter_cmd.split()
        fields = {"input": str(src), "output": str(tgt)}
        if self.converter_cmd_args:
            fields.update(self.converter_cmd_args)
        cmd = []
        for token in cmd_pre:
            cmd.append(token.format_map(fields))
        logger.debug("Conversion cmd: %s", cmd)
        subprocess.run(cmd)

    @staticmethod
    def _convert_thread(src: Path, tgt: Path, walker: "Walker"):
        logger.debug("  Considering for conversion %s -> %s", src, tgt)
        try:
            src_mtime = src.stat().st_mtime_ns
            tgt_mtime = tgt.stat().st_mtime_ns
            if src_mtime >= tgt_mtime or src.is_dir() != tgt.is_dir():
                logger.debug("    Converting newer %s -> %s", src, tgt)
                if tgt.is_dir():
                    shutil.rmtree(tgt)
                else:
                    tgt.unlink()
                walker._actual_convert(src=src, tgt=tgt)
        except FileNotFoundError:
            logger.debug("    Converting %s -> %s", src, tgt)
            walker._actual_convert(src=src, tgt=tgt)

    def _convert(self, src: Path, tgt: Path):
        self.results.put(
            self.converter_threadpool.submit(
                self._convert_thread, src=src, tgt=tgt, walker=self
            )
        )
