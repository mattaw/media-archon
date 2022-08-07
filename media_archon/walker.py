# SPDX-FileCopyrightText: 2022-present Matthew Swabey <matthew@swabey.org>
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import os
import re
import shutil
import subprocess
import tempfile
from collections.abc import Iterable
from concurrent.futures import FIRST_EXCEPTION, Future, ThreadPoolExecutor, wait
from logging import getLogger
from pathlib import Path
from queue import Empty, Queue
from typing import Any, Dict, List, Optional, Set, Union

import tomli
from attrs import define, evolve

logger = getLogger(__name__)

# Note - in python if we a=b where b is anything other than str, int + a few more types
#  a=b just copies a reference. Ruthlessly exploit this to pass around the same
#  thread pools and the queue etc.
# There is a light ThreadPoolExecutor to manage the overlapping IO and directory
# exploration, and any copying that is needed. Thinking cpu_count() * 100?
# Heavy threadpool should be cpu_count * 2


CONFIG_FILE_NAME = "media-archon.toml"


def sp(path: Path) -> str:
    """Shorten path to parent and filename"""
    path_list = str(path).split(os.sep)
    return "." + os.sep + os.sep.join(path_list[-2:])


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
    config_file_mtime: int

    @classmethod
    def from_toml(cls, src_dir: Path, config_path: Optional[Path]) -> "Walker":
        """Read the config file and build the ranger from the config

        Args:
            config_path: Path to the TOML config file.
            src_dir: Path to the media library to convert.

        Raises:
            FileNotFoundError: Config file not found at the config_path.
            PermissionError: File at config_path is not readable.
            TOMLDecodeError: Config file is not valid TOML
            WalkerConfException: Config file is not correct
        """
        try:
            src_dir = validate_is_dir(src_dir)
        except TypeError as e:
            raise WalkerConfException(
                f"{src_dir} is not a valid source directory"
                " path on this operating system."
            ) from e
        except FileNotFoundError:
            raise WalkerConfException(f"{src_dir} was not found.")

        if config_path is not None:
            config_path = config_path.expanduser()
        elif (src_dir / CONFIG_FILE_NAME).is_file():
            config_path = src_dir / CONFIG_FILE_NAME
        if config_path is None:
            raise FileNotFoundError(f"Config '{config_path}' not found.")

        with open(config_path, "rb") as f:  # tomli requires "rb"
            try:
                toml_dict = tomli.load(f)
            except tomli.TOMLDecodeError as e:
                raise WalkerConfException(
                    f"Config '{config_path}' does not contain valid TOML."
                ) from e

        config_file_mtime = config_path.stat().st_mtime_ns

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

        num_walkers = num_cpus * 5 if not num_walkers else num_walkers
        num_converters = num_cpus * 1 if not num_converters else num_converters

        walker_threadpool = ThreadPoolExecutor(
            max_workers=num_walkers, thread_name_prefix="light"
        )
        converter_threadpool = ThreadPoolExecutor(
            max_workers=num_converters, thread_name_prefix="heavy"
        )
        logger.info("%d walker threads created", num_walkers)
        logger.info("%d converter threads created", num_converters)

        # Create results queue - this can get quite large!
        results: Queue[Future] = Queue()

        # Check/sanitize source and target:

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
            config_file_mtime=config_file_mtime,
        )
        return walker

    def update_from_toml(self, config_path: Path) -> "Walker":
        """Update command arguments mid-process"""
        with open(config_path, "rb") as f:  # tomli requires "rb"
            try:
                toml_dict = tomli.load(f)
            except tomli.TOMLDecodeError as e:
                raise WalkerConfException(
                    f"Config '{config_path}' does not contain valid TOML."
                ) from e
        new_walker = evolve(
            self,
            config_file_mtime=config_path.stat().st_mtime_ns,
            converter_cmd=toml_dict["converter"]["cmd"],
            converter_cmd_args=toml_dict["converter"]["cmd_args"],
        )
        logger.info("Updating converter cmd and cmd_args from %s", sp(config_path))
        return new_walker

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

    @staticmethod
    def _walk_thread(walker: "Walker") -> None:
        logger.debug("Walking '%s'", str(walker.src_dir))

        # Create target dir deleting file if unexpectedly present
        if not walker.tgt_dir.exists():
            logger.debug("  Creating dir %s", sp(walker.tgt_dir))
            walker.tgt_dir.mkdir(exist_ok=True)
        elif not walker.tgt_dir.is_dir():
            logger.debug("  Replacing with dir %s", sp(walker.tgt_dir))
            walker.tgt_dir.unlink()
            walker.tgt_dir.mkdir()

        # Build set of all objects in the directory exluding .* and
        #  handling an updated config
        src_objects: List[Path] = []
        for src_obj in walker.src_dir.iterdir():
            src_obj_name = src_obj.name
            if src_obj_name[:1] == ".":
                logger.debug("  Ignoring %s", sp(src_obj))
                continue
            elif src_obj_name == walker.config_file_name:
                logger.debug("  Found config file %s", sp(src_obj))
                walker = walker.update_from_toml(src_obj)
                continue
            else:
                src_objects.append(src_obj)

        # Process every item in the directory
        expected_tgt_names = set()  # Record all expected files in target
        for item in src_objects:
            item_name = item.name
            if item.is_dir():
                # Create an updated walker to work on subdir
                new_syncwalker: "Walker" = evolve(
                    walker,
                    src_dir=walker.src_dir / item_name,
                    tgt_dir=walker.tgt_dir / item_name,
                )
                new_syncwalker.start()
                expected_tgt_names.add(item_name)
            elif item.is_file():
                if item.suffix in walker.copier_input_exts:
                    # Potential copy file
                    tgt = walker.tgt_dir / item_name
                    walker._copy(item, tgt)
                    expected_tgt_names.add(item_name)
                elif item.suffix in walker.conv_input_exts:
                    # Potential convert file
                    tgt_name = item.stem + walker.converter_output
                    tgt = walker.tgt_dir / tgt_name
                    walker._convert(item, tgt)
                    expected_tgt_names.add(tgt_name)

        # Unlink / rmdir any extra items from tgt_dir
        for tobj in walker.tgt_dir.iterdir():
            tobj_name = tobj.name
            if tobj_name not in expected_tgt_names and tobj_name[:1] != ".":
                walker._delete(tobj)

    def start(self):
        self.results.put(self.walker_threadpool.submit(Walker._walk_thread, self))

    @staticmethod
    def _copy_thread(src: Path, tgt: Path):
        logger.debug("  Copy? %s", sp(src))
        try:
            tgt_mtime = tgt.stat().st_mtime_ns
            src_mtime = src.stat().st_mtime_ns
            if src_mtime >= tgt_mtime or src.is_dir() != tgt.is_dir():
                logger.info("    Copying newer %s", sp(src))
                if tgt.is_dir():
                    shutil.rmtree(tgt)
                shutil.copyfile(src, tgt)
        except FileNotFoundError:
            logger.info("    Copying %s", sp(src))
            shutil.copyfile(src, tgt)

    def _copy(self, src: Path, tgt: Path):
        self.results.put(
            self.walker_threadpool.submit(self._copy_thread, src=src, tgt=tgt)
        )

    @staticmethod
    def _delete_thread(tgt: Path):
        logger.debug("  Delete? %s", sp(tgt))
        if tgt.is_dir():
            logger.info("    Cleaning dir %s", sp(tgt))
            shutil.rmtree(tgt)
        else:
            logger.info("    Cleaning file %s", sp(tgt))
            tgt.unlink()

    def _delete(self, tgt: Path):
        self.results.put(self.walker_threadpool.submit(self._delete_thread, tgt=tgt))

    def _actual_convert(self, src: Path, tgt: Path):
        with tempfile.TemporaryDirectory(prefix="media-archon-") as tmpdir:
            tmptgt = tmpdir + os.sep + str(tgt.name)
            cmd_pre: List[str] = [str(self.converter_exe)] + self.converter_cmd.split()
            fields: Dict[str, Union[int, str]] = {
                "input": str(src),
                "output": tmptgt,
            }
            if self.converter_cmd_args:
                fields.update(self.converter_cmd_args)
            cmd = []
            for token in cmd_pre:
                cmd.append(token.format_map(fields))
            logger.debug("Conversion cmd: %s", cmd)
            subprocess.run(cmd, capture_output=True, check=True)
            shutil.copyfile(tmptgt, tgt)

    @staticmethod
    def _convert_thread(src: Path, tgt: Path, walker: "Walker"):
        """Thread payload to convert src to tgt if src.mtime is newer or
        the current config file mtime is newer"""
        logger.debug("  Convert? %s", sp(src))
        try:
            try:
                tgt_mtime = tgt.stat().st_mtime_ns
                src_mtime = src.stat().st_mtime_ns
                if (
                    src_mtime >= tgt_mtime
                    or src.is_dir() != tgt.is_dir()
                    or walker.config_file_mtime >= tgt_mtime
                ):
                    logger.info("    Converting newer %s", sp(src))
                    if tgt.is_dir():
                        shutil.rmtree(tgt)
                    walker._actual_convert(src=src, tgt=tgt)
            except FileNotFoundError:
                logger.info("    Converting %s", sp(src))
                walker._actual_convert(src=src, tgt=tgt)
        except subprocess.CalledProcessError as e:
            logger.error("  Conversion Failed", exc_info=e)

    def _convert(self, src: Path, tgt: Path):
        self.results.put(
            self.converter_threadpool.submit(
                self._convert_thread, src=src, tgt=tgt, walker=self
            )
        )
