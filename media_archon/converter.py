# SPDX-FileCopyrightText: 2022-present Matthew Swabey <matthew@swabey.org>
#
# SPDX-License-Identifier: AGPL-3.0-or-later

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, List, Set

from attrs import define


@define
class Converter:
    config: Any
    converter_exe: Path
    convert_suffixes: Set[str]
    copy_suffixes: Set[str]
    light_pool: ThreadPoolExecutor  # Large number of threads for copy etc.
    heavy_pool: ThreadPoolExecutor  # Smaller number of threads for exec converter

    def process(files: List[Path], tgt_dir: Path) -> None:
        # Look for config, if exists update
        # Queue for convertion or copy files to destination
        pass

    @staticmethod
    def convert():
        pass
