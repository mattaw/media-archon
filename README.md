# Media Archon

[![PyPI - Version](https://img.shields.io/pypi/v/media-archon.svg)](https://pypi.org/project/media-archon)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/media-archon.svg)](https://pypi.org/project/media-archon)

-----

**Table of Contents**

- [Installation](#installation)
- [License](#license)

## Installation

```console
pip install media-archon
```

Copy the config file `media-archon.toml` into the top level of the directory tree of media you wish to convert and edit to meet your needs.

## Theory

Media archon is an aggressively multithreaded Linux/Unix/MacOSX tool designed to mirror a tree of source media files to a destination tree, converting matching media using any command line tool selected by the user. It has been designed and tested around using the excellent [fre:ac](https://www.freac.org/) opensource audio software to convert existing MP3 audiobooks etc. into Opus files for unbeatable size vs. quality.

When executed with `media-archon /directory/of/media/files/`, it will: 

1. Look for and read the `/directory/of/media/files/media-archon.toml`. 
1. Attempt to discover how many threads can execute in parallel on the host (Threads).
1. Create two threadpools, one `light` (default 10 × Threads) for exploring directories and copying, and one `heavy` (default 1 × Threads). These can be overriden in the config.

After configuring itself it will schedule a `light` thread to walk `/directory/of/media/files/` which will:

1. Iterate through the objects in the directory looking for a configuration override file (default `media-archon-override.toml`). If found it will update its converter parameters for this directory and all its subdirectories.
1. Loop through the objects again and depending on whether it is a directory or a file with a particular extension (e.g. `.mp3`):
   1. If a directory, schedule a new `light` thread to search it in parallel.
   2. If a file on the ignore list (`.*`) ignore.
   3. If a file with an extension in the copy list schedule a `light` thread to copy it to the destination.
   4. If a file with an extension in the convert list schedule a `heavy` thread to convert it from the source to the target using the supplied command line in the config.
1. Delete files and directories in the target that are not in the source.

## License

`media-archon` is distributed under the terms of the [AGPL-3.0-or-later](https://spdx.org/licenses/AGPL-3.0-or-later.html) license.
