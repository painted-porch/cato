"""Marcus root directory discovery for Cato.

Single source of truth for locating the Marcus installation so Cato is
usable on any machine — no developer-specific absolute paths are baked
into the code.

Configuration is layered: ``config.json`` holds shared settings (ports,
cutoff date) and is committed; ``config.local.json`` holds the
machine-specific Marcus path and is gitignored. The ``cato`` CLI writes
``config.local.json`` after prompting the user on first start.

Two keys describe where Marcus data lives, in either file:

* ``marcus_data_path`` — one Marcus ``data/`` directory.
* ``marcus_data_paths`` — a list of them, one per parallel Marcus
  instance. When present and non-empty it overrides the singular key,
  exactly as ``backend/api.py`` treats it.

Marcus-root resolution order (first match wins):

1. ``MARCUS_ROOT`` environment variable.
2. ``marcus_data_path`` / ``marcus_data_paths`` from the merged config
   (``config.local.json`` overrides ``config.json``).
3. Auto-detection of well-known locations: Marcus checked out as a
   sibling of Cato, or ``~/dev/marcus``.
"""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

# cato_src/core/marcus_paths.py -> parents[2] is the Cato project root.
_CATO_ROOT = Path(__file__).resolve().parents[2]
_CONFIG_PATH = _CATO_ROOT / "config.json"
_LOCAL_CONFIG_PATH = _CATO_ROOT / "config.local.json"


def _read_json(path: Path) -> Dict[str, Any]:
    """Return a parsed JSON object, or an empty dict if absent/invalid."""
    try:
        with open(path) as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def load_merged_config() -> Dict[str, Any]:
    """Load ``config.json`` overlaid with ``config.local.json``.

    ``config.local.json`` is gitignored and holds machine-specific
    settings (the Marcus path); its keys win over the committed
    ``config.json``.
    """
    config = _read_json(_CONFIG_PATH)
    config.update(_read_json(_LOCAL_CONFIG_PATH))
    return config


def configured_marcus_data_paths() -> List[Path]:
    """Return every Marcus ``data/`` directory named in the merged config.

    ``marcus_data_paths`` (a list) wins over ``marcus_data_path`` (a
    string) when it is a non-empty list, mirroring ``backend/api.py`` so
    the launcher's first-run guard and the backend agree on what counts
    as configured. Entries that are not non-empty strings are skipped,
    and a bare string under the plural key is ignored rather than being
    iterated character by character.

    Returns
    -------
    List[Path]
        Configured data directories with ``~`` expanded, in config order.
        Empty when nothing is configured. Existence is not checked; see
        :func:`existing_configured_marcus_data_paths`.
    """
    config = load_merged_config()
    multi = config.get("marcus_data_paths")
    if isinstance(multi, list):
        paths = [Path(p).expanduser() for p in multi if isinstance(p, str) and p]
        if paths:
            return paths
    single = config.get("marcus_data_path")
    if isinstance(single, str) and single:
        return [Path(single).expanduser()]
    return []


def existing_configured_marcus_data_paths() -> List[Path]:
    """Return the configured Marcus data directories that exist on disk.

    Returns
    -------
    List[Path]
        The subset of :func:`configured_marcus_data_paths` that are
        directories, in config order.
    """
    return [p for p in configured_marcus_data_paths() if p.is_dir()]


def write_local_marcus_root(marcus_root: Path) -> Path:
    """Record ``marcus_root`` as the single Marcus install in ``config.local.json``.

    Used by the ``cato`` launcher after prompting on first start. Other
    keys already in ``config.local.json`` are kept. A stale
    ``marcus_data_paths`` list is removed, because it would otherwise
    override the path being saved and leave Cato pointing at directories
    the user just told us are gone.

    Parameters
    ----------
    marcus_root : Path
        The Marcus root directory (the folder containing ``data/`` and
        ``src/``). Stored as its ``data/`` subdirectory, resolved.

    Returns
    -------
    Path
        The path of the local config file that was written.
    """
    local = _read_json(_LOCAL_CONFIG_PATH)
    local.pop("marcus_data_paths", None)
    local["marcus_data_path"] = str(Path(marcus_root).expanduser().resolve() / "data")
    with open(_LOCAL_CONFIG_PATH, "w") as f:
        json.dump(local, f, indent=2)
        f.write("\n")
    return _LOCAL_CONFIG_PATH


def _config_marcus_root() -> Optional[Path]:
    """Marcus root derived from the merged config, or None if unconfigured.

    The config stores Marcus *data* directories; the Marcus root is the
    parent of the first one.
    """
    paths = configured_marcus_data_paths()
    if not paths:
        return None
    return paths[0].parent


def discover_marcus_root(marker: str = "src") -> Optional[Path]:
    """Locate the Marcus root directory.

    Parameters
    ----------
    marker:
        Relative path that must exist under a candidate for it to be
        accepted (e.g. ``"src/analysis"`` or ``"src/cost_tracking"``).
        Lets callers require the specific Marcus subpackage they import.

    Returns
    -------
    Optional[Path]
        The first candidate containing ``marker``, or None if none match.
    """
    candidates: list[Path] = []

    env_root = os.environ.get("MARCUS_ROOT")
    if env_root:
        candidates.append(Path(env_root).expanduser())

    config_root = _config_marcus_root()
    if config_root:
        candidates.append(config_root)

    # Auto-detection fallbacks — no machine-specific absolute paths.
    candidates.append(_CATO_ROOT.parent / "marcus")  # sibling of Cato
    candidates.append(Path.home() / "dev" / "marcus")

    for candidate in candidates:
        if (candidate / marker).exists():
            return candidate
    return None
