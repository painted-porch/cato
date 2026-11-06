"""Marcus root directory discovery for Cato.

Single source of truth for locating the Marcus installation so Cato is
usable on any machine — no developer-specific absolute paths are baked
into the code.

Configuration is layered: ``config.json`` holds shared settings (ports,
cutoff date) and is committed; ``config.local.json`` holds the
machine-specific Marcus path and is gitignored. The ``cato`` CLI writes
``config.local.json`` after prompting the user on first start. Only the
Marcus-path keys are read from the local file: every other setting lives
in ``config.json`` alone, where the backend's ``/api/settings`` endpoints
read and write it, so the value that filters the data is always the value
the dashboard shows.

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
import logging
import os
from pathlib import Path
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# cato_src/core/marcus_paths.py -> parents[2] is the Cato project root.
_CATO_ROOT = Path(__file__).resolve().parents[2]
_CONFIG_PATH = _CATO_ROOT / "config.json"
_LOCAL_CONFIG_PATH = _CATO_ROOT / "config.local.json"

# The only keys config.local.json may override. Everything else is owned by
# config.json and the /api/settings endpoints that read and write it.
_LOCAL_OVERLAY_KEYS = ("marcus_data_path", "marcus_data_paths")


def _read_json(path: Path) -> Dict[str, Any]:
    """Return a parsed JSON object, or an empty dict if absent/invalid."""
    try:
        with open(path) as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def load_merged_config() -> Dict[str, Any]:
    """Load ``config.json`` overlaid with the Marcus-path keys of ``config.local.json``.

    ``config.local.json`` is gitignored and holds the machine-specific
    Marcus location: ``marcus_data_path`` and ``marcus_data_paths`` win
    over the committed ``config.json``. Any other key in the local file is
    ignored and reported, because the backend's settings endpoints read
    and write those settings in ``config.json`` only; honoring a local
    copy would make the dashboard filter by one value and display another.

    Returns
    -------
    Dict[str, Any]
        The merged configuration.
    """
    config = _read_json(_CONFIG_PATH)
    local = _read_json(_LOCAL_CONFIG_PATH)
    ignored = sorted(k for k in local if k not in _LOCAL_OVERLAY_KEYS)
    if ignored:
        logger.warning(
            "Ignoring %s in config.local.json: only %s may be set there; "
            "other settings belong in config.json",
            ", ".join(ignored),
            " and ".join(_LOCAL_OVERLAY_KEYS),
        )
    config.update({k: v for k, v in local.items() if k in _LOCAL_OVERLAY_KEYS})
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
    keys already in ``config.local.json`` are kept in the file (they are
    the user's, even though only the Marcus-path keys are read). A stale
    ``marcus_data_paths`` list is removed from the local file, and if the
    tracked ``config.json`` carries one it is masked with an explicit
    empty list, because a plural list anywhere in the merged config would
    otherwise override the path being saved and leave Cato pointing at
    directories the user just told us are gone.

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
    if _read_json(_CONFIG_PATH).get("marcus_data_paths"):
        # The tracked file carries a list we cannot delete from here. An
        # explicit empty list in the local file overrides it, and an empty
        # list means "no plural paths" to every reader, so the singular
        # path saved below is the one that takes effect.
        local["marcus_data_paths"] = []
    local["marcus_data_path"] = str(Path(marcus_root).expanduser().resolve() / "data")
    with open(_LOCAL_CONFIG_PATH, "w") as f:
        json.dump(local, f, indent=2)
        f.write("\n")
    return _LOCAL_CONFIG_PATH


def marcus_root_from_env() -> Optional[Path]:
    """Return the Marcus root named by ``MARCUS_ROOT``, if it is usable.

    Returns
    -------
    Optional[Path]
        ``MARCUS_ROOT`` with ``~`` expanded when it is set and contains a
        ``data/`` directory; None when it is unset or does not look like a
        Marcus root. Callers decide whether to warn about the latter.
    """
    env_root = os.environ.get("MARCUS_ROOT")
    if not env_root:
        return None
    root = Path(env_root).expanduser()
    return root if (root / "data").is_dir() else None


def live_marcus_roots() -> List[Path]:
    """Return the Marcus roots live-mode aggregation should read.

    ``MARCUS_ROOT`` takes precedence over anything configured, so the
    environment override steers the whole dashboard and not only the
    historical modules that go through :func:`discover_marcus_root`.
    Without it, the roots are the parents of the configured data paths
    (plural list first, then the single path), in config order.

    Returns
    -------
    List[Path]
        Roots to aggregate over, or an empty list when nothing is set or
        configured and the caller should fall back to auto-detection.
    """
    env_root = marcus_root_from_env()
    if env_root is not None:
        return [env_root]
    return [p.parent for p in configured_marcus_data_paths()]


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
