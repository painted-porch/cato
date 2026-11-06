"""Unit tests for Marcus-root discovery and layered config.

Covers ``cato_src/core/marcus_paths.py``: the ``config.local.json``
overlay on ``config.json`` and the ``discover_marcus_root`` resolution
order (MARCUS_ROOT env var → merged config → auto-detection).
"""

import json
import shutil
import subprocess  # nosec B404 - tests run the repo's own launcher script
from pathlib import Path
from typing import Any

import pytest

from cato_src.core import marcus_paths


@pytest.fixture
def isolated_config(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> Path:
    """Point marcus_paths at a tmp Cato root with tmp config files.

    Returns the fake Cato root (``tmp_path/cato``); ``config.json`` /
    ``config.local.json`` live inside it. The root is nested under the
    per-test ``tmp_path`` so the sibling-of-Cato auto-detection probes
    ``tmp_path/marcus`` — a location no other test can pollute.
    """
    cato_root = tmp_path / "cato"
    cato_root.mkdir()
    monkeypatch.setattr(marcus_paths, "_CATO_ROOT", cato_root)
    monkeypatch.setattr(marcus_paths, "_CONFIG_PATH", cato_root / "config.json")
    monkeypatch.setattr(
        marcus_paths, "_LOCAL_CONFIG_PATH", cato_root / "config.local.json"
    )
    # Redirect $HOME so the real ~/dev/marcus can't satisfy auto-detection.
    monkeypatch.setattr(marcus_paths.Path, "home", staticmethod(lambda: tmp_path))
    return cato_root


def _write(path: Path, data: Any) -> None:
    with open(path, "w") as f:
        json.dump(data, f)


def _make_marcus_root(base: Path, name: str, marker: str = "src/analysis") -> Path:
    """Create a fake Marcus tree at base/name with the given marker path."""
    root = base / name
    (root / marker).mkdir(parents=True)
    (root / "data").mkdir()
    return root


# ---------------------------------------------------------------------------
# load_merged_config
# ---------------------------------------------------------------------------


def test_merged_config_empty_when_no_files(isolated_config: Path) -> None:
    """Missing config files yield an empty dict, not an error."""
    assert marcus_paths.load_merged_config() == {}


def test_merged_config_reads_config_json(isolated_config: Path) -> None:
    """config.json is loaded when config.local.json is absent."""
    _write(isolated_config / "config.json", {"marcus_data_path": "/a/data"})
    assert marcus_paths.load_merged_config()["marcus_data_path"] == "/a/data"


def test_local_config_overrides_config_json(isolated_config: Path) -> None:
    """config.local.json keys win over config.json keys."""
    _write(
        isolated_config / "config.json",
        {"marcus_data_path": "/shared/data", "history_cutoff_date": "2026-01-01"},
    )
    _write(
        isolated_config / "config.local.json",
        {"marcus_data_path": "/local/data"},
    )
    merged = marcus_paths.load_merged_config()
    # Overridden by the local file...
    assert merged["marcus_data_path"] == "/local/data"
    # ...but shared keys from config.json survive.
    assert merged["history_cutoff_date"] == "2026-01-01"


def test_merged_config_tolerates_invalid_json(isolated_config: Path) -> None:
    """Malformed config files are ignored rather than crashing."""
    (isolated_config / "config.json").write_text("{not json")
    assert marcus_paths.load_merged_config() == {}


# ---------------------------------------------------------------------------
# discover_marcus_root
# ---------------------------------------------------------------------------


def test_discover_from_config_local(
    isolated_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """config.local.json's marcus_data_path resolves to its parent root."""
    monkeypatch.delenv("MARCUS_ROOT", raising=False)
    root = _make_marcus_root(isolated_config, "marcus_install")
    _write(
        isolated_config / "config.local.json",
        {"marcus_data_path": str(root / "data")},
    )
    assert marcus_paths.discover_marcus_root("src/analysis") == root


def test_env_var_overrides_config(
    isolated_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """MARCUS_ROOT takes precedence over the configured path."""
    config_root = _make_marcus_root(isolated_config, "configured")
    env_root = _make_marcus_root(isolated_config, "from_env")
    _write(
        isolated_config / "config.local.json",
        {"marcus_data_path": str(config_root / "data")},
    )
    monkeypatch.setenv("MARCUS_ROOT", str(env_root))
    assert marcus_paths.discover_marcus_root("src/analysis") == env_root


def test_marcus_data_paths_plural_resolves_first(
    isolated_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """marcus_data_paths (plural) resolves to the parent of its first entry."""
    monkeypatch.delenv("MARCUS_ROOT", raising=False)
    root = _make_marcus_root(isolated_config, "primary")
    _write(
        isolated_config / "config.local.json",
        {
            "marcus_data_paths": [
                str(root / "data"),
                str(isolated_config / "second" / "data"),
            ]
        },
    )
    assert marcus_paths.discover_marcus_root("src/analysis") == root


def test_marker_must_exist(
    isolated_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A configured root missing the requested marker is rejected."""
    monkeypatch.delenv("MARCUS_ROOT", raising=False)
    # Has src/analysis but not src/cost_tracking.
    root = _make_marcus_root(isolated_config, "marcus", marker="src/analysis")
    _write(
        isolated_config / "config.local.json",
        {"marcus_data_path": str(root / "data")},
    )
    assert marcus_paths.discover_marcus_root("src/analysis") == root
    assert marcus_paths.discover_marcus_root("src/cost_tracking") is None


def test_auto_detect_sibling_of_cato(
    isolated_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """With no env var or config, a Marcus sibling of Cato is found."""
    monkeypatch.delenv("MARCUS_ROOT", raising=False)
    # _CATO_ROOT is isolated_config; its sibling "marcus" should be detected.
    root = _make_marcus_root(isolated_config.parent, "marcus")
    assert marcus_paths.discover_marcus_root("src/analysis") == root


def test_discover_returns_none_when_nothing_found(
    isolated_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Returns None when Marcus cannot be located anywhere.

    No MARCUS_ROOT, no config, no sibling, and $HOME is redirected by
    the fixture so the real ~/dev/marcus cannot be found.
    """
    monkeypatch.delenv("MARCUS_ROOT", raising=False)
    assert marcus_paths.discover_marcus_root("src/analysis") is None


# ---------------------------------------------------------------------------
# Aggregator auto-detection (no-arg construction)
# ---------------------------------------------------------------------------


def test_aggregator_auto_detect_uses_resolver(
    isolated_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A no-arg Aggregator() resolves its root via the shared resolver."""
    from cato_src.core.aggregator import Aggregator

    root = _make_marcus_root(isolated_config, "marcus")
    monkeypatch.setenv("MARCUS_ROOT", str(root))
    assert Aggregator().marcus_root == root


def test_aggregator_raises_when_marcus_not_found(
    isolated_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A no-arg Aggregator() fails loudly when Marcus cannot be located.

    Replaces the old silent fallback that pointed at the Cato root.
    """
    from cato_src.core.aggregator import Aggregator

    monkeypatch.delenv("MARCUS_ROOT", raising=False)
    with pytest.raises(RuntimeError, match="Marcus"):
        Aggregator()


# ---------------------------------------------------------------------------
# Configured data paths (what the `cato` launcher's first-run guard checks)
# ---------------------------------------------------------------------------


def test_configured_data_paths_empty_when_unconfigured(isolated_config: Path) -> None:
    """No config at all means no configured Marcus data paths."""
    assert marcus_paths.configured_marcus_data_paths() == []


def test_configured_data_paths_reads_singular_key(isolated_config: Path) -> None:
    """marcus_data_path (singular) yields a one-element list."""
    _write(isolated_config / "config.local.json", {"marcus_data_path": "/a/data"})
    assert marcus_paths.configured_marcus_data_paths() == [Path("/a/data")]


def test_configured_data_paths_plural_overrides_singular(
    isolated_config: Path,
) -> None:
    """marcus_data_paths (plural) wins over the singular key, in order.

    Mirrors ``backend/api.py``, where the plural list overrides the single
    path, so the launcher guard and the backend agree on what is configured.
    """
    _write(isolated_config / "config.json", {"marcus_data_path": "/single/data"})
    _write(
        isolated_config / "config.local.json",
        {"marcus_data_paths": ["/one/data", "~/two/data"]},
    )
    assert marcus_paths.configured_marcus_data_paths() == [
        Path("/one/data"),
        Path("~/two/data").expanduser(),
    ]


def test_configured_data_paths_ignores_malformed_plural(
    isolated_config: Path,
) -> None:
    """A bare string or non-string entries under the plural key are ignored.

    A bare string would otherwise be iterated character by character; the
    singular key is used instead so a typo does not wipe out the config.
    """
    _write(
        isolated_config / "config.local.json",
        {"marcus_data_paths": "/not/a/list/data", "marcus_data_path": "/ok/data"},
    )
    assert marcus_paths.configured_marcus_data_paths() == [Path("/ok/data")]
    _write(
        isolated_config / "config.local.json",
        {"marcus_data_paths": [None, 3, "", "/good/data"]},
    )
    assert marcus_paths.configured_marcus_data_paths() == [Path("/good/data")]


def test_existing_configured_data_paths_filters_to_directories(
    isolated_config: Path,
) -> None:
    """Only configured data paths that exist on disk are reported as usable."""
    present = _make_marcus_root(isolated_config, "present")
    _write(
        isolated_config / "config.local.json",
        {
            "marcus_data_paths": [
                str(present / "data"),
                str(isolated_config / "gone" / "data"),
            ]
        },
    )
    assert marcus_paths.existing_configured_marcus_data_paths() == [present / "data"]


def test_write_local_marcus_root_preserves_other_keys(isolated_config: Path) -> None:
    """Saving a Marcus root keeps unrelated local settings and drops a stale list.

    The plural key overrides the singular one everywhere else, so leaving a
    dead ``marcus_data_paths`` behind would make the freshly saved path
    unreachable.
    """
    root = _make_marcus_root(isolated_config, "marcus")
    _write(
        isolated_config / "config.local.json",
        {"marcus_data_paths": ["/dead/data"], "history_cutoff_date": "2026-01-01"},
    )
    marcus_paths.write_local_marcus_root(root)
    saved = json.loads((isolated_config / "config.local.json").read_text())
    assert saved == {
        "marcus_data_path": str(root / "data"),
        "history_cutoff_date": "2026-01-01",
    }


def test_write_local_marcus_root_masks_tracked_plural_list(
    isolated_config: Path,
) -> None:
    """A replacement root saved locally supersedes a plural list in config.json.

    The tracked ``config.json`` may carry ``marcus_data_paths``; the local
    file cannot delete a tracked key, so the save must leave an override
    behind that makes the merged config fall through to the new path.
    """
    root = _make_marcus_root(isolated_config, "marcus")
    _write(
        isolated_config / "config.json",
        {"marcus_data_paths": ["/dead/one/data", "/dead/two/data"]},
    )
    marcus_paths.write_local_marcus_root(root)
    assert marcus_paths.configured_marcus_data_paths() == [root / "data"]
    saved = json.loads((isolated_config / "config.local.json").read_text())
    assert saved["marcus_data_paths"] == []


# ---------------------------------------------------------------------------
# Live-mode roots (what backend/api.py aggregates over)
# ---------------------------------------------------------------------------


def test_live_roots_env_overrides_configured_paths(
    isolated_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """MARCUS_ROOT wins over configured data paths for live-mode aggregation.

    Before the fix, any configured key made the backend aggregate the saved
    installation while the historical modules followed MARCUS_ROOT, so the
    two halves of the dashboard could show different Marcus installs.
    """
    configured = _make_marcus_root(isolated_config, "configured")
    env_root = _make_marcus_root(isolated_config, "from_env")
    _write(
        isolated_config / "config.local.json",
        {"marcus_data_paths": [str(configured / "data")]},
    )
    monkeypatch.setenv("MARCUS_ROOT", str(env_root))
    assert marcus_paths.live_marcus_roots() == [env_root]


def test_live_roots_invalid_env_falls_back_to_config(
    isolated_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A MARCUS_ROOT without a data/ directory is not used for live mode.

    Mirrors the launcher, which warns about an invalid MARCUS_ROOT and
    falls through to the configured path.
    """
    configured = _make_marcus_root(isolated_config, "configured")
    bogus = isolated_config / "not_marcus"
    bogus.mkdir()
    _write(
        isolated_config / "config.local.json",
        {"marcus_data_path": str(configured / "data")},
    )
    monkeypatch.setenv("MARCUS_ROOT", str(bogus))
    assert marcus_paths.marcus_root_from_env() is None
    assert marcus_paths.live_marcus_roots() == [configured]


def test_live_roots_follow_configured_paths_in_order(
    isolated_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Without MARCUS_ROOT, live roots are the parents of the configured paths."""
    monkeypatch.delenv("MARCUS_ROOT", raising=False)
    _write(
        isolated_config / "config.local.json",
        {"marcus_data_paths": ["/one/data", "/two/data"]},
    )
    assert marcus_paths.live_marcus_roots() == [Path("/one"), Path("/two")]


def test_live_roots_empty_when_unconfigured(
    isolated_config: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """No MARCUS_ROOT and no config means the caller must auto-detect."""
    monkeypatch.delenv("MARCUS_ROOT", raising=False)
    assert marcus_paths.live_marcus_roots() == []


# ---------------------------------------------------------------------------
# The `cato` launcher's guard, run for real (Codex finding on PR #46)
# ---------------------------------------------------------------------------


def _launcher_sandbox(tmp_path: Path) -> Path:
    """Copy the ``cato`` launcher into a tmp Cato root with the real package.

    The script ``cd``s to its own directory and imports ``cato_src`` from
    there, and the package locates its config files relative to its own
    resolved location, so the sandbox holds a copy of the script, a copy
    of the ``cato_src`` package (a symlink would resolve back to the repo
    and read the repo's config) and a minimal ``config.json``.
    """
    repo_root = Path(__file__).resolve().parents[1]
    sandbox = tmp_path / "cato_root"
    sandbox.mkdir()
    (sandbox / "cato").write_bytes((repo_root / "cato").read_bytes())
    (sandbox / "cato").chmod(0o755)
    shutil.copytree(
        repo_root / "cato_src",
        sandbox / "cato_src",
        ignore=shutil.ignore_patterns("__pycache__"),
    )
    _write(
        sandbox / "config.json",
        {"backend": {"host": "localhost", "port": 4301}, "frontend": {"port": 5173}},
    )
    return sandbox


def _run_guard(
    sandbox: Path, monkeypatch: pytest.MonkeyPatch
) -> "subprocess.CompletedProcess[str]":
    """Source the launcher and run ``ensure_marcus_config`` with no terminal."""
    monkeypatch.delenv("MARCUS_ROOT", raising=False)
    # Fixed argv, no user input: this is the launcher under test.
    return subprocess.run(  # nosec B603 B607
        ["bash", "-c", "source ./cato >/dev/null; ensure_marcus_config"],
        cwd=sandbox,
        stdin=subprocess.DEVNULL,
        capture_output=True,
        text=True,
        timeout=30,
    )


def test_launcher_guard_accepts_plural_only_config(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A config.local.json holding only marcus_data_paths passes the guard.

    Before the fix the guard looked for ``marcus_data_path`` alone, so a
    multi-root setup was treated as unconfigured: it prompted (or, with no
    terminal, exited 1) and would have overwritten the list.
    """
    sandbox = _launcher_sandbox(tmp_path)
    root = _make_marcus_root(tmp_path, "marcus")
    _write(sandbox / "config.local.json", {"marcus_data_paths": [str(root / "data")]})
    result = _run_guard(sandbox, monkeypatch)
    assert result.returncode == 0, result.stdout + result.stderr


def test_launcher_guard_accepts_tracked_config_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A Marcus path configured in the tracked config.json also passes the guard."""
    sandbox = _launcher_sandbox(tmp_path)
    root = _make_marcus_root(tmp_path, "marcus")
    _write(
        sandbox / "config.json",
        {
            "backend": {"host": "localhost", "port": 4301},
            "frontend": {"port": 5173},
            "marcus_data_path": str(root / "data"),
        },
    )
    result = _run_guard(sandbox, monkeypatch)
    assert result.returncode == 0, result.stdout + result.stderr


def test_launcher_guard_warns_but_passes_when_one_of_several_is_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """One missing instance out of several warns instead of forcing a rewrite."""
    sandbox = _launcher_sandbox(tmp_path)
    root = _make_marcus_root(tmp_path, "marcus")
    gone = tmp_path / "gone" / "data"
    _write(
        sandbox / "config.local.json",
        {"marcus_data_paths": [str(root / "data"), str(gone)]},
    )
    result = _run_guard(sandbox, monkeypatch)
    assert result.returncode == 0, result.stdout + result.stderr
    assert str(gone) in result.stdout + result.stderr


def test_launcher_guard_still_fails_without_terminal_when_unconfigured(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Nothing configured and no TTY exits non-zero with instructions."""
    sandbox = _launcher_sandbox(tmp_path)
    result = _run_guard(sandbox, monkeypatch)
    assert result.returncode != 0
    assert "MARCUS_ROOT" in result.stdout + result.stderr
