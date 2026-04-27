from pathlib import Path

import pytest

from postprocessing.utils.paths import get_paths, ProjectPaths


def test_get_paths_returns_project_paths_instance() -> None:
    paths = get_paths()
    assert isinstance(paths, ProjectPaths)


def test_project_root_is_repo_root() -> None:
    paths = get_paths()
    assert (paths.project_root / "configs" / "paths.yaml").is_file()
    assert (paths.project_root / "pyproject.toml").is_file()


def test_all_data_paths_are_path_objects() -> None:
    paths = get_paths()
    for field_name in type(paths.data).model_fields:
        value = getattr(paths.data, field_name)
        assert isinstance(value, Path), f"data.{field_name} is not a Path"


def test_existing_directories_resolve_correctly() -> None:
    paths = get_paths()
    assert paths.data.raw_dir.is_dir()
    assert paths.data.raw_station_dir.is_dir()
    assert paths.data.manifests_dir.is_dir()
    assert paths.config_dir.is_dir()


def test_paths_caching_returns_same_instance() -> None:
    a = get_paths()
    b = get_paths()
    assert a is b


def test_log_dir_defaults_to_repo_logs() -> None:
    paths = get_paths()
    assert paths.log_dir.name == "logs"
    assert paths.log_dir.parent == paths.project_root
