import os
from importlib import metadata
from unittest.mock import mock_open, patch

from zoopipe.utils.engine import get_core_dependencies, is_dev_mode


def test_is_dev_mode_returns_true_in_dev_environment():
    test_module_dir = "/fake/project/src/zoopipe/utils"
    test_project_root = "/fake/project"
    zoopipe_dir = os.path.join(test_project_root, "src", "zoopipe")
    with (
        patch("zoopipe.utils.engine.os.path.abspath") as mock_abspath,
        patch("zoopipe.utils.engine.os.path.dirname") as mock_dirname,
        patch("zoopipe.utils.engine.os.path.exists") as mock_exists,
        patch("zoopipe.utils.engine.os.listdir") as mock_listdir,
    ):
        mock_abspath.return_value = test_module_dir
        mock_dirname.side_effect = [
            test_module_dir,
            os.path.dirname(test_module_dir),
            os.path.dirname(os.path.dirname(test_module_dir)),
            test_project_root,
        ]
        mock_exists.side_effect = lambda path: path in [
            os.path.join(test_project_root, "src", "zoopipe"),
            os.path.join(test_project_root, "pyproject.toml"),
        ]
        mock_listdir.side_effect = lambda path: (
            ["__init__.py", "zoopipe_rust_core.so"] if path == zoopipe_dir else []
        )

        result = is_dev_mode()

        assert result is True


def test_is_dev_mode_returns_false_when_src_missing():
    test_module_dir = "/fake/project/src/zoopipe"
    test_project_root = "/fake/project"
    with (
        patch("os.path.abspath") as mock_abspath,
        patch("os.path.dirname") as mock_dirname,
        patch("os.path.exists") as mock_exists,
    ):
        mock_abspath.return_value = test_module_dir
        mock_dirname.side_effect = [
            test_module_dir,
            test_project_root,
        ]
        mock_exists.side_effect = lambda path: path == os.path.join(
            test_project_root, "pyproject.toml"
        )

        result = is_dev_mode()

        assert result is False


def test_is_dev_mode_returns_false_when_pyproject_missing():
    test_module_dir = "/fake/project/src/zoopipe"
    test_project_root = "/fake/project"
    with (
        patch("os.path.abspath") as mock_abspath,
        patch("os.path.dirname") as mock_dirname,
        patch("os.path.exists") as mock_exists,
    ):
        mock_abspath.return_value = test_module_dir
        mock_dirname.side_effect = [
            test_module_dir,
            test_project_root,
        ]
        mock_exists.side_effect = lambda path: path == os.path.join(
            test_project_root, "src", "zoopipe"
        )

        result = is_dev_mode()

        assert result is False


def test_is_dev_mode_returns_false_on_exception():
    with patch("os.path.exists", side_effect=Exception("Permission error")):
        result = is_dev_mode()

        assert result is False


def test_get_core_dependencies_in_dev_mode():
    toml_content = """
[project]
dependencies = [
    "pydantic>=2.0",
    "typing-extensions>=4.0",
]
"""

    with (
        patch("zoopipe.utils.engine.is_dev_mode", return_value=True),
        patch("builtins.open", mock_open(read_data=toml_content)),
    ):
        deps = get_core_dependencies()

        assert "pydantic>=2.0" in deps
        assert "typing-extensions>=4.0" in deps


def test_get_core_dependencies_in_user_mode():
    with (
        patch("zoopipe.utils.engine.is_dev_mode", return_value=False),
        patch("importlib.metadata.version", return_value="1.2.3"),
    ):
        deps = get_core_dependencies()

        assert "zoopipe==1.2.3" in deps


def test_get_core_dependencies_fallback_when_metadata_not_found():
    with (
        patch("zoopipe.utils.engine.is_dev_mode", return_value=False),
        patch(
            "importlib.metadata.version",
            side_effect=metadata.PackageNotFoundError(),
        ),
    ):
        deps = get_core_dependencies()

        assert "pydantic>=2.0" in deps


def test_get_core_dependencies_handles_malformed_toml():
    toml_content = """
[project]
name = "zoopipe"
"""

    with (
        patch("zoopipe.utils.engine.is_dev_mode", return_value=True),
        patch("builtins.open", mock_open(read_data=toml_content)),
    ):
        deps = get_core_dependencies()

        assert deps == []


def test_get_core_dependencies_handles_file_read_error():
    with (
        patch("zoopipe.utils.engine.is_dev_mode", return_value=True),
        patch("builtins.open", side_effect=Exception("File not found")),
    ):
        deps = get_core_dependencies()

        assert deps == []
