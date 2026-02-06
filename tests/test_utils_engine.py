from importlib import metadata
from unittest.mock import mock_open, patch

from zoopipe.utils.engine import get_core_dependencies, is_dev_mode


def test_is_dev_mode_returns_true_in_dev_environment():
    with patch("os.path.exists") as mock_exists, patch("os.listdir") as mock_listdir:
        mock_exists.side_effect = lambda path: path in [
            "src/zoopipe",
            "pyproject.toml",
        ]
        mock_listdir.return_value = ["__init__.py", "zoopipe_rust_core.so"]

        result = is_dev_mode()

        assert result is True


def test_is_dev_mode_returns_false_when_src_missing():
    with patch("os.path.exists") as mock_exists:
        mock_exists.side_effect = lambda path: path == "pyproject.toml"

        result = is_dev_mode()

        assert result is False


def test_is_dev_mode_returns_false_when_pyproject_missing():
    with patch("os.path.exists") as mock_exists:
        mock_exists.side_effect = lambda path: path == "src/zoopipe"

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
