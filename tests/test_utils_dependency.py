from unittest.mock import MagicMock, patch

from zoopipe.utils.dependency import (
    _try_env_install_with_pip,
    _try_env_install_with_poetry,
    _try_env_install_with_uv,
    install_dependencies,
)


def test_try_env_install_with_pip_success():
    with (
        patch("importlib.util.find_spec") as mock_find_spec,
        patch("subprocess.check_call") as mock_check_call,
    ):
        mock_find_spec.return_value = MagicMock()
        mock_check_call.return_value = 0

        result = _try_env_install_with_pip(["pydantic>=2.0"])

        assert result is True
        mock_check_call.assert_called_once()


def test_try_env_install_with_pip_not_available():
    with patch("importlib.util.find_spec", return_value=None):
        result = _try_env_install_with_pip(["pydantic>=2.0"])
        assert result is False


def test_try_env_install_with_pip_subprocess_error():
    with (
        patch("importlib.util.find_spec") as mock_find_spec,
        patch("subprocess.check_call") as mock_check_call,
    ):
        mock_find_spec.return_value = MagicMock()
        mock_check_call.side_effect = OSError("Installation failed")

        result = _try_env_install_with_pip(["invalid-package"])

        assert result is False


def test_try_env_install_with_uv_success():
    with (
        patch("shutil.which", return_value="/usr/bin/uv"),
        patch("subprocess.check_call") as mock_check_call,
    ):
        mock_check_call.return_value = 0

        result = _try_env_install_with_uv(["pydantic>=2.0"])

        assert result is True
        mock_check_call.assert_called_once()


def test_try_env_install_with_uv_not_available():
    with patch("shutil.which", return_value=None):
        result = _try_env_install_with_uv(["pydantic>=2.0"])
        assert result is False


def test_try_env_install_with_uv_subprocess_error():
    with (
        patch("shutil.which", return_value="/usr/bin/uv"),
        patch("subprocess.check_call") as mock_check_call,
    ):
        mock_check_call.side_effect = OSError("Installation failed")

        result = _try_env_install_with_uv(["invalid-package"])

        assert result is False


def test_try_env_install_with_poetry_success():
    with (
        patch("shutil.which", return_value="/usr/bin/poetry"),
        patch("subprocess.check_call") as mock_check_call,
    ):
        mock_check_call.return_value = 0

        result = _try_env_install_with_poetry(["pydantic>=2.0"])

        assert result is True
        mock_check_call.assert_called_once()


def test_try_env_install_with_poetry_not_available():
    with patch("shutil.which", return_value=None):
        result = _try_env_install_with_poetry(["pydantic>=2.0"])
        assert result is False


def test_try_env_install_with_poetry_subprocess_error():
    with (
        patch("shutil.which", return_value="/usr/bin/poetry"),
        patch("subprocess.check_call") as mock_check_call,
    ):
        mock_check_call.side_effect = OSError("Installation failed")

        result = _try_env_install_with_poetry(["invalid-package"])

        assert result is False


def test_install_dependencies_with_empty_list():
    result = install_dependencies([])
    assert result is None


def test_install_dependencies_fallback_chain_pip_succeeds():
    packages = ["pydantic>=2.0"]

    with (
        patch(
            "zoopipe.utils.dependency._try_env_install_with_pip", return_value=True
        ) as mock_pip,
        patch("zoopipe.utils.dependency._try_env_install_with_uv") as mock_uv,
        patch("zoopipe.utils.dependency._try_env_install_with_poetry") as mock_poetry,
    ):
        install_dependencies(packages)

        mock_pip.assert_called_once_with(packages)
        mock_uv.assert_not_called()
        mock_poetry.assert_not_called()


def test_install_dependencies_fallback_chain_uv_succeeds():
    packages = ["pydantic>=2.0"]

    with (
        patch(
            "zoopipe.utils.dependency._try_env_install_with_pip", return_value=False
        ) as mock_pip,
        patch(
            "zoopipe.utils.dependency._try_env_install_with_uv", return_value=True
        ) as mock_uv,
        patch("zoopipe.utils.dependency._try_env_install_with_poetry") as mock_poetry,
    ):
        install_dependencies(packages)

        mock_pip.assert_called_once_with(packages)
        mock_uv.assert_called_once_with(packages)
        mock_poetry.assert_not_called()


def test_install_dependencies_fallback_chain_poetry_succeeds():
    packages = ["pydantic>=2.0"]

    with (
        patch(
            "zoopipe.utils.dependency._try_env_install_with_pip", return_value=False
        ) as mock_pip,
        patch(
            "zoopipe.utils.dependency._try_env_install_with_uv", return_value=False
        ) as mock_uv,
        patch(
            "zoopipe.utils.dependency._try_env_install_with_poetry", return_value=True
        ) as mock_poetry,
    ):
        install_dependencies(packages)

        mock_pip.assert_called_once_with(packages)
        mock_uv.assert_called_once_with(packages)
        mock_poetry.assert_called_once_with(packages)


def test_install_dependencies_all_fail():
    packages = ["pydantic>=2.0"]

    with (
        patch("zoopipe.utils.dependency._try_env_install_with_pip", return_value=False),
        patch("zoopipe.utils.dependency._try_env_install_with_uv", return_value=False),
        patch(
            "zoopipe.utils.dependency._try_env_install_with_poetry", return_value=False
        ),
    ):
        result = install_dependencies(packages)
        assert result is None
