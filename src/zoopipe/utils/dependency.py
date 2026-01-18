import importlib.util
import shutil
import subprocess
import sys


def _try_env_install_with_pip(packages: list[str]) -> bool:
    """
    Try to install packages using standard pip module.
    """
    if importlib.util.find_spec("pip") is None:
        return False

    try:
        subprocess.check_call(
            [sys.executable, "-m", "pip", "install", *packages],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return True
    except (subprocess.CalledProcessError, OSError):
        return False


def _try_env_install_with_uv(packages: list[str]) -> bool:
    """
    Try to install packages using 'uv pip install'.
    """
    uv_path = shutil.which("uv")
    if not uv_path:
        return False

    try:
        subprocess.check_call(
            [uv_path, "pip", "install", *packages],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return True
    except (subprocess.CalledProcessError, OSError):
        return False


def _try_env_install_with_poetry(packages: list[str]) -> bool:
    """
    Try to install packages using 'poetry run pip install'.
    """
    poetry_path = shutil.which("poetry")
    if not poetry_path:
        return False

    try:
        # Assuming we are in a poetry env
        subprocess.check_call(
            [poetry_path, "run", "pip", "install", *packages],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        return True
    except (subprocess.CalledProcessError, OSError):
        return False


def install_dependencies(packages: list[str]) -> None:
    """
    Agnostically install dependencies using available package managers.
    Strategies: pip -> uv -> poetry.
    If all fail, it does nothing (assuming manual provisioning).
    """
    if not packages:
        return

    if _try_env_install_with_pip(packages):
        return
    if _try_env_install_with_uv(packages):
        return
    if _try_env_install_with_poetry(packages):
        return
