import os
import re
from importlib import metadata


def is_dev_mode() -> bool:
    """
    Check if we are in development mode based on the environment.
    (e.g., being in the zoopipe repo with source files and pyproject.toml).
    """
    try:
        return (
            os.path.exists("src/zoopipe")
            and os.path.exists("pyproject.toml")
            and any(
                f.endswith(".so") or f.endswith(".py")
                for f in os.listdir("src/zoopipe")
            )
        )
    except Exception:
        return False


def get_core_dependencies() -> list[str]:
    """
    Extract core dependencies from pyproject.toml in dev mode,
    or return the current zoopipe version in user mode.
    """
    deps = []
    if is_dev_mode():
        try:
            with open("pyproject.toml", "r") as f:
                toml_content = f.read()
                # Find dependencies = [ ... ] block
                match = re.search(
                    r"dependencies\s*=\s*\[(.*?)\]", toml_content, re.DOTALL
                )
                if match:
                    dep_block = match.group(1)
                    deps = re.findall(r'["\'](.*?)["\']', dep_block)
        except Exception:
            pass
    else:
        try:
            version = metadata.version("zoopipe")
            deps.append(f"zoopipe=={version}")
        except metadata.PackageNotFoundError:
            # Fallback to hardcoded core if everything fails
            deps = ["pydantic>=2.0"]

    return list(set(deps))
