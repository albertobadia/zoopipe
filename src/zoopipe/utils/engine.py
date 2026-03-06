import os
import re
from importlib import metadata


def is_dev_mode() -> bool:
    """
    Check if we are in development mode based on the environment.
    (e.g., being in the zoopipe repo with source files and pyproject.toml).
    """
    try:
        module_dir = os.path.dirname(os.path.abspath(__file__))
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(module_dir)))
        src_zoopipe_path = os.path.join(project_root, "src", "zoopipe")
        pyproject_path = os.path.join(project_root, "pyproject.toml")
        return (
            os.path.exists(src_zoopipe_path)
            and os.path.exists(pyproject_path)
            and any(
                f.endswith(".so") or f.endswith(".py")
                for f in os.listdir(src_zoopipe_path)
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
            module_dir = os.path.dirname(os.path.abspath(__file__))
            project_root = os.path.dirname(os.path.dirname(os.path.dirname(module_dir)))
            pyproject_path = os.path.join(project_root, "pyproject.toml")
            with open(pyproject_path, "r") as f:
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
