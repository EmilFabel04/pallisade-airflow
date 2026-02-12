import importlib
import os
import pathlib


def test_all_dags_import() -> None:
    """Fail fast if any DAG file cannot be imported.

    This catches common issues like missing dependencies or executing code at parse time.
    """

    dags_dir = pathlib.Path(__file__).resolve().parents[1] / "dags"
    assert dags_dir.exists(), "dags/ directory is missing"

    py_files = [p for p in dags_dir.glob("*.py") if p.name != "__init__.py"]
    assert py_files, "No DAG files found in dags/"

    # Ensure repo root is on PYTHONPATH for imports
    repo_root = str(pathlib.Path(__file__).resolve().parents[1])
    if repo_root not in os.sys.path:
        os.sys.path.insert(0, repo_root)

    for f in py_files:
        module_name = f"dags.{f.stem}" if (dags_dir / "__init__.py").exists() else f.stem
        importlib.import_module(module_name)
