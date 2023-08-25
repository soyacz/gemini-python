import toml


def test_version_consistency(project_root_dir):
    pyproject = toml.load(project_root_dir / "pyproject.toml")
    version_pyproject = pyproject["tool"]["poetry"]["version"]

    version_txt = (project_root_dir / "gemini_python" / "version.txt").read_text().strip()

    assert (
        version_pyproject == version_txt
    ), f"Versions do not match: {version_pyproject} != {version_txt}"
