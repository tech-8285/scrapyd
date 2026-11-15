import os
import warnings
from pathlib import Path

import pytest
from setuptools import setup
from setuptools.warnings import SetuptoolsDeprecationWarning

BASEDIR = Path(__file__).parent.parent.resolve()
PROJECTS_DIR = BASEDIR / "tests" / "projects"
FIXTURES_DIR = BASEDIR / "tests" / "fixtures"

# Eggs the integration tests upload to freshly started Scrapyd servers.
REQUIRED_EGGS = ("mybot", "spiders_slow")


def _build_eggs():
    cwd = Path.cwd()
    os.chdir(PROJECTS_DIR)
    try:
        for project in REQUIRED_EGGS:
            egg_path = FIXTURES_DIR / f"{project}.egg"
            if egg_path.exists():
                continue

            packages = ["mybot"]
            if (PROJECTS_DIR / project / "spiders").exists():
                packages.append("mybot.spiders")

            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=SetuptoolsDeprecationWarning)
                setup(
                    script_args=["bdist_egg"],
                    name="project",
                    packages=packages,
                    package_dir={"mybot": project},
                    entry_points={"scrapy": ["settings = mybot.settings"]},
                    zip_safe=True,
                )

            egg_file = next(PROJECTS_DIR.joinpath("dist").iterdir())
            egg_file.rename(egg_path)

            setup(script_args=["clean", "--all", "--build-scripts=build"], packages=packages)
    finally:
        os.chdir(cwd)


@pytest.fixture(scope="session", autouse=True)
def _ensure_eggs():
    _build_eggs()
