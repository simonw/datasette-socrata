from setuptools import setup
import os

VERSION = "0.3.1"


def get_long_description():
    with open(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "README.md"),
        encoding="utf8",
    ) as fp:
        return fp.read()


setup(
    name="datasette-socrata",
    description="Import data from Socrata into Datasette",
    long_description=get_long_description(),
    long_description_content_type="text/markdown",
    author="Simon Willison",
    url="https://github.com/simonw/datasette-socrata",
    project_urls={
        "Issues": "https://github.com/simonw/datasette-socrata/issues",
        "CI": "https://github.com/simonw/datasette-socrata/actions",
        "Changelog": "https://github.com/simonw/datasette-socrata/releases",
    },
    license="Apache License, Version 2.0",
    classifiers=[
        "Framework :: Datasette",
        "License :: OSI Approved :: Apache Software License",
    ],
    version=VERSION,
    packages=["datasette_socrata"],
    entry_points={"datasette": ["socrata = datasette_socrata"]},
    install_requires=[
        "datasette",
        "sqlite-utils>=3.27",
        "datasette-low-disk-space-hook",
    ],
    extras_require={"test": ["pytest", "pytest-asyncio", "pytest-httpx"]},
    package_data={"datasette_socrata": ["templates/*"]},
    python_requires=">=3.7",
)
