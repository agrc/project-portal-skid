#!/usr/bin/env python
# -*- encoding: utf-8 -*-
"""
setup.py
A module that installs the SKIDNAME skid as a module
"""

from pathlib import Path

from setuptools import find_packages, setup

#: Load version from source file
version = {}
version_file = Path(__file__).parent / "src" / "skidname" / "version.py"
exec(version_file.read_text(), version)

setup(
    name="skidname",
    version=version["__version__"],
    license="MIT",
    long_description=(Path(__file__).parent / "README.md").read_text(),
    long_description_content_type="text/markdown",
    author="UGRC",
    author_email="ugrc-developers@utah.gov",
    url="https://github.com/agrc/skid",
    packages=find_packages("src"),
    package_dir={"": "src"},
    include_package_data=True,
    zip_safe=True,
    classifiers=[
        # complete classifier list: http://pypi.python.org/pypi?%3Aaction=list_classifiers
        "Development Status :: 5 - Production/Stable",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Topic :: Utilities",
    ],
    project_urls={
        "Issue Tracker": "https://github.com/agrc/skid/issues",
    },
    keywords=["gis"],
    install_requires=[
        "ugrc-palletjack>=5.0,<6.1",
        "ugrc-supervisor>=3.1.3",
    ],
    extras_require={
        "tests": [
            "pytest-cov>=6",
            "pytest-instafail==0.5.*",
            "pytest-mock==3.*",
            "pytest-ruff==0.*",
            "pytest-watch==4.*",
            "pytest>=7,<10",
            "ruff==0.*",
            "functions-framework>=3.8,<3.11",
        ]
    },
    setup_requires=[
        "pytest-runner",
    ],
    entry_points={
        "console_scripts": [
            "skidname = skidname.main:function",
        ]
    },
)
