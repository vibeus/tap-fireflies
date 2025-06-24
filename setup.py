#!/usr/bin/env python
from setuptools import setup

setup(
    name="tap-fireflies",
    version="1.0.0",
    description="Singer.io tap for extracting Fireflies data",
    author="Data Team @ Vibe",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_fireflies"],
    install_requires=[
        # NB: Pin these to a more specific version for tap reliability
        'backoff==2.2.1',
        'requests==2.32.3',
        'singer-python==6.0.0'
    ],
    entry_points="""
    [console_scripts]
    tap-fireflies=tap_fireflies:main
    """,
    packages=["tap_fireflies"],
    package_data = {
        "schemas": ["tap_fireflies/schemas/*.json"]
    },
    include_package_data=True,
)
