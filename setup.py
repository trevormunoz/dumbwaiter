#!/usr/bin/env python
# coding: utf-8

from setuptools import setup
from dumbwaiter import __version__

setup(
    name = 'dumbwaiter',
    version = __version__,
    description = 'Extract-Tranform-Load pipeline for NYPL menu data',
    author = 'Trevor Muñoz and Katie Rawson',
    author_email = 'trevor@trevormunoz.com',
    packages = ['dumbwaiter'],
    entry_points = {
        'console_scripts': [
            'dumbwaiter=dumbwaiter.__main__:main'
        ],
    }
)