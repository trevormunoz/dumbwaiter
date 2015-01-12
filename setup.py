#!/usr/bin/env python
# coding: utf-8

from setuptools import setup

setup(
    name = 'dumbwaiter',
    version = '0.3.0',
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