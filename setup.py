from __future__ import (absolute_import, division, print_function)

import versioneer
import setuptools

# To use a consistent encoding
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

# Get the long description from the README file
with open(path.join(here, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

with open(path.join(here, 'requirements.txt')) as f:
    requirements = f.read().splitlines()

setuptools.setup(
    name='nslsii',
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    license="BSD (3-clause)",
    python_requires=">=3.10",
    packages=setuptools.find_packages(),
    long_description=long_description,
    long_description_content_type='text/markdown',
    description='Tools for data collection and analysis at NSLS-II',
    author='Brookhaven National Laboratory',
    install_requires=requirements,
    entry_points={
        "console_scripts": [
            "sync-experiment = nslsii.sync_experiment:main",
            "what-is-ioc = nslsii.epics_utils:main",
            "axis-saver-ioc = nslsii.iocs.caproto_saver:start_axis_ioc"
        ],
    },
)
