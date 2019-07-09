# Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
# See License in the project root for license information.

import setuptools

setuptools.setup(
    name='cruise-control-client',
    version='0.1.1',
    author='mgrubent',
    author_email='mgrubentrejo@linkedin.com',
    description='A Python client for cruise-control',
    url='https://github.com/linkedin/cruise-control',
    packages=setuptools.find_packages(),
    install_requires=[
        'pandas',
        'requests'
    ],
    license='Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").'
            ' See License in the project root for license information.',
)
