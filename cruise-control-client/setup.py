# Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").
# See License in the project root for license information.

import setuptools

LONG_DESCRIPTION ='''`cruise-control-client` is an API-complete Python client for [`cruise-control`](https://github.com/linkedin/cruise-control).

It comes with a command-line interface to the client (`cccli`) (see [README](https://github.com/linkedin/cruise-control/tree/migrate_to_kafka_2_4/docs/wiki/Python%20Client)).

`cruise-control-client` can also be used in Python applications needing programmatic access to `cruise-control`.'''

setuptools.setup(
    name='cruise-control-client',
    version='1.1.3',
    author='mgrubent',
    author_email='mgrubentrejo@linkedin.com',
    description='A Python client for cruise-control',
    long_description=LONG_DESCRIPTION,
    long_description_content_type='text/markdown',
    url='https://github.com/linkedin/cruise-control',
    entry_points={
        'console_scripts': [
            'cccli = cruisecontrolclient.client.cccli:main'
        ]
    },
    packages=setuptools.find_packages(),
    install_requires=[
        'requests'
    ],
    license='Copyright 2019 LinkedIn Corp. Licensed under the BSD 2-Clause License (the "License").'
            ' See License in the project root for license information.',
)
