import os

import setuptools
from setuptools import setup

from stargate import __version__

with open('requirements.txt') as req:
    requirements = req.read()

setup(
    name='stargate-python-runner',
    author='AML',
    description='Stargate python runner with all the runtime services implementations',
    long_description='Stargate python runner to invoke the given python lambda',
    license='Apple Internal',
    author_email='',
    url='https://github.pie.apple.com/aml/stargate',
    packages=setuptools.find_packages(include=('stargate',
                                               'stargate.bin',
                                               'stargate.gateway',
                                               'stargate.util',
                                               'stargate.util.jsonschema',
                                               'stargate.executor',
                                               'stargate.rpc',
                                               'stargate.rpc.proto',
                                               'stargate.service')),
    entry_points={
        'console_scripts': [
            'init_stargate_python_runner = stargate.bin.stargate_python_init:init_runner',
        ]
    },
    package_data={
        '': ['*.json']
    },
    include_package_data=True,
    install_requires=requirements,
    data_files=[
        ('.', ['requirements.txt'])
    ],
    dependency_links=[
        'https://pypi.apple.com/simple/',
        'https://artifacts.apple.com/api/pypi/apple-pypi-integration-local/simple'
    ],
    setup_requires=['wheel'],
    version=os.getenv('STARGATE_VERSION', __version__)
)
