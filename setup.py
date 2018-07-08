#!/usr/bin/env python
# -*- encoding: utf-8 -*-
import os
import re
import sys
from setuptools import setup, find_packages
from setuptools.command.test import test as TestCommand


class PyTest(TestCommand):
    user_options = [('pytest-args=', 'a', "Arguments to pass to py.test")]

    def initialize_options(self):
        TestCommand.initialize_options(self)
        self.pytest_args = []

    def finalize_options(self):
        TestCommand.finalize_options(self)
        self.test_args = []
        self.test_suite = True

    def run_tests(self):
        # import here, cause outside the eggs aren't loaded
        import pytest
        errno = pytest.main(self.pytest_args)
        sys.exit(errno)


def version():
    import bqsqoop
    return bqsqoop.__version__


README = open(os.path.join(os.path.dirname(__file__), 'README.rst')).read()

egg = re.compile("\#egg\=(.*?)$")
requirements = open('requirements.txt').read().splitlines()
REQUIREMENTS = [req for req in requirements if (
    not req.startswith('-e') and not req.startswith('#') and req != '')]
DEPENDENCY_LINKS = [
    req.replace('-e ', '') for req in requirements if req.startswith('-e')
]
REQUIREMENTS.extend(
    [egg.findall(req)[0] for req in requirements if req.startswith('-e')]
)
REQUIREMENTS.append('futures; python_version == "2.7"')


test_requirements = open('test-requirements.txt').read().splitlines()
TEST_REQUIREMENTS = [req for req in test_requirements if (
    not req.startswith('-e') and not req.startswith('#') and req != '')]

setup(
    name='bq-sqoop',
    author='Arun Kumar Ramanathan',
    author_email='therealrako@gmail.com',
    license='MIT License',
    url='https://github.com/therako/bqsqoop',
    description='A CLI client for exporting elasticsearch data to csv',
    long_description=README,
    version=version(),
    packages=find_packages(exclude=('tests',)),
    cmdclass={'test': PyTest},
    scripts=('bin/bq-sqoop',),
    install_requires=REQUIREMENTS,
    tests_require=TEST_REQUIREMENTS,
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Development Status :: 2 - Pre-Alpha',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
    ],
)
