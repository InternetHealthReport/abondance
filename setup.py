# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open('README.md') as f:
    readme = f.read()

with open('LICENSE') as f:
    license = f.read()

setup(
    name='abondance',
    version='0.0.1',
    description="Pyhton library for Internet Health Report API",
    long_description=readme,
    author='Romain Fontugne',
    url='https://github.com/InternetHealthReport/abondance',
    license=license,
    packages=find_packages(exclude=('tests', 'docs'))
)

