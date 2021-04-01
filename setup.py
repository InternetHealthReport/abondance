# -*- coding: utf-8 -*-

from setuptools import setup, find_packages

with open('README.md') as f:
    readme = f.read()

setup(
    name='abondance',
    version='0.1.2',
    description="Pyhton library for Internet Health Report API",
    long_description=readme,
    long_description_content_type="text/markdown",
    author='Romain Fontugne',
    url='https://github.com/InternetHealthReport/abondance',
    packages=find_packages(exclude=('tests', 'docs')),
    install_requires=[
        'arrow',
        'requests_futures',
        'ujson'
        ]
)

