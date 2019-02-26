# coding=utf-8

from setuptools import setup
from os import path

here = path.abspath(path.dirname(__file__))

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name='metric-client',
    version='0.10.1',
    description='metric client for python',
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='https://github.com/zskymn/metric-client-py',
    author='zskymn',
    author_email='zsymn@163.com',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Scientific/Engineering',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.5',
    ],
    packages=['metric_client'],
    install_requires=['qtdigest==0.3.0', 'requests>=2.6.0'],
    extras_require={
        'test': ['pytest', 'pytest-cov'],
    },
    package_data={
        'metric_client': ['./README.md'],
    }
)
