# coding=utf-8

from setuptools import setup
from os import path

here = path.abspath(path.dirname(__file__))

setup(
    name='metric-client',
    version='0.0.1',
    description='metric client for python',
    url='',
    author='zskymn',
    author_email='zsymn@163.com',
    license='MIT',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: Scientific/Engineering',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7'
        'Programming Language :: Python :: 3.5',
    ],
    packages=['metric_client'],
    install_requires=['qtdigest==0.3.0', 'requests>=2.6.0'],
    extras_require={
        'test': ['pytest', 'pytest-cov'],
    },
    package_data={
        'metric_client': ['../README.md'],
    }
)
