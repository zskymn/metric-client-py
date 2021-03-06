# coding=utf-8

from setuptools import setup
from os import path

here = path.abspath(path.dirname(__file__))

setup(
    name='metric-client',
    version='3.2.0',
    description='metric client for python',
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
        'Programming Language :: Python :: 3.7',
    ],
    packages=['metric_client'],
    install_requires=['qtdigest-cffi==0.5.0', 'requests>=2.6.0', 'cffi>=1.4.0'],
    extras_require={
        'test': ['pytest', 'pytest-cov'],
    },
    package_data={
        'metric_client': ['./README.md'],
    }
)
