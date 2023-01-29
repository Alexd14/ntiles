from distutils.core import setup

from setuptools import find_packages

setup(
    name='ntiles',
    version='0.1.4.1',
    packages=find_packages(),
    license='Apache License 2.0',
    description='Vectorized quantile backtester.',
    url='https://github.com/Alexd14/ntiles-backtester',
    download_url='https://github.com/Alexd14/ntiles-backtester/archive/refs/tags/v0.1.4.1.zip',
    keywords=['factor', 'backtesting', 'alphalens', 'vectorized backtesting', 'equity trading'],
    install_requires=[
        'numba',
        'pandas',
        'numpy',
        'matplotlib',
        'empyrical',
        'factor_toolbox',
        # 'equity-db'
        ],
    classifiers=[
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
    ],
)
