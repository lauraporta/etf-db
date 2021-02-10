from setuptools import setup

setup(name = 'etf_db',
      version = '0.0.1',
      description = 'A python package for downloading public data from etfdb.com screener tool',
      author = 'Laura Porta',
      author_email = 'porta.laura.2@gmail.com',
      packages = ['etf_db'],
      install_requires = 'requests, pandas, json')