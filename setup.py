#!/usr/bin/env python

from setuptools import setup, find_packages

setup(name='tap-youtube-analytics',
      version='0.0.9',
      description='Singer.io tap for extracting data from the Google Search Console API',
      author='jeff.huth@bytecode.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_youtube_analytics'],
      install_requires=[
          'backoff==1.8.0',
          'requests==2.24.0',
          'pyhumps==1.6.1',
          'singer-python==5.9.0'
      ],
      extras_require={
          'dev': [
              'ipdb==0.11',
              'pylint==2.5.3',
          ]
      },
      entry_points='''
          [console_scripts]
          tap-youtube-analytics=tap_youtube_analytics:main
      ''',
      packages=find_packages(),
      package_data={
          'tap_youtube_analytics': [
              'schemas/*.json',
              '*.json'
          ]
      })
