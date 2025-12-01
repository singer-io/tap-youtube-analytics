from setuptools import setup, find_packages

setup(name='tap-youtube-analytics',
      version='0.0.9',
      description='Singer.io tap for extracting data from the Google Search Console API',
      author='jeff.huth@bytecode.io',
      classifiers=['Programming Language :: Python :: 3 :: Only'],
      py_modules=['tap_youtube_analytics'],
      install_requires=[
          'backoff==2.2.1',
          'requests==2.32.5',
          'pyhumps==3.8.0',
          'singer-python==6.1.1'
      ],
      extras_require={
          'dev': [
              'ipdb==0.13.13',
              'pylint==3.3.6',
          ]
      },
      entry_points='''
          [console_scripts]
          tap-youtube-analytics=tap_youtube_analytics:main
      """,
    packages=find_packages(),
    package_data={
        "tap_youtube_analytics": ["schemas/*.json", "*.json"],
    },
    include_package_data=True,
)
