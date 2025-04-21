from setuptools import setup


setup(
    name="tap-youtube-analytics",
    version="1.0.0",
    description="Singer.io tap for extracting data from youtube-analytics API",
    author="Stitch",
    url="http://singer.io",
    classifiers=["Programming Language :: Python :: 3 :: Only"],
    py_modules=["tap_youtube_analytics"],
    install_requires=[
        "singer-python==6.1.1",
        "requests==2.32.3",
    ],
    extras_require={
        "dev": [
            "ipdb==0.13.13",
            "pylint==3.3.6",
        ]
    },
    entry_points="""
          [console_scripts]
          tap-youtube-analytics=tap_youtube_analytics:main
      """,
    packages=["tap-youtube-analytics"],
    package_data={
        "tap_youtube_analytics": ["schemas/*.json", "*.json"],
    },
    include_package_data=True,
)
