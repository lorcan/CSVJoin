import os
from setuptools import setup

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name = "cvsjoin",
    version = "0.0.1",
    author = "Lorcan Coyle",
    description = ("A simple utility to join CSV files."),
    license = "MIT License",
    keywords = "csv join",
    url = "https://github.com/lorcan/CSVJoin",
    long_description=read('README.md'),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Utilities",
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: Developers",
        "Programming Language :: Python",
        "Natural Language :: English"
    ],
)