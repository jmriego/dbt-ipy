# setup.py
from setuptools import setup

setup(
  name="dbt-ipy",
  version="1.0.0",
  packages=["dbt-ipy"],
  license="GNU GPLv3",
  author="jmriego",
  author_email="jmriego@telefonica.net",
  url="http://www.github.com/jmriego/dbt-ipy",
  description="IPython magic to use dbt",
  long_description=open("README.md").read(),
  long_description_content_type="text/markdown",
  keywords="ipython dbt sql",
  install_requires = ['ipython>=1.0', 'dbt>=0.18'],
  classifiers=[
      "Development Status :: 3 - Alpha",
      "Framework :: IPython",
      "Programming Language :: Python",
  ],
)
