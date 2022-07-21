# setup.py
from setuptools import setup

setup(
  name="dbt-ipy",
  version="1.0.1",
  packages=["dbt-ipy"],
  license="GNU GPLv3",
  author="jmriego",
  author_email="jmriego@telefonica.net",
  url="http://www.github.com/jmriego/dbt-ipy",
  description="IPython magic to use dbt",
  long_description=open("README.md").read(),
  long_description_content_type="text/markdown",
  keywords="ipython dbt sql",
  install_requires = ['ipython>=1.0', 'dbt-core>=1.0'],
  classifiers=[
      "Development Status :: 3 - Alpha",
      "Framework :: IPython",
      "Programming Language :: Python",
  ],
)
