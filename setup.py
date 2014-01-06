import os
from setuptools import setup, find_packages

from backdrop import collector

requirements = ['requests', 'pytz', 'argparse', 'python-dateutil', 'logstash_formatter']

HERE = os.path.dirname(__file__)
try:
    long_description = open(os.path.join(HERE, 'README.rst')).read()
except:
    long_description = None

setup(
    name='backdrop-collector',
    version=collector.__VERSION__,
    packages=find_packages(exclude=['test*']),

    # metadata for upload to PyPI
    author=collector.__AUTHOR__,
    author_email=collector.__AUTHOR_EMAIL__,
    maintainer='Government Digital Service',
    url='https://github.com/alphagov/backdrop-collector',

    description='backdrop-collector: tools for sending data to backdrop',
    long_description=long_description,
    license='MIT',
    keywords='api data performance_platform',

    install_requires=requirements,
)
