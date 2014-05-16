import os
from setuptools import setup, find_packages

from performanceplatform import collector

requirements = [
    'requests',
    'pytz==2013d',
    'argparse',
    'python-dateutil',
    'logstash_formatter',
    'gapy',
    'google-api-python-client==1.0',
]

test_requirements = [
    'PyHamcrest',
    'nose',
    'mock',
    'pep8',
    'coverage',
    'freezegun',
]

HERE = os.path.dirname(__file__)
try:
    long_description = open(os.path.join(HERE, 'README.rst')).read()
except:
    long_description = None

setup(
    name='performanceplatform-collector',
    version=collector.__VERSION__,
    packages=find_packages(exclude=['test*']),

    # metadata for upload to PyPI
    author=collector.__AUTHOR__,
    author_email=collector.__AUTHOR_EMAIL__,
    maintainer='Government Digital Service',
    url='https://github.com/alphagov/performanceplatform-collector',

    description='performanceplatform-collector: tools for sending data to the Performance Platform',
    long_description=long_description,
    license='MIT',
    keywords='api data performance_platform',

    install_requires=requirements,
    tests_require=test_requirements,

    test_suite='nose.collector',

    entry_points={
        'console_scripts': [
            'pp-collector=performanceplatform.collector.main:main'
        ]
    }
)
